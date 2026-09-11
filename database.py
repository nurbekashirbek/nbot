import os
import logging
from contextlib import contextmanager
from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


DATABASE_URL = os.getenv("DATABASE_URL")


def _require_database_url():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")


def get_connection():
    _require_database_url()
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        connect_timeout=15,
        application_name="oms-kz-bot",
    )


@contextmanager
def db_connection(dict_cursor=False):
    conn = get_connection()
    try:
        if dict_cursor:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()
        try:
            yield conn, cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()


def test_connection():
    try:
        with db_connection(dict_cursor=True) as (_, cur):
            cur.execute("SELECT NOW() AS server_time")
            row = cur.fetchone()
        logging.info("Database connection successful. Server time: %s", row["server_time"])
        return True, f"Connection successful | {row['server_time']}"
    except Exception as exc:
        logging.exception("Database connection failed")
        return False, str(exc)


def get_table_counts():
    tables = ("orders", "order_status_history", "daily_order_snapshot", "daily_otd")
    out = {}
    with db_connection() as (_, cur):
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            out[table] = cur.fetchone()[0]
    return out


ORDER_COLUMNS = (
    "order_code",
    "pickup_point_id",
    "store_name",
    "creation_date",
    "planned_transmission_date",
    "actual_transmission_date",
    "current_status",
    "is_cancelled",
    "was_delayed",
    "is_currently_delayed",
    "delay_started_at",
    "delay_resolved_at",
    "delay_minutes",
)


def bulk_upsert_orders(orders):
    """Bulk insert/update orders and return {order_code: db_id}."""
    if not orders:
        return {}

    values = [
        tuple(o.get(col) for col in ORDER_COLUMNS)
        for o in orders
        if o.get("order_code")
    ]
    if not values:
        return {}

    sql = """
        INSERT INTO orders (
            order_code,
            pickup_point_id,
            store_name,
            creation_date,
            planned_transmission_date,
            actual_transmission_date,
            current_status,
            is_cancelled,
            was_delayed,
            is_currently_delayed,
            delay_started_at,
            delay_resolved_at,
            delay_minutes,
            first_seen_at,
            last_seen_at,
            cancelled_at,
            created_at,
            updated_at
        )
        VALUES %s
        ON CONFLICT (order_code) DO UPDATE SET
            pickup_point_id = EXCLUDED.pickup_point_id,
            store_name = EXCLUDED.store_name,
            creation_date = COALESCE(EXCLUDED.creation_date, orders.creation_date),
            planned_transmission_date = COALESCE(
                EXCLUDED.planned_transmission_date,
                orders.planned_transmission_date
            ),
            actual_transmission_date = COALESCE(
                EXCLUDED.actual_transmission_date,
                orders.actual_transmission_date
            ),
            current_status = EXCLUDED.current_status,
            is_cancelled = orders.is_cancelled OR EXCLUDED.is_cancelled,
            cancelled_at = CASE
                WHEN orders.is_cancelled OR EXCLUDED.is_cancelled
                THEN COALESCE(orders.cancelled_at, NOW())
                ELSE NULL
            END,
            was_delayed = orders.was_delayed OR EXCLUDED.was_delayed,
            is_currently_delayed = CASE
                WHEN orders.is_cancelled OR EXCLUDED.is_cancelled THEN FALSE
                ELSE EXCLUDED.is_currently_delayed
            END,
            delay_started_at = CASE
                WHEN orders.delay_started_at IS NULL THEN EXCLUDED.delay_started_at
                WHEN EXCLUDED.delay_started_at IS NULL THEN orders.delay_started_at
                ELSE LEAST(orders.delay_started_at, EXCLUDED.delay_started_at)
            END,
            delay_resolved_at = CASE
                WHEN (
                    orders.is_currently_delayed = TRUE
                    AND EXCLUDED.is_currently_delayed = FALSE
                )
                OR EXCLUDED.is_cancelled = TRUE
                THEN COALESCE(EXCLUDED.delay_resolved_at, NOW())
                ELSE COALESCE(EXCLUDED.delay_resolved_at, orders.delay_resolved_at)
            END,
            delay_minutes = GREATEST(
                COALESCE(orders.delay_minutes, 0),
                COALESCE(EXCLUDED.delay_minutes, 0)
            ),
            last_seen_at = NOW(),
            updated_at = NOW()
        RETURNING id, order_code
    """

    template = """(
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        NOW(),NOW(),
        CASE WHEN %s THEN NOW() ELSE NULL END,
        NOW(),NOW()
    )"""

    # is_cancelled is the 8th value (index 7), repeated for cancelled_at.
    extended_values = [v + (bool(v[7]),) for v in values]

    with db_connection(dict_cursor=True) as (_, cur):
        execute_values(
            cur,
            sql,
            extended_values,
            template=template,
            page_size=500,
        )
        rows = cur.fetchall()

    return {str(row["order_code"]): row["id"] for row in rows}


def bulk_insert_history_if_changed(orders, id_map):
    """
    Add status history only when the latest stored state differs.
    This prevents the history table from growing on every refresh.
    """
    rows = []
    for o in orders:
        order_id = id_map.get(str(o.get("order_code")))
        if not order_id:
            continue
        rows.append(
            (
                order_id,
                o.get("current_status"),
                bool(o.get("was_delayed")),
                bool(o.get("is_currently_delayed")),
                bool(o.get("is_cancelled")),
                o.get("planned_transmission_date"),
                o.get("actual_transmission_date"),
            )
        )

    if not rows:
        return 0

    sql = """
        WITH incoming (
            order_id,
            status,
            was_delayed,
            is_currently_delayed,
            is_cancelled,
            planned_transmission_date,
            actual_transmission_date
        ) AS (VALUES %s)
        INSERT INTO order_status_history (
            order_id,
            status,
            was_delayed,
            is_currently_delayed,
            is_cancelled,
            planned_transmission_date,
            actual_transmission_date,
            recorded_at
        )
        SELECT
            i.order_id,
            i.status,
            i.was_delayed,
            i.is_currently_delayed,
            i.is_cancelled,
            i.planned_transmission_date,
            i.actual_transmission_date,
            NOW()
        FROM incoming i
        WHERE NOT EXISTS (
            SELECT 1
            FROM LATERAL (
                SELECT h.*
                FROM order_status_history h
                WHERE h.order_id = i.order_id
                ORDER BY h.recorded_at DESC, h.id DESC
                LIMIT 1
            ) last
            WHERE
                COALESCE(last.status, '') = COALESCE(i.status, '')
                AND last.was_delayed IS NOT DISTINCT FROM i.was_delayed
                AND last.is_currently_delayed IS NOT DISTINCT FROM i.is_currently_delayed
                AND last.is_cancelled IS NOT DISTINCT FROM i.is_cancelled
                AND last.planned_transmission_date IS NOT DISTINCT FROM i.planned_transmission_date
                AND last.actual_transmission_date IS NOT DISTINCT FROM i.actual_transmission_date
        )
    """

    # Explicit casts are important here. If a whole VALUES batch contains NULL
    # for one of the timestamp columns, PostgreSQL can infer that VALUES column
    # as text, which then breaks IS NOT DISTINCT FROM against timestamptz.
    history_template = (
        "(%s::bigint,%s::text,%s::boolean,%s::boolean,%s::boolean,"
        "%s::timestamptz,%s::timestamptz)"
    )

    with db_connection() as (_, cur):
        execute_values(
            cur,
            sql,
            rows,
            template=history_template,
            page_size=500,
        )
        return cur.rowcount


def bulk_save_orders(orders, write_history=True):
    if not orders:
        return {}
    id_map = bulk_upsert_orders(orders)
    if write_history:
        bulk_insert_history_if_changed(orders, id_map)
    return id_map


def get_order_by_code(order_code):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute("SELECT * FROM orders WHERE order_code = %s", (str(order_code),))
        return cur.fetchone()


def get_orders_by_codes(order_codes):
    codes = [str(x) for x in order_codes if x]
    if not codes:
        return {}
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute("SELECT * FROM orders WHERE order_code = ANY(%s)", (codes,))
        return {str(r["order_code"]): r for r in cur.fetchall()}


def bulk_save_morning_snapshot(report_date, orders, id_map):
    rows = []
    for o in orders:
        order_id = id_map.get(str(o.get("order_code")))
        if not order_id:
            continue
        rows.append(
            (
                report_date,
                order_id,
                str(o.get("order_code")),
                o.get("store_name"),
                o.get("pickup_point_id"),
                o.get("planned_transmission_date"),
                o.get("actual_transmission_date"),
                o.get("current_status"),
                True,
                bool(o.get("is_cancelled")),
                False,
                False,
                int(o.get("delay_minutes") or 0),
            )
        )

    if not rows:
        return 0

    sql = """
        INSERT INTO daily_order_snapshot (
            report_date,
            order_id,
            order_code,
            store_name,
            pickup_point_id,
            planned_transmission_date,
            actual_transmission_date,
            final_status,
            was_present_morning,
            was_cancelled,
            was_on_time,
            was_delayed,
            delay_minutes,
            checked_at,
            created_at,
            updated_at
        )
        VALUES %s
        ON CONFLICT (report_date, order_id) DO NOTHING
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),NOW())"

    with db_connection() as (_, cur):
        execute_values(cur, sql, rows, template=template, page_size=500)
        inserted = cur.rowcount
        cur.execute(
            """
            UPDATE orders o
            SET
                morning_snapshot_date = %s,
                was_in_morning_snapshot = TRUE,
                updated_at = NOW()
            WHERE o.id = ANY(%s)
            """,
            (report_date, [r[1] for r in rows]),
        )
    return inserted


def get_snapshot_count(report_date):
    with db_connection() as (_, cur):
        cur.execute(
            "SELECT COUNT(*) FROM daily_order_snapshot WHERE report_date = %s",
            (report_date,),
        )
        return cur.fetchone()[0]


def get_snapshot_orders(report_date):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT
                s.*,
                o.current_status,
                o.is_currently_delayed,
                o.was_delayed AS order_was_delayed,
                o.is_cancelled AS order_is_cancelled
            FROM daily_order_snapshot s
            JOIN orders o ON o.id = s.order_id
            WHERE s.report_date = %s
            ORDER BY s.store_name, s.order_code
            """,
            (report_date,),
        )
        return cur.fetchall()


def bulk_finalize_snapshot(report_date, final_orders, id_map):
    rows = []
    for o in final_orders:
        order_id = id_map.get(str(o.get("order_code")))
        if not order_id:
            continue
        rows.append(
            (
                report_date,
                order_id,
                str(o.get("order_code")),
                o.get("actual_transmission_date"),
                o.get("current_status"),
                bool(o.get("is_cancelled")),
                bool(o.get("was_on_time")),
                bool(o.get("snapshot_delayed")),
                int(o.get("otd_delay_minutes") or 0),
            )
        )

    if not rows:
        return 0

    sql = """
        UPDATE daily_order_snapshot AS s
        SET
            order_code = v.order_code,
            actual_transmission_date = v.actual_transmission_date,
            final_status = v.final_status,
            was_cancelled = v.was_cancelled,
            was_on_time = v.was_on_time,
            was_delayed = v.was_delayed,
            delay_minutes = v.delay_minutes,
            checked_at = NOW(),
            updated_at = NOW()
        FROM (VALUES %s) AS v(
            report_date,
            order_id,
            order_code,
            actual_transmission_date,
            final_status,
            was_cancelled,
            was_on_time,
            was_delayed,
            delay_minutes
        )
        WHERE s.report_date = v.report_date
          AND s.order_id = v.order_id
    """
    with db_connection() as (_, cur):
        execute_values(cur, sql, rows, page_size=500)
        return cur.rowcount


def get_open_delays():
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT *
            FROM orders
            WHERE is_currently_delayed = TRUE
              AND is_cancelled = FALSE
            ORDER BY planned_transmission_date, store_name, order_code
            """
        )
        return cur.fetchall()


def get_open_delay_codes():
    with db_connection() as (_, cur):
        cur.execute(
            """
            SELECT order_code
            FROM orders
            WHERE is_currently_delayed = TRUE
              AND is_cancelled = FALSE
            ORDER BY order_code
            """
        )
        return [str(r[0]) for r in cur.fetchall()]



def get_operational_orders(cutoff_end):
    """
    Orders that still need courier handover and whose planned handover time
    is before cutoff_end. Used for today's pending + open delays.
    """
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT *
            FROM orders
            WHERE is_cancelled = FALSE
              AND actual_transmission_date IS NULL
              AND planned_transmission_date IS NOT NULL
              AND planned_transmission_date < %s
            ORDER BY planned_transmission_date, store_name, order_code
            """,
            (cutoff_end,),
        )
        return cur.fetchall()


def get_operational_order_codes(cutoff_end):
    with db_connection() as (_, cur):
        cur.execute(
            """
            SELECT order_code
            FROM orders
            WHERE is_cancelled = FALSE
              AND actual_transmission_date IS NULL
              AND planned_transmission_date IS NOT NULL
              AND planned_transmission_date < %s
            ORDER BY planned_transmission_date, store_name, order_code
            """,
            (cutoff_end,),
        )
        return [str(r[0]) for r in cur.fetchall()]


def get_delayed_snapshot_orders(report_date):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT *
            FROM daily_order_snapshot
            WHERE report_date = %s
              AND was_delayed = TRUE
              AND was_cancelled = FALSE
            ORDER BY store_name, order_code
            """,
            (report_date,),
        )
        return cur.fetchall()


def replace_daily_otd(report_date, rows):
    if not rows:
        return 0

    values = [
        (
            report_date,
            r.get("pickup_point_id"),
            r["store_name"],
            int(r.get("morning_orders") or 0),
            int(r.get("cancelled_orders") or 0),
            int(r.get("actual_orders") or 0),
            int(r.get("on_time_orders") or 0),
            int(r.get("delayed_orders") or 0),
            r.get("otd_percent"),
            int(r.get("open_delays") or 0),
        )
        for r in rows
    ]

    sql = """
        INSERT INTO daily_otd (
            report_date,
            pickup_point_id,
            store_name,
            morning_orders,
            cancelled_orders,
            actual_orders,
            on_time_orders,
            delayed_orders,
            otd_percent,
            open_delays,
            created_at,
            updated_at
        )
        VALUES %s
        ON CONFLICT (report_date, store_name) DO UPDATE SET
            pickup_point_id = EXCLUDED.pickup_point_id,
            morning_orders = EXCLUDED.morning_orders,
            cancelled_orders = EXCLUDED.cancelled_orders,
            actual_orders = EXCLUDED.actual_orders,
            on_time_orders = EXCLUDED.on_time_orders,
            delayed_orders = EXCLUDED.delayed_orders,
            otd_percent = EXCLUDED.otd_percent,
            open_delays = EXCLUDED.open_delays,
            updated_at = NOW()
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())"

    with db_connection() as (_, cur):
        execute_values(cur, sql, values, template=template, page_size=500)
        return cur.rowcount


def get_daily_otd(report_date):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT *
            FROM daily_otd
            WHERE report_date = %s
            ORDER BY store_name
            """,
            (report_date,),
        )
        return cur.fetchall()


def get_otd_history(start_date, end_date):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT
                report_date,
                SUM(morning_orders)::int AS morning_orders,
                SUM(cancelled_orders)::int AS cancelled_orders,
                SUM(actual_orders)::int AS actual_orders,
                SUM(on_time_orders)::int AS on_time_orders,
                SUM(delayed_orders)::int AS delayed_orders,
                CASE
                    WHEN SUM(actual_orders) > 0
                    THEN ROUND(SUM(on_time_orders)::numeric / SUM(actual_orders) * 100, 2)
                    ELSE NULL
                END AS otd_percent,
                SUM(open_delays)::int AS open_delays
            FROM daily_otd
            WHERE report_date BETWEEN %s AND %s
            GROUP BY report_date
            ORDER BY report_date DESC
            """,
            (start_date, end_date),
        )
        return cur.fetchall()


def get_period_store_otd(start_date, end_date):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT
                store_name,
                MAX(pickup_point_id) AS pickup_point_id,
                SUM(morning_orders)::int AS morning_orders,
                SUM(cancelled_orders)::int AS cancelled_orders,
                SUM(actual_orders)::int AS actual_orders,
                SUM(on_time_orders)::int AS on_time_orders,
                SUM(delayed_orders)::int AS delayed_orders,
                CASE
                    WHEN SUM(actual_orders) > 0
                    THEN ROUND(SUM(on_time_orders)::numeric / SUM(actual_orders) * 100, 2)
                    ELSE NULL
                END AS otd_percent,
                MAX(open_delays)::int AS open_delays
            FROM daily_otd
            WHERE report_date BETWEEN %s AND %s
            GROUP BY store_name
            ORDER BY store_name
            """,
            (start_date, end_date),
        )
        return cur.fetchall()


def get_store_period_otd(store_name, start_date, end_date):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT
                report_date,
                store_name,
                morning_orders,
                cancelled_orders,
                actual_orders,
                on_time_orders,
                delayed_orders,
                otd_percent,
                open_delays
            FROM daily_otd
            WHERE store_name = %s
              AND report_date BETWEEN %s AND %s
            ORDER BY report_date DESC
            """,
            (store_name, start_date, end_date),
        )
        return cur.fetchall()


def get_period_snapshots(start_date, end_date):
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT *
            FROM daily_order_snapshot
            WHERE report_date BETWEEN %s AND %s
            ORDER BY report_date, store_name, order_code
            """,
            (start_date, end_date),
        )
        return cur.fetchall()


def get_report_date_bounds():
    with db_connection(dict_cursor=True) as (_, cur):
        cur.execute(
            """
            SELECT
                MIN(report_date) AS min_date,
                MAX(report_date) AS max_date
            FROM daily_otd
            """
        )
        return cur.fetchone()
