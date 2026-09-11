import os
import logging
from contextlib import contextmanager
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


# ============================================================
# CONNECTION
# ============================================================

def get_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL is not set")

    return psycopg2.connect(
        database_url,
        sslmode="require",
        connect_timeout=15
    )


@contextmanager
def db_connection():
    connection = None

    try:
        connection = get_connection()
        yield connection
        connection.commit()

    except Exception:
        if connection:
            connection.rollback()
        raise

    finally:
        if connection:
            connection.close()


# ============================================================
# TEST
# ============================================================

def test_connection():
    connection = None

    try:
        connection = get_connection()

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:
            cursor.execute(
                "SELECT NOW() AS server_time;"
            )

            result = cursor.fetchone()

        logging.info(
            "Database connected. "
            f"Server time: {result['server_time']}"
        )

        return True, "Connection successful"

    except psycopg2.OperationalError as e:
        error = str(e)
        logging.error(f"Database error: {error}")

        lower = error.lower()

        if "password authentication failed" in lower:
            return False, "Wrong database password"

        if "could not translate host name" in lower:
            return False, "Database hostname cannot be resolved"

        if "network is unreachable" in lower:
            return False, "Database network unreachable"

        if "timeout expired" in lower:
            return False, "Database connection timeout"

        return False, "PostgreSQL connection error"

    except Exception as e:
        logging.exception("Database connection error")
        return False, str(e)

    finally:
        if connection:
            connection.close()


def get_table_counts():
    try:
        with db_connection() as connection:
            with connection.cursor() as cursor:

                result = {}

                for table in [
                    "orders",
                    "order_status_history",
                    "daily_order_snapshot",
                    "daily_otd"
                ]:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table};"
                    )
                    result[table] = cursor.fetchone()[0]

                return result

    except Exception:
        logging.exception("get_table_counts failed")
        return None


# ============================================================
# ORDERS
# ============================================================

def upsert_order(order):
    """
    order = {
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
        delay_minutes
    }
    """

    with db_connection() as connection:

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                INSERT INTO orders (
                    order_code,
                    pickup_point_id,
                    store_name,
                    creation_date,
                    planned_transmission_date,
                    actual_transmission_date,
                    first_seen_at,
                    last_seen_at,
                    current_status,
                    is_cancelled,
                    cancelled_at,
                    was_delayed,
                    is_currently_delayed,
                    delay_started_at,
                    delay_resolved_at,
                    delay_minutes,
                    updated_at
                )
                VALUES (
                    %(order_code)s,
                    %(pickup_point_id)s,
                    %(store_name)s,
                    %(creation_date)s,
                    %(planned_transmission_date)s,
                    %(actual_transmission_date)s,
                    NOW(),
                    NOW(),
                    %(current_status)s,
                    %(is_cancelled)s,
                    CASE
                        WHEN %(is_cancelled)s = TRUE
                        THEN NOW()
                        ELSE NULL
                    END,
                    %(was_delayed)s,
                    %(is_currently_delayed)s,
                    %(delay_started_at)s,
                    %(delay_resolved_at)s,
                    %(delay_minutes)s,
                    NOW()
                )

                ON CONFLICT (order_code)
                DO UPDATE SET

                    pickup_point_id =
                        EXCLUDED.pickup_point_id,

                    store_name =
                        EXCLUDED.store_name,

                    creation_date =
                        COALESCE(
                            EXCLUDED.creation_date,
                            orders.creation_date
                        ),

                    planned_transmission_date =
                        COALESCE(
                            EXCLUDED.planned_transmission_date,
                            orders.planned_transmission_date
                        ),

                    actual_transmission_date =
                        COALESCE(
                            EXCLUDED.actual_transmission_date,
                            orders.actual_transmission_date
                        ),

                    last_seen_at = NOW(),

                    current_status =
                        EXCLUDED.current_status,

                    is_cancelled =
                        EXCLUDED.is_cancelled,

                    cancelled_at =
                        CASE
                            WHEN EXCLUDED.is_cancelled = TRUE
                            THEN COALESCE(
                                orders.cancelled_at,
                                NOW()
                            )
                            ELSE orders.cancelled_at
                        END,

                    was_delayed =
                        orders.was_delayed
                        OR EXCLUDED.was_delayed,

                    is_currently_delayed =
                        EXCLUDED.is_currently_delayed,

                    delay_started_at =
                        COALESCE(
                            orders.delay_started_at,
                            EXCLUDED.delay_started_at
                        ),

                    delay_resolved_at =
                        CASE
                            WHEN
                                EXCLUDED.is_currently_delayed = FALSE
                                AND
                                (
                                    orders.is_currently_delayed = TRUE
                                    OR EXCLUDED.was_delayed = TRUE
                                )
                            THEN COALESCE(
                                EXCLUDED.delay_resolved_at,
                                orders.delay_resolved_at
                            )
                            ELSE orders.delay_resolved_at
                        END,

                    delay_minutes =
                        GREATEST(
                            orders.delay_minutes,
                            EXCLUDED.delay_minutes
                        ),

                    updated_at = NOW()

                RETURNING *;
                """,
                order
            )

            return cursor.fetchone()


def get_order_by_code(order_code):
    with db_connection() as connection:

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT *
                FROM orders
                WHERE order_code = %s;
                """,
                (str(order_code),)
            )

            return cursor.fetchone()


# ============================================================
# STATUS HISTORY
# ============================================================

def save_status_history(order_id, order):
    try:
        with db_connection() as connection:

            with connection.cursor() as cursor:

                cursor.execute(
                    """
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
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        NOW()
                    );
                    """,
                    (
                        order_id,
                        order.get("current_status"),
                        order.get("was_delayed", False),
                        order.get(
                            "is_currently_delayed",
                            False
                        ),
                        order.get(
                            "is_cancelled",
                            False
                        ),
                        order.get(
                            "planned_transmission_date"
                        ),
                        order.get(
                            "actual_transmission_date"
                        )
                    )
                )

    except Exception:
        logging.exception(
            "save_status_history failed"
        )


# ============================================================
# MORNING SNAPSHOT
# ============================================================

def mark_order_morning_snapshot(
    order_id,
    report_date
):
    with db_connection() as connection:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                UPDATE orders
                SET
                    morning_snapshot_date = %s,
                    was_in_morning_snapshot = TRUE,
                    updated_at = NOW()
                WHERE id = %s;
                """,
                (
                    report_date,
                    order_id
                )
            )


def save_daily_snapshot(
    report_date,
    order_id,
    order
):
    with db_connection() as connection:

        with connection.cursor() as cursor:

            cursor.execute(
                """
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
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    TRUE,
                    %s,
                    %s,
                    %s,
                    %s,
                    NOW(),
                    NOW(),
                    NOW()
                )

                ON CONFLICT (
                    report_date,
                    order_id
                )
                DO UPDATE SET

                    order_code =
                        EXCLUDED.order_code,

                    store_name =
                        EXCLUDED.store_name,

                    pickup_point_id =
                        EXCLUDED.pickup_point_id,

                    planned_transmission_date =
                        EXCLUDED.planned_transmission_date,

                    actual_transmission_date =
                        EXCLUDED.actual_transmission_date,

                    final_status =
                        EXCLUDED.final_status,

                    was_cancelled =
                        EXCLUDED.was_cancelled,

                    was_on_time =
                        EXCLUDED.was_on_time,

                    was_delayed =
                        EXCLUDED.was_delayed,

                    delay_minutes =
                        EXCLUDED.delay_minutes,

                    checked_at = NOW(),

                    updated_at = NOW();
                """,
                (
                    report_date,
                    order_id,
                    str(order["order_code"]),
                    order["store_name"],
                    order["pickup_point_id"],
                    order.get(
                        "planned_transmission_date"
                    ),
                    order.get(
                        "actual_transmission_date"
                    ),
                    order.get(
                        "current_status"
                    ),
                    order.get(
                        "is_cancelled",
                        False
                    ),
                    order.get(
                        "was_on_time",
                        False
                    ),
                    order.get(
                        "snapshot_delayed",
                        False
                    ),
                    order.get(
                        "delay_minutes",
                        0
                    )
                )
            )


def get_snapshot_orders(report_date):
    with db_connection() as connection:

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT
                    s.*,
                    o.current_status,
                    o.is_currently_delayed,
                    o.was_delayed
                FROM daily_order_snapshot s
                JOIN orders o
                    ON o.id = s.order_id
                WHERE s.report_date = %s
                ORDER BY
                    s.store_name,
                    s.order_code;
                """,
                (report_date,)
            )

            return cursor.fetchall()


# ============================================================
# OPEN DELAYS
# ============================================================

def get_open_delays():
    with db_connection() as connection:

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT *
                FROM orders
                WHERE
                    is_currently_delayed = TRUE
                    AND is_cancelled = FALSE
                ORDER BY
                    planned_transmission_date,
                    store_name,
                    order_code;
                """
            )

            return cursor.fetchall()


# ============================================================
# DAILY OTD
# ============================================================

def save_daily_otd(
    report_date,
    store_name,
    pickup_point_id,
    morning_orders,
    cancelled_orders,
    actual_orders,
    on_time_orders,
    delayed_orders,
    otd_percent,
    open_delays
):
    with db_connection() as connection:

        with connection.cursor() as cursor:

            cursor.execute(
                """
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
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    NOW(),
                    NOW()
                )

                ON CONFLICT (
                    report_date,
                    store_name
                )
                DO UPDATE SET

                    pickup_point_id =
                        EXCLUDED.pickup_point_id,

                    morning_orders =
                        EXCLUDED.morning_orders,

                    cancelled_orders =
                        EXCLUDED.cancelled_orders,

                    actual_orders =
                        EXCLUDED.actual_orders,

                    on_time_orders =
                        EXCLUDED.on_time_orders,

                    delayed_orders =
                        EXCLUDED.delayed_orders,

                    otd_percent =
                        EXCLUDED.otd_percent,

                    open_delays =
                        EXCLUDED.open_delays,

                    updated_at = NOW();
                """,
                (
                    report_date,
                    pickup_point_id,
                    store_name,
                    morning_orders,
                    cancelled_orders,
                    actual_orders,
                    on_time_orders,
                    delayed_orders,
                    otd_percent,
                    open_delays
                )
            )


def get_daily_otd(report_date):
    with db_connection() as connection:

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT *
                FROM daily_otd
                WHERE report_date = %s
                ORDER BY store_name;
                """,
                (report_date,)
            )

            return cursor.fetchall()


def get_otd_history(days=7):
    with db_connection() as connection:

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT
                    report_date,
                    SUM(morning_orders)
                        AS morning_orders,
                    SUM(cancelled_orders)
                        AS cancelled_orders,
                    SUM(actual_orders)
                        AS actual_orders,
                    SUM(on_time_orders)
                        AS on_time_orders,
                    SUM(delayed_orders)
                        AS delayed_orders,

                    CASE
                        WHEN SUM(actual_orders) > 0
                        THEN ROUND(
                            SUM(on_time_orders)::NUMERIC
                            /
                            SUM(actual_orders)
                            * 100,
                            2
                        )
                        ELSE NULL
                    END AS otd_percent

                FROM daily_otd

                GROUP BY report_date

                ORDER BY report_date DESC

                LIMIT %s;
                """,
                (days,)
            )

            return cursor.fetchall()
