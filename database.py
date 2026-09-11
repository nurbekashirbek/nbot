import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL is not set")

    return psycopg2.connect(
        database_url,
        sslmode="require"
    )


def test_connection():
    connection = None

    try:
        connection = get_connection()

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT NOW() AS server_time;")
            result = cursor.fetchone()

        logging.info(f"Database connection successful. Server time: {result['server_time']}")
        return True

    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return False

    finally:
        if connection:
            connection.close()


def get_table_counts():
    connection = None

    try:
        connection = get_connection()

        result = {}

        with connection.cursor() as cursor:
            for table_name in [
                "orders",
                "order_status_history",
                "daily_order_snapshot",
                "daily_otd"
            ]:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name};"
                )

                result[table_name] = cursor.fetchone()[0]

        return result

    except Exception as e:
        logging.error(f"Failed to get table counts: {e}")
        return None

    finally:
        if connection:
            connection.close()
