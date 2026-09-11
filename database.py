import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL is not set in Render Environment")

    return psycopg2.connect(
        database_url,
        sslmode="require",
        connect_timeout=15
    )


def test_connection():
    connection = None

    try:
        connection = get_connection()

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT NOW() AS server_time;")
            result = cursor.fetchone()

        logging.info(
            f"Database connection successful. "
            f"Server time: {result['server_time']}"
        )

        return True, "Connection successful"

    except psycopg2.OperationalError as e:
        error_text = str(e)

        logging.error(
            f"PostgreSQL connection error: {error_text}"
        )

        if "password authentication failed" in error_text.lower():
            return False, "Wrong database password"

        if "could not translate host name" in error_text.lower():
            return False, "Database hostname cannot be resolved"

        if "network is unreachable" in error_text.lower():
            return False, "Database network is unreachable. Use Supabase Session pooler."

        if "timeout expired" in error_text.lower():
            return False, "Database connection timed out"

        if "no password supplied" in error_text.lower():
            return False, "Database password is missing"

        return False, "PostgreSQL connection error. Check Render logs."

    except Exception as e:
        logging.error(
            f"Database connection failed: {e}"
        )

        return False, str(e)

    finally:
        if connection:
            connection.close()


def get_table_counts():
    connection = None

    try:
        connection = get_connection()

        result = {}

        with connection.cursor() as cursor:

            tables = [
                "orders",
                "order_status_history",
                "daily_order_snapshot",
                "daily_otd"
            ]

            for table_name in tables:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name};"
                )

                result[table_name] = cursor.fetchone()[0]

        return result

    except Exception as e:
        logging.error(
            f"Failed to get table counts: {e}"
        )

        return None

    finally:
        if connection:
            connection.close()
