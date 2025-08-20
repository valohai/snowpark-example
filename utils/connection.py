import os

from snowflake.snowpark import Session


SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")


def get_login_token() -> str:
    try:
        with open("/snowflake/session/token", "r") as f:
            return f.read()
    except Exception as e:
        print(f"Error reading token file: {e}")
        return None


def get_session_config() -> dict[str, str]:
    configs = {
        "account": SNOWFLAKE_ACCOUNT,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "host": SNOWFLAKE_HOST,
        "token": get_login_token(),
        "authenticator": "oauth",
        "role": SNOWFLAKE_ROLE,
    }

    return {k: v for k, v in configs.items() if v is not None}


def create_session(
    role: str | None = None,
    db_name: str | None = None,
    schema_name: str | None = None,
    warehouse_name: str | None = None,
) -> Session | None:
    session = Session.builder.configs(get_session_config()).create()
    if role and role != SNOWFLAKE_ROLE:
        session.use_role(role)

    if db_name and db_name != SNOWFLAKE_DATABASE:
        session.use_database(db_name)

    if schema_name and schema_name != SNOWFLAKE_SCHEMA:
        session.use_schema(schema_name)

    if warehouse_name and warehouse_name != SNOWFLAKE_WAREHOUSE:
        session.use_warehouse(warehouse_name)

    return session
