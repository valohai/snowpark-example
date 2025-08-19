import os
import valohai
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.table import Table

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

def export_table_to_csv(table: Table):
    df = pd.DataFrame([r.as_dict() for r in table.collect()])
    timestamp = pd.Timestamp.now().isoformat()
    filename = f"{table.table_name}_{timestamp}.csv"
    output = valohai.outputs().path(filename)
    print(f"Writing data to {output}")
    df.to_csv(output, index=False)
    print(f"Data written to {output}")


def main():
    db_name = valohai.parameters("db_name").value
    schema_name = valohai.parameters("schema_name").value
    warehouse_name = valohai.parameters("warehouse_name").value
    role = valohai.parameters("role").value

    train_rows = int(valohai.parameters("train_rows").value)
    train_db_name = valohai.parameters("train_db_name").value
    incoming_data_db_name = valohai.parameters("incoming_data_db_name").value

    data_csv = valohai.inputs("data_csv").path()

    session = create_session(role, db_name, schema_name, warehouse_name)
    print("\nConnection Established with the following parameters:")
    print("Role                        : {}".format(session.get_current_role()))
    print("Database                    : {}".format(session.get_current_database()))
    print("Schema                      : {}".format(session.get_current_schema()))
    print("Warehouse                   : {}".format(session.get_current_warehouse()))

    # Load full 1M dataset into dataframe
    insurance_df = pd.read_csv(data_csv)
    # Capitalize column names
    insurance_df.columns = insurance_df.columns.str.upper()

    # Rearrange columns to fit target schema
    cols = insurance_df.columns.tolist()
    cols = cols[:3] + cols[-1:] + cols[3:-1]
    insurance_df = insurance_df[cols]

    source_of_truth_df = session.write_pandas(
        insurance_df[:train_rows],
        table_name=train_db_name,  # SOURCE_OF_TRUTH
        database=db_name,
        schema=schema_name,
        auto_create_table=True,
    )

    incoming_data_source_df = session.write_pandas(
        insurance_df[train_rows:],
        table_name=incoming_data_db_name,  # INCOMING_DATA_SOURCE
        database=db_name,
        schema=schema_name,
        auto_create_table=True,
    )

    export_table_to_csv(source_of_truth_df)
    export_table_to_csv(incoming_data_source_df)

if __name__ == "__main__":
    main()