import datetime
import os
import pandas as pd
import valohai
from snowflake.snowpark import Session

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

def get_login_token():
    try:
        with open("/snowflake/session/token", "r") as f:
            return f.read()
    except Exception as e:
        print(f"Error reading token file: {e}")
        return None

if __name__ == "__main__":
    session = None
    try:
        configs = {
            "account": SNOWFLAKE_ACCOUNT,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
            "host": SNOWFLAKE_HOST,
            "token": get_login_token(),
            "authenticator": "oauth",
            "role": SNOWFLAKE_ROLE,
        }
        print("Connecting with credentials:", configs)
        session = Session.builder.configs({k: v for k, v in configs.items() if v is not None}).create()
        session.use_role(SNOWFLAKE_ROLE)
        session.use_database("insurance")
        session.use_schema("ml_pipe")
        session.use_warehouse(SNOWFLAKE_WAREHOUSE)
        print(f"Connected to database: {session.get_current_database()}, schema: {session.get_current_schema()}")

        insurance_table = session.table("source_of_truth")
        insurance_rows = insurance_table.collect()
        
        print("Insurance Table Data has %d rows:", len(insurance_rows))
        
        timestamp = datetime.datetime.now().isoformat()
        filename = f"insurance_data_{timestamp}.csv"
        
        output = valohai.outputs().path(filename)
        print(f"Writing data to {output}")
        df = pd.DataFrame([r.as_dict() for r in insurance_rows])
        df.to_csv(output, index=False)
        print(f"Data written to {output}")
        
        
    except Exception as e:
        print(f"Error creating Snowflake session: {e}")
    
    if session:
        session.close()