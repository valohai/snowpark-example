import datetime as dt
import valohai
import pandas as pd

from snowflake.ml._internal.utils import sql_identifier

from utils.connection import create_session

if __name__ == "__main__":
    db_name = valohai.parameters("db_name").value
    schema_name = valohai.parameters("schema_name").value
    warehouse_name = valohai.parameters("warehouse_name").value
    role = valohai.parameters("role").value

    incoming_data_db_name = valohai.parameters("incoming_data_db_name").value
    number_of_rows = valohai.parameters("number_of_rows").value
    landing_table_name = valohai.parameters("landing_table_name").value

    session = create_session(role, db_name, schema_name, warehouse_name)
    if session is None:
        raise RuntimeError("Failed to create Snowflake session")

    print("\nConnection Established with the following parameters:")
    print("Role                        : {}".format(session.get_current_role()))
    print("Database                    : {}".format(session.get_current_database()))
    print("Schema                      : {}".format(session.get_current_schema()))
    print("Warehouse                   : {}".format(session.get_current_warehouse()))

    # Ensure identifer name
    sql_identifier.SqlIdentifier(landing_table_name)
    sql_identifier.SqlIdentifier(incoming_data_db_name)

    selected_data = session.sql(f"SELECT * FROM {incoming_data_db_name} LIMIT {number_of_rows};")
    # Simulate streamed data
    query = f"""
INSERT INTO {landing_table_name} (
    AGE,
    GENDER,
    BMI,
    CHARGES,
    CHILDREN,
    SMOKER,
    REGION,
    MEDICAL_HISTORY,
    FAMILY_MEDICAL_HISTORY,
    EXERCISE_FREQUENCY,
    OCCUPATION,
    COVERAGE_LEVEL
)
SELECT
    AGE,
    GENDER,
    BMI,
    CHARGES,
    CHILDREN,
    SMOKER,
    REGION,
    MEDICAL_HISTORY,
    FAMILY_MEDICAL_HISTORY,
    EXERCISE_FREQUENCY,
    OCCUPATION,
    COVERAGE_LEVEL
FROM {incoming_data_db_name}
LIMIT ?;
"""

    params = [
        number_of_rows,
    ]
    result = session.sql(query, params).collect()
    print(result)

    # Optionally, you can output the result to a CSV file
    timestamp = dt.datetime.now().isoformat()
    output_path = valohai.outputs().path(f"simulated_data_{timestamp}.csv")
    df = pd.DataFrame([r.as_dict() for r in selected_data.collect()])
    df.to_csv(output_path, index=False)
    print(f"Simulated data written to {output_path}")
