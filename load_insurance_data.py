import valohai
import pandas as pd

from utils.connection import create_session
from utils.data_loader import load_data


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

    train_df, incoming_df = load_data(
        session,
        csv_path=data_csv,
        train_rows=train_rows,
        train_db_name=train_db_name,
        db_name=db_name,
        schema_name=schema_name,
        incoming_data_db_name=incoming_data_db_name,
        as_new=True,  # Overwrite existing tables
    )

    timestamp = pd.Timestamp.now().isoformat()
    train_df_output = valohai.outputs().path(f"train_df_{timestamp}.csv")
    train_df.to_csv(train_df_output, index=False)

    incoming_df_output = valohai.outputs().path(f"incoming_df_{timestamp}.csv")
    incoming_df.to_csv(incoming_df_output, index=False)

if __name__ == "__main__":
    main()