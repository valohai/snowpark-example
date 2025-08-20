import datetime as dt
import valohai
from opentelemetry import trace

from utils.predictor import InsurancePredictor
from utils.connection import create_session

if __name__ == "__main__":
    db_name = valohai.parameters("db_name").value
    schema_name = valohai.parameters("schema_name").value
    warehouse_name = valohai.parameters("warehouse_name").value
    role = valohai.parameters("role").value

    streamdb_name = valohai.parameters("streamdb_name").value
    model_name = valohai.parameters("model_name").value
    targetdb_name = valohai.parameters("target_db_name").value

    session = create_session(role, db_name, schema_name, warehouse_name)
    if session is None:
        raise RuntimeError("Failed to create Snowflake session")
    

    print("\nConnection Established with the following parameters:")
    print("Role                        : {}".format(session.get_current_role()))
    print("Database                    : {}".format(session.get_current_database()))
    print("Schema                      : {}".format(session.get_current_schema()))
    print("Warehouse                   : {}".format(session.get_current_warehouse()))

    tracer = trace.get_tracer("insurance_model.predict")
    predictor = InsurancePredictor(
        tracer=tracer,
        session=session,
        schema_name=schema_name,
        streamdb_name=streamdb_name,
    )
    timestamp = dt.datetime.now().isoformat()
    output_path = valohai.outputs().path(f"predictions_{timestamp}.csv")
    predictor.run(model_name=model_name, result_target_db=targetdb_name, export_path=output_path)