from __future__ import annotations

import time
import valohai
from snowflake.core import Root, CreateMode
from snowflake.core.service import Service, ServiceSpec, ServiceResource
from snowflake.core.exceptions import ConflictError

from utils.connection import create_session

def get_ingress_for_endpoint(svc: ServiceResource, endpoint: str) -> str | None:
    for _ in range(10): # only try 10 times
        # Find the target endpoint.
        target_endpoint = None
        for ep in svc.get_endpoints():
            if ep.is_public and ep.name == endpoint:
                target_endpoint = ep
                break
        else:
            print(f"Endpoint {endpoint} not found")
            return None

        # Return endpoint URL or wait for it to be provisioned.
        if target_endpoint.ingress_url.startswith("Endpoints provisioning "):
            print(f"{target_endpoint.ingress_url} is still in provisioning. Wait for 10 seconds.")
            time.sleep(10)
        else:
            return target_endpoint.ingress_url
        
    print("Timed out waiting for endpoint to become available")


if __name__ == "__main__":
    db_name = valohai.parameters("db_name").value
    schema_name = valohai.parameters("schema_name").value
    warehouse_name = valohai.parameters("warehouse_name").value
    role = valohai.parameters("role").value
    
    streamlit_image = valohai.parameters("streamlit_image").value
    compute_pool_name = valohai.parameters("compute_pool_name").value
    service_name = valohai.parameters("service_name").value

    session = create_session()
    root = Root(session)

    specification = f"""
    spec:
      containers:
      - name: insurance-streamlit
        image: {streamlit_image}
        env:
          SNOWFLAKE_DATABASE: {db_name}
          SNOWFLAKE_SCHEMA: {schema_name}
          SNOWFLAKE_WAREHOUSE: {warehouse_name}
          SNOWFLAKE_ROLE: {role}
        readinessProbe:
          port: 8501
          path: /
      endpoints:
      - name: insurance
        port: 8501
        public: true
    """
    schema = root.databases[db_name].schemas[schema_name]
    try:
        insurance_service = schema.services.create(
            Service(
                name=service_name,
                compute_pool=compute_pool_name,
                spec=ServiceSpec(specification),
                min_instances=1,
                max_instances=1,
            ),
            mode=CreateMode.error_if_exists,
        )
    except ConflictError:
        print(f"Service {service_name} already exists, using the existing one.")
        insurance_service = schema.services[service_name]
        session.use_database(db_name)
        session.use_schema(schema_name)
        session.use_warehouse(warehouse_name)
        session.sql(f"ALTER SERVICE {service_name} RESUME").collect()

    public_url = get_ingress_for_endpoint(insurance_service, "insurance")
    print(f"https://{public_url}/")
