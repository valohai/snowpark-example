import os
import json
import pandas as pd
import streamlit as st

from datetime import datetime

from snowflake.snowpark import Session
from snowflake.ml.registry import registry


@st.cache_resource()
def get_login_token() -> str:
    try:
        with open("/snowflake/session/token", "r") as f:
            return f.read()
    except Exception as e:
        print(f"Error reading token file: {e}")
        return None


@st.cache_resource()
def connect_to_snowflake() -> Session:
    configs = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "host": os.getenv("SNOWFLAKE_HOST"),
        "token": get_login_token(),
        "authenticator": "oauth",
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    }
    configs = {k: v for k, v in configs.items() if v is not None}
    return Session.builder.configs(configs).create()

# Cache models list
@st.cache_data
def get_models_list(_model_registry: registry.Registry):
    return _model_registry.show_models()

@st.cache_data
def get_model_versions(model_name: str, _model_registry: registry.Registry):
    return _model_registry.get_model(model_name).show_versions()

@st.cache_data
def get_model_metric(model_name: str, version: str, _model_registry: registry.Registry):
    return _model_registry.get_model(model_name).version(f'"{version}"').get_metric('mean_abs_pct_err')

@st.cache_data
def parse_model_details(models_df: pd.DataFrame, version: str):
    """Parse model details including inputs/outputs and creation date"""
    try:
        # Get user_data for the specific version
        user_data_str = models_df.loc[models_df['name'] == version, 'user_data'].iloc[0]
        user_data_json = json.loads(user_data_str)
        
        # Get creation date
        model_date_str = str(models_df.loc[models_df['name'] == version, 'created_on'].iloc[0])
        date_object = datetime.strptime(model_date_str, "%Y-%m-%d %H:%M:%S.%f%z")
        formatted_date = date_object.strftime("%B %d, %Y %H:%M:%S")
        
        # Parse inputs and outputs
        predict_function = next(
            (func for func in user_data_json['snowpark_ml_data']['functions'] 
             if func['name'] == 'PREDICT'), None
        )
        
        inputs = []
        outputs = []
        
        if predict_function:
            inputs = [inp['name'] for inp in predict_function['signature']['inputs']]
            outputs = [out['name'] for out in predict_function['signature']['outputs']]
        
        return {
            'date': formatted_date,
            'inputs': inputs,
            'outputs': outputs,
            'success': True
        }
    except Exception as e:
        return {
            'date': None,
            'inputs': [],
            'outputs': [],
            'success': False,
            'error': str(e)
        }
    
