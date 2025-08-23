import streamlit as st
from snowflake.ml.registry import registry

from utils.cached import get_model_metric, get_model_versions, get_models_list, parse_model_details


# Create model registry
model_registry: registry.Registry = st.session_state.model_registry


st.title("Model Catalog")
st.write("This page allows the user to view various aspects of the models saved in the model registry.\
         Select a model to see the available versions.")

# Get models list (cached)
models_df_main = get_models_list(model_registry)

# Select model
model_name = st.selectbox("Model", models_df_main.name if hasattr(models_df_main, 'name') else [])


# Select model
if model_name:
    versions_df = get_model_versions(model_name, model_registry)
    st.write(versions_df)

    st.subheader('Select a version to view more details')
    version = st.selectbox("Version", versions_df.name if hasattr(versions_df, 'name') else [])

    if version:
        # Create 2 columns
        col1, col2, col3 = st.columns(3)

        model_details = parse_model_details(versions_df, version)

    # Column 1: Stats
    with col1:
        st.subheader("Stats")
        
        # Get MAPE metric (cached)
        mape_value = get_model_metric(model_name, version, model_registry)
        if mape_value is not None:
            st.write(f"**MAPE:** {round(mape_value, 4)}")
        else:
            st.write("**MAPE:** Not available")
        
        # Show creation date
        if model_details['success'] and model_details['date']:
            st.write(f"**Created on:** {model_details['date']}")
        else:
            st.write("**Created on:** Not available")
    
    with col2:
        st.subheader("Expected Inputs")
        
        if model_details['success'] and model_details['inputs']:
            inputs_text = "\n".join([f"• {inp}" for inp in model_details['inputs']])
            st.markdown(f"""
            <div style="overflow-y: auto; max-height: 300px; padding: 10px; border: 1px solid #ddd; border-radius: 5px;">
            {inputs_text}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.write("Expected inputs not available for this model")

    with col3:
        st.subheader("Expected Outputs")
        
        if model_details['success'] and model_details['outputs']:
            outputs_text = "\n".join([f"• {out}" for out in model_details['outputs']])
            st.markdown(f"""
            <div style="overflow-y: auto; max-height: 300px; padding: 10px; border: 1px solid #ddd; border-radius: 5px;">
            {outputs_text}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.write("Expected outputs not available for this model")
