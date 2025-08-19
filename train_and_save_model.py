from __future__ import annotations
import os
import traceback
import valohai
import numpy as np
import json

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import snowflake.ml.modeling.preprocessing as snowmlpp

from pandas import DataFrame as pd_DataFrame
from opentelemetry import trace
from snowflake.snowpark import Session
from snowflake import telemetry
from snowflake.snowpark import DataFrame
from snowflake.ml.registry import registry
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.modeling.model_selection import GridSearchCV
from snowflake.ml.modeling.metrics import (
    mean_absolute_percentage_error,
    mean_squared_error,
)

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
    config = get_session_config()
    print(f"Creating session with config: {config}")
    session = Session.builder.configs(config).create()
    if role and role != SNOWFLAKE_ROLE:
        session.use_role(role)

    if db_name and db_name != SNOWFLAKE_DATABASE:
        session.use_database(db_name)

    if schema_name and schema_name != SNOWFLAKE_SCHEMA:
        session.use_schema(schema_name)

    if warehouse_name and warehouse_name != SNOWFLAKE_WAREHOUSE:
        session.use_warehouse(warehouse_name)

    return session


class InsuranceTrainer:
    LABEL_COLUMNS = ["CHARGES"]
    OUTPUT_COLUMNS = ["PREDICTED_CHARGES"]

    def __init__(
        self,
        session: Session,
        tracer: trace.Tracer,
        source_of_truth: str,
        schema_name: str,
        major_version: bool = True,
    ):
        self.session = session
        self.tracer = tracer
        self.source_of_truth = source_of_truth
        self.schema_name = schema_name
        self.major_version = major_version

    def load_data(self) -> DataFrame:
        print("Loading data from source table: ", self.source_of_truth)
        with self.tracer.start_as_current_span("data_loading"):
            df = self.session.table(self.source_of_truth).limit(10000)
            telemetry.set_span_attribute("data.row_count", df.count())
            print("Loaded data:")
            df.show()
            return df

    def get_feature_column_names(self, df: DataFrame) -> list[str]:
        return [i for i in df.schema.names if i not in self.LABEL_COLUMNS]

    def get_ohe_columns(self, df: DataFrame) -> list[str]:
        categorical_types = [T.StringType]
        cols_to_ohe = [
            col.name
            for col in df.schema.fields
            if (type(col.datatype) in categorical_types)
        ]
        return cols_to_ohe

    def get_ohe_cols_output(self, cols_to_ohe: list[str]) -> list[str]:
        return [col + "_OHE" for col in cols_to_ohe]

    def engineer_features(
        self,
        df: DataFrame,
        cols_to_ohe: list[str],
    ) -> DataFrame:
        print("Starting feature engineering on columns: ", cols_to_ohe)
        with self.tracer.start_as_current_span("feature_engineering"):

            def fix_values(columnn):
                return F.upper(F.regexp_replace(F.col(columnn), "[^a-zA-Z0-9]+", "_"))

            for col in cols_to_ohe:
                df = df.na.fill("NONE", subset=col)
                df = df.withColumn(col, fix_values(col))
            telemetry.add_event("feature_engineering_complete")
            print("Feature engineering completed:")
            df.show()
            return df

    def define_pipeline(
        self,
        cols_to_ohe: list[str],
        ohe_cols_output: list[str],
    ) -> Pipeline:
        print("Defining pipeline with OHE columns: ", ohe_cols_output)
        with self.tracer.start_as_current_span("define_pipeline"):
            # Define the pipeline
            pipe = Pipeline(
                steps=[
                    (
                        "ohe",
                        snowmlpp.OneHotEncoder(
                            input_cols=cols_to_ohe,
                            output_cols=ohe_cols_output,
                            drop_input_cols=True,
                        ),
                    ),
                    (
                        "grid_search_reg",
                        GridSearchCV(
                            estimator=XGBRegressor(),
                            param_grid={
                                "n_estimators": [50, 100, 200],
                                "learning_rate": [0.01, 0.1, 0.5],
                            },
                            n_jobs=-1,
                            scoring="neg_mean_absolute_percentage_error",
                            input_cols=None,
                            label_cols=self.LABEL_COLUMNS,
                            output_cols=self.OUTPUT_COLUMNS,
                            drop_input_cols=True,
                        ),
                    ),
                ]
            )
            return pipe

    def train_test_split(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        print("Splitting data into train and test sets")
        with self.tracer.start_as_current_span("train_test_split"):
            return df.randomSplit([0.8, 0.2], seed=42)

    def fit_pipeline(self, pipe: Pipeline, train_df: pd_DataFrame) -> Pipeline:
        print("Fitting pipeline to training data")
        train_df.head(10)
        with self.tracer.start_as_current_span("fit_pipeline"):
            try:
                pipe.fit(train_df)
                telemetry.set_span_attribute("training.param_grid", "Fitting done")
                return pipe
            except Exception as e:
                print("Error during pipeline fitting: ", e)
                for step in pipe.steps:
                    print(f"Step: {step[0]}")
                    print(f"  - Type: {type(step[1])}")
                    print(f"  - Input cols: {step[1].input_cols}")
                raise e

    def evaluate_model(
        self,
        pipe: Pipeline,
        test_df: DataFrame,
    ) -> tuple[float, float]:
        print("Evaluating model on test data")
        with self.tracer.start_as_current_span("model_evaluation"):
            results = pipe.predict(test_df)

            # Calculate MAPE
            mape = mean_absolute_percentage_error(
                df=results,
                y_true_col_names=self.LABEL_COLUMNS,
                y_pred_col_names=self.OUTPUT_COLUMNS,
            )
            print("Model evaluation completed with MAPE: %f", mape)

            # Calculate MSE
            mse = mean_squared_error(
                df=results,
                y_true_col_names=self.LABEL_COLUMNS,
                y_pred_col_names=self.OUTPUT_COLUMNS,
            )
            print("Model evaluation completed with MSE: ", mse)
            telemetry.set_span_attribute("model.mape", mape)
            telemetry.set_span_attribute("model.mse", mse)
            return mape, mse

    def set_model_version(self, registry_object: registry.Registry, model_name: str):
        # See what we've logged so far, dynamically set the model version
        model_list = registry_object.show_models()

        if len(model_list) == 0:
            return "V1"

        model_list_filter = model_list[model_list["name"] == model_name]

        if len(model_list_filter) == 0:
            return "V1"

        version_list_string = model_list_filter["versions"].iloc[0]
        version_list = json.loads(version_list_string)
        version_numbers = [float(s.replace("V", "")) for s in version_list]
        model_last_version = max(version_numbers)

        if np.isnan(model_last_version):
            model_new_version = "V1"

        elif not np.isnan(model_last_version) and self.major_version:
            model_new_version = round(model_last_version + 1, 2)
            model_new_version = "V" + str(model_new_version)

        else:
            model_new_version = round(model_last_version + 0.1, 2)
            model_new_version = "V" + str(model_new_version)

        return model_new_version

    def register_model(
        self,
        train_df: DataFrame,
        pipe: Pipeline,
        mape: float,
        mse: float,
        schema_name: str,
        feature_column_names: list[str],
        major_version: bool = True,
    ) -> None:
        print("Registering model in the registry")
        with self.tracer.start_as_current_span("model_registration"):
            # Create model regisry object

            model_registry = registry.Registry(
                session=session,
                database_name=session.get_current_database(),
                schema_name=schema_name,
            )

            # Save model to registry
            X = train_df.select(feature_column_names).limit(100)

            model_name = "INSURANCE_CHARGES_PREDICTION"
            version_name = self.set_model_version(
                model_registry,
                model_name,
            )
            model_version = model_registry.log_model(
                model=pipe,
                model_name=model_name,
                version_name=version_name,
                sample_input_data=X,
            )

            model_version.set_metric(metric_name="mean_abs_pct_err", value=mape)
            model_version.set_metric(metric_name="mean_sq_err", value=mse)
            telemetry.add_event("model_registered", {"version": version_name})

            session.sql(
                f'alter model INSURANCE_CHARGES_PREDICTION set default_version = "{version_name}";'
            )

    def run(self):
        with self.tracer.start_as_current_span("train_save_ins_model"):
            try:
                telemetry.set_span_attribute(
                    "model.name",
                    "INSURANCE_CHARGES_PREDICTION",
                )
                df = self.load_data()
                feature_column_names = self.get_feature_column_names(df)
                cols_to_ohe = self.get_ohe_columns(df)
                ohe_cols_output = self.get_ohe_cols_output(cols_to_ohe)

                df = self.engineer_features(df, cols_to_ohe)
                pipe = self.define_pipeline(
                    cols_to_ohe,
                    ohe_cols_output,
                )

                train_df, test_df = self.train_test_split(df)

                # Force conversion to pandas DataFrame for fitting
                # otherwise, Snowpark won't use the local environment to fit
                # and will try to fit on the Snowflake cluster, which we would
                # have to sort out the dependencies for.
                local_train_df = train_df.to_pandas()

                fit_pipe = self.fit_pipeline(pipe, local_train_df)
                mape, mse = self.evaluate_model(fit_pipe, test_df)

                self.register_model(
                    train_df=train_df,
                    pipe=fit_pipe,
                    mape=mape,
                    mse=mse,
                    schema_name=self.schema_name,
                    major_version=self.major_version,
                    feature_column_names=feature_column_names,
                )
            except Exception as e:
                telemetry.add_event(
                    "pipeline_failure",
                    {"error": str(e), "stack_trace": traceback.format_exc()},
                )
                raise e


if __name__ == "__main__":
    db_name = valohai.parameters("db_name").value
    schema_name = valohai.parameters("schema_name").value
    warehouse_name = valohai.parameters("warehouse_name").value
    role = valohai.parameters("role").value
    source_table = valohai.parameters("source_table").value

    session = create_session(role, db_name, schema_name, warehouse_name)
    if session is None:
        raise RuntimeError("Failed to create Snowflake session")

    print("\nConnection Established with the following parameters:")
    print("Role                        : {}".format(session.get_current_role()))
    print("Database                    : {}".format(session.get_current_database()))
    print("Schema                      : {}".format(session.get_current_schema()))
    print("Warehouse                   : {}".format(session.get_current_warehouse()))

    tracer = trace.get_tracer("insurance_model.train")
    trainer = InsuranceTrainer(
        tracer=tracer,
        session=session,
        schema_name=schema_name,
        source_of_truth=source_table,
    )
    trainer.run()
