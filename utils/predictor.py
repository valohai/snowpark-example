from __future__ import annotations
import traceback
import pandas as pd
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

from snowflake import telemetry
from snowflake.ml.registry import registry
from snowflake.snowpark import Session, MergeResult
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.dataframe import col as column
from opentelemetry import trace


class InsurancePredictor:
    def __init__(
        self,
        session: Session,
        tracer: trace.Tracer,
        streamdb_name: str,
        schema_name: str,
    ):
        self.session = session
        self.tracer = tracer
        self.streamdb_name = streamdb_name
        self.schema_name = schema_name

    def read_from_streamdb(self) -> SnowparkDataFrame:
        with self.tracer.start_as_current_span("read_from_stream"):
            # Get the newly inserted records from the stream
            df = self.session.table(self.streamdb_name).filter(
                column("METADATA$ACTION") == "INSERT"
            )
        return df

    def standardize_values(self, df: SnowparkDataFrame) -> SnowparkDataFrame:
        with self.tracer.start_as_current_span("standardize_values"):
            # Define Snowflake categorical types and determine which columns to OHE
            categorical_types = [T.StringType]
            cols_to_ohe = [
                col.name
                for col in df.schema.fields
                if (type(col.datatype) in categorical_types)
            ]

            def fix_values(columnn):
                return F.upper(F.regexp_replace(F.col(columnn), "[^a-zA-Z0-9]+", "_"))

            for col in cols_to_ohe:
                df = df.na.fill("NONE", subset=col)
                df = df.withColumn(col, fix_values(col))

        return df

    def load_model_from_registry(self, model_name: str) -> registry.ModelVersion:
        with self.tracer.start_as_current_span("load_model_from_registry"):
            model_registry = registry.Registry(
                session=self.session,
                database_name=self.session.get_current_database(),
                schema_name=self.schema_name,
            )
            model_version = model_registry.get_model(model_name).default

        return model_version

    def run_predict(
        self, df: SnowparkDataFrame, model_version: registry.ModelVersion
    ) -> SnowparkDataFrame | pd.DataFrame:
        with self.tracer.start_as_current_span("run_predict_pipeline"):
            results = model_version.run(df, function_name="predict")
        return results

    def write_predictions(
        self,
        predict_results: SnowparkDataFrame | pd.DataFrame,
        target_db_name: str,
        export_path: str | None = None,
    ) -> MergeResult:
        with self.tracer.start_as_current_span("write_predictions"):
            target = self.session.table(target_db_name)
            cols_to_update = {
                col: predict_results[col]
                for col in target.columns
                if "METADATA_UPDATED_AT" not in col
            }
            metadata_col_to_update = {"METADATA_UPDATED_AT": F.current_timestamp()}
            updates = {**cols_to_update, **metadata_col_to_update}
            result = target.merge(
                predict_results,
                target["METADATA$ROW_ID"] == predict_results["METADATA$ROW_ID"],
                [
                    F.when_matched().update(updates),
                    F.when_not_matched().insert(updates),
                ],
            )
            print(f"Merge result: {result}")
            if export_path:
                data = self.session.table(target_db_name)
                df = pd.DataFrame([r.as_dict() for r in data.collect()])
                df.to_csv(export_path, index=False)
                print(f"Predictions written to {export_path}")

    def run(
        self,
        model_name: str,
        result_target_db: str,
        export_path: str | None = None,
    ) -> None:
        try:
            df = self.read_from_streamdb()
            df = self.standardize_values(df)
            model_version = self.load_model_from_registry(model_name)
            results = self.run_predict(df, model_version)
            self.write_predictions(
                predict_results=results,
                target_db_name=result_target_db,
                export_path=export_path,
            )

        except Exception as e:
            telemetry.add_event(
                "predict_pipeline_failure",
                {"error": str(e), "stack_trace": traceback.format_exc()},
            )
            raise  # Re-raise to preserve error handling
