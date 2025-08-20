from __future__ import annotations

import pandas as pd

from snowflake.snowpark import Session

def load_data(
    session: Session,
    csv_path: str,
    train_rows: int,
    train_db_name: str,
    db_name: str,
    schema_name: str,
    incoming_data_db_name: str,
    as_new: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    insurance_df = pd.read_csv(csv_path)

    insurance_df.columns = insurance_df.columns.str.upper()

    # Rearrange columns to fit target schema
    cols = insurance_df.columns.tolist()
    cols = cols[:3] + cols[-1:] + cols[3:-1]
    insurance_df = insurance_df[cols]
    train_df = insurance_df[:train_rows]
    incoming_df = insurance_df[train_rows:]

    if as_new:
        session.sql(f"TRUNCATE TABLE {train_db_name}").collect()
        session.sql(f"TRUNCATE TABLE {incoming_data_db_name}").collect()

    session.write_pandas(
        train_df,
        table_name=train_db_name,  # SOURCE_OF_TRUTH
        database=db_name,
        schema=schema_name,
        auto_create_table=True,
    )

    session.write_pandas(
        incoming_df,
        table_name=incoming_data_db_name,  # INCOMING_DATA_SOURCE
        database=db_name,
        schema=schema_name,
        auto_create_table=True,
    )

    return train_df, incoming_df