import base64
import json
import pandas as pd
    
def model(dbt, session):
    """
    Custom transformation in Airbyte replication 

    """
    dbt.config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
        )
    delta_hours = dbt.config.get("delta_hours")
    timestamp_column = dbt.config.get("timestamp_column")

    df = dbt.source('public', 'hubspot')
    if dbt.is_incremental:
        updated_at = f"SELECT MAX({timestamp_column}) - INTERVAL '{delta_hours}' FROM {dbt.this};"
        df = df.filter(df['_AIRBYTE_EMITTED_AT'] >= session.sql(updated_at).collect()[0][0]).to_pandas()
        df = pd.json_normalize(data=df['_AIRBYTE_DATA'].apply(json.loads), sep='_')
        
    return df
