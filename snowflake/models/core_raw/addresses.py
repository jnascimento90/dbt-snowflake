import base64
from snowflake.snowpark.functions import md5    
    
def model(dbt, session):
    """
    Custom transformation in Airbyte replication "Children & Addresses" <> Snowflake.
    Both the input and output table are the same, the gotphoto.core_raw.addresses
    in Snowflake. This model masks the STREET column in the table.

    dbt configs are given to make this model incremental. Addional configuration
    are written in models/config.yml:
    
    delta_hours = Lookback period in the Snowflake table.
    timestamp_column = Which column to use for the incremental lookback.

    Returns:
        Snowpark dataframe with masked "STREET" column.
    """
    dbt.config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        merge_update_columns= ['STREET']
        )
    delta_hours = dbt.config.get("delta_hours")
    timestamp_column = dbt.config.get("timestamp_column")

    df = dbt.source('public', 'addresses')
    if dbt.is_incremental:
        updated_at = f"select max({timestamp_column}) - INTERVAL '{delta_hours}' from {dbt.this};"
        df = df.filter(df._airbyte_emitted_at >= session.sql(updated_at).collect()[0][0])
        df = df.with_column("STREET", md5("STREET"))
        
    return df
