import base64
import pandas

def encoding(x):
    x = x.encode("ascii")
    base64_bytes = base64.b64encode(x)
    return base64_bytes.decode("ascii")
    
    
def model(dbt, session):
    dbt.config(materialized="table")
    
    df = dbt.source('public', 'people_raw_json_split').to_pandas()
    df['family_name'] = df['family_name'].apply(encoding)
    del df['given_name']

    # fix datetime
    df["_AIRBYTE_EMITTED_AT"] = df["_AIRBYTE_EMITTED_AT"]/1000000
    #df["_AIRBYTE_EMITTED_AT"] = pd.to_datetime(df["_AIRBYTE_EMITTED_AT"],unit='s')

    return df
