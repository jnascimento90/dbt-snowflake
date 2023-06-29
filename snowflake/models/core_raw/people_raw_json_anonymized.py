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
    #df["_AIRBYTE_EMITTED_AT"] = pandas.to_datetime(df["_AIRBYTE_EMITTED_AT"],unit='us')
    df["_AIRBYTE_EMITTED_AT"] = df["_AIRBYTE_EMITTED_AT"].dt.tz_localize('UTC')

    session.write_pandas(df, 'PEOPLE_RAW_JSON_SPLIT', overwrite=True)
    #df.to_sql('PEOPLE_RAW_JSON_SPLIT', con=session, if_exists='replace')

    test_df = pandas.DataFrame()
    
    return test_df
