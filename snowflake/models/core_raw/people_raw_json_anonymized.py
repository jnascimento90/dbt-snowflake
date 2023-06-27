# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import json
import pandas as pd
import datetime
import base64

def encoding(x):
    x = x.encode("ascii")
    base64_bytes = base64.b64encode(x)
    return base64_bytes.decode("ascii")
    
    
def model(dbt, session: snowpark.Session):
    
    #dbt.config(materialized="table")
    #df = dbt.source('public', 'people_raw_json_split').to_pandas()
    
    tableName = 'public.PEOPLE_RAW_JSON_SPLIT'
    dataframe = session.table(tableName)

    # transform the dataframe into a pandas df
    dataframe_pd = dataframe.toPandas()

    
    dataframe_pd['family_name'] = dataframe_pd['family_name'].apply(encoding)
    del dataframe_pd['given_name']
    dataframe_pd["_AIRBYTE_EMITTED_AT"] = pd.to_datetime(dataframe_pd["_AIRBYTE_EMITTED_AT"],unit='us')

    # create a new snowpark dataframe from this pandas dataset
    new_dataframe = session.create_dataframe(dataframe_pd)
    
    # Print a sample of the dataframe to standard output.
    new_dataframe.show()
    
    # Return value will appear in the Results tab.
    return new_dataframe
