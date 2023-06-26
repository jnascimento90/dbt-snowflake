# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import json
import pandas as pd

def main(session: snowpark.Session):
    
    tableName = 'public._AIRBYTE_RAW_PEOPLE_RAW'
    dataframe = session.table(tableName)
    
    #df = dataframe['_AIRBYTE_DATA']

    # transform the dataframe into a pandas df
    dataframe_pd = dataframe.toPandas()

    # subselect the data column
    airbyte_data = dataframe.select(col("_AIRBYTE_DATA"))

    # new dataframe which will hold the airbyte data split into columns
    airbyte_data_split = pd.DataFrame()

    # iterate through every row
    for itertator in airbyte_data.collect():

        # change from str to dict
        res = json.loads(itertator["_AIRBYTE_DATA"])

        # create a dataframe for that row and append it to the main one
        row_df = pd.DataFrame(res, index=[0])
        airbyte_data_split = pd.concat([airbyte_data_split, row_df], ignore_index=True)

        
    # join with the original dataframe and remove the old column data
    all_df = airbyte_data_split.join(dataframe_pd)
    all_df = all_df.drop(['_AIRBYTE_DATA'], axis=1)

    # create a new snowpark dataframe from this pandas dataset
    new_dataframe = session.create_dataframe(all_df)
    
    # Print a sample of the dataframe to standard output.
    new_dataframe.show()
    
    #newTableName = 'public._AIRBYTE_RAW_SPLIT_JSON'
    #new_dataframe = session.write_pandas(new_dataframe, newTableName)

    # Return value will appear in the Results tab.
    return new_dataframe
