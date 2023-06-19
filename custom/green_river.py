import pandas as pd
import plotly as px
from google.oauth2 import service_account
from google.cloud import bigquery
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



# Set the path to the Google Cloud credentials JSON file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/secrets/charming-sonar-346314-90cd9e0f3b0a.json'

# Retrieve the credentials for the BigQuery API from the credentials JSON file
credentials = service_account.Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

@custom
def transform_custom(*args, **kwargs):
    # Initialize the SQL query to select all columns from the revenue table in the dbt_prod dataset
    sql_query = """
    select * from dbt_prod.mart_subscriptions
    """

    # Import the data from BigQuery into a Pandas DataFrame using pd.read_gbq()
    # Pass the SQL query, project ID, and credentials as arguments
    df = pd.read_gbq(sql_query, project_id=credentials.project_id, credentials=credentials)
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
