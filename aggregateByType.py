from google.api_core import exceptions
from google.cloud import bigquery

def createTable(dataset_id, table_id):
    # Adds test Bigquery data, yields the project ID and then tears down.
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    # DO NOT SUBMIT: trim this down once we find out what works
    table.schema = (
        bigquery.SchemaField("Type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Sale_Date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("Total_Sales", "FLOAT")
    )

    try:
        table = bigquery_client.create_table(table)
    except exceptions.Conflict:
        pass
    return table_ref

def loadTable(request):
    table_ref = createTable('sales_data', 'agg_by_type')
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    job_config.destination = table_ref
    job_config.destination
    sql = """
        select t.Type, a.Date as Sale_Date, sum(a.Weekly_Sales) as Total_Sales from sales_data.store AS t, sales_data.sales AS a where t.store=a.store group by t.type, a.Date
    """

    # Start the query, passing in the extra configuration.
    query_job = bigquery.Client().query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))


#loadTable("")
table_ref = createTable('sales_data', 'agg_by_type')