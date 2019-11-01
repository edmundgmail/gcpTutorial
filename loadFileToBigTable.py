import datetime
from google.cloud import bigtable
from google.cloud.bigtable import column_family
import csv
import gcsfs


project_id='big-data-on-gcp'
instance_id='inst-employee'
table_id='sales1'
column_family_id = 'cf1'
bucket_name='big_data_landing_zone'
filename='sales data-set.csv'

def createTable(project_id, instance_id, table_id, column_family_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    table = instance.table(table_id)
    print('Creating column family cf1 with Max Version GC rule...')
    # Create a column family with GC policy : most recent N versions
    # Define the GC policy to retain only the most recent 2 versions
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))
    return table

def loadCsvFileToBigTable(project_id, table, column_family_id, bucket_name, filename):
    column_encoded = []
    table_rows=[]
    fs = gcsfs.GCSFileSystem(project=project_id)

    #bigtable limit on the number of cells which could be written in 1 shot
    batch_limit = 100000

    #number of columns
    num_columns = 1

    with fs.open(bucket_name+'/'+filename, 'rt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                #update the batch_size based on the number of columns
                num_columns = len(column_encoded)
                # calls the lambda function to encode list of column names
                column_encoded=list(map(lambda x: x.encode(), row))
                line_count += 1

            else:
                #construnct row-key
                row_key = "#".join(row[:3]).encode()
                #create a row
                table_row = table.row(row_key)
                #add teh column values to the table_row
                for i, column in enumerate(column_encoded):
                    table_row.set_cell(column_family_id, column, row[i], timestamp=datetime.datetime.utcnow())

                #append the table_row to a list
                table_rows.append(table_row)

                if len(table_rows) * num_columns >= batch_limit:
                    table.mutate_rows(table_rows)
                    table_rows=[]
                line_count += 1

    if len(table_rows)>0:
        table.mutate_rows(table_rows)

table = createTable(project_id, instance_id, table_id, column_family_id)
loadCsvFileToBigTable(project_id, table, column_family_id, bucket_name, filename)

def temp():
    print('Writing some greetings to the table.')
    greetings = ['Hello World!', 'Hello Cloud Bigtable!', 'Hello Python!']
    rows = []
    column = 'greeting'.encode()
    for i, value in enumerate(greetings):
        # Note: This example uses sequential numeric IDs for simplicity,
        # but this can result in poor performance in a production
        # application.  Since rows are stored in sorted order by key,
        # sequential keys can result in poor distribution of operations
        # across nodes.
        #
        # For more information about how to design a Bigtable schema for
        # the best performance, see the documentation:
        #
        #     https://cloud.google.com/bigtable/docs/schema-design
        row_key = 'greeting{}'.format(i).encode()
        row = table.row(row_key)
        row.set_cell(column_family_id,
                     column,
                     value,
                     timestamp=datetime.datetime.utcnow())
        rows.append(row)
    table.mutate_rows(rows)
#temp()