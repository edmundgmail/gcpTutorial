from google.cloud import bigquery
from google.cloud import storage

from zipfile import ZipFile
from zipfile import is_zipfile
import io

def zipExtract(bucketname, zipfilename_with_path, target_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)

    destination_blob_pathname = zipfilename_with_path

    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(target_path+"/"+contentfilename)
                blob.upload_from_string(contentfile)

def listBlobsWithPrefix(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=None)
    ret=[]
    for blob in blobs:
        ret.append(blob.name)
    return ret

def deleteBlobWithPrefix(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=None)
    for blob in blobs:
        blob.delete()
        print('Blob {} deleted.'.format(blob.name))


def loadFileToTable(project_id, dataset_id, table_name, bucket_name, files):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    for file in files:
        uri = "gs://"+bucket_name+"/"+file
        load_job = client.load_table_from_uri(
            uri, dataset_ref.table(table_name), job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(dataset_ref.table(table_name))
        print("Loaded {} rows.".format(destination_table.num_rows))


def loadTable():
    zipExtract('big_data_landing_zone', 'sales data-set.csv.zip', 'unzipped')
    files = listBlobsWithPrefix('big_data_landing_zone', 'unzipped')
    loadFileToTable('big-data-on-gcp','sales_data','sales','big_data_landing_zone',files)
    deleteBlobWithPrefix('big_data_landing_zone', 'unzipped')

loadTable()