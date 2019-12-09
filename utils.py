from google.cloud import bigquery
from google.cloud import storage
import datetime
import time

class GoogleBigQueryException(Exception):
    pass

class GoogleCloudService:
    def __init__(self,today):
        self.today = today

    def export_BQ_TO_GCS(self,table_id, file_name_prefix,bucket_name
                         project = "bigquery-public-data",dataset_id = "google_political_ads",
                        ):

        date_today = self.today.strftime("%m_%d")
        destination_uri = "gs://{}/{}".format(bucket_name, f"{file_name_prefix}_{date_today}.csv")
        # print(destination_uri)
        dataset_ref = client.dataset(dataset_id, project=project)
        table_ref   = dataset_ref.table(table_id)
        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location="US",
        )  # API request
        print(extract_job.result())  # Waits for job to complete.

        print(
            "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
        )

    # Note: Client.list_blobs requires at least package version 1.17.0.
    #       However, doesn't work on my environment.  need verifying later
    #blobs = storage_client.list_blobs(bucket_name)
    def list_blobs(self,bucket_name,folder_name):
        """Lists all the blobs in the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        #blobs = list(bucket.list_blobs(prefix='dark_money/'))
        return [blob.name for blob in bucket.list_blobs(prefix=f'{folder_name}/')]

    def block_until_done(self, imput_file, GCS_bucket, GCS_folder):
        print("blocking until report done: \n\t%s" % imput_file)
        n = 0
        while True:
            tem_file_lists = self.list_blobs(GCS_bucket,GCS_folder)

            if imput_file in tem_file_lists:
                print("report status is finished ... could download %s now" % imput_file)
                return True
            elif n >= 30:
                print("waiting for too long, quit")
                return False
            else:
                print("report status is not finished ... waiting")
                time.sleep(5)
                n += 1

    def download_blob(self,bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        blob.download_to_filename(destination_file_name)

        print('Blob {} downloaded to {}.'.format(
            source_blob_name,
            destination_file_name))