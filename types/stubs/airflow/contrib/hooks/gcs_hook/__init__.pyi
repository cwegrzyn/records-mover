import google.cloud.storage


# https://airflow.apache.org/docs/stable/_modules/airflow/contrib/hooks/gcs_hook.html
class GoogleCloudStorageHook:
    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None):
        ...

    def get_conn(self) -> google.cloud.storage.Client:
        ...
