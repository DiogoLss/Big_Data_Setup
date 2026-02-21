from minio import Minio
from airflow.models import Variable
from datetime import datetime

class MinioManager:
    def __init__(self):
        self.endpoint = 'minio:9000'
        self.access_key = 'admin'
        self.secret_key = 'admin123'

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False
        )

    def upload_file(self, bucket, path_to_save,format, source_file):
        date = datetime.now().strftime('%y-%m-%d-%H-%M-%S-%f')
        path_to_save = f'{path_to_save}/{date}.{format}'
        self.client.fput_object(bucket, path_to_save, source_file)

    
if __name__ == "__main__":
    uploader = MinioManager()
    uploader.upload_file('nome_do_bucket', 'caminho/de/destino', 'caminho/do/arquivo')