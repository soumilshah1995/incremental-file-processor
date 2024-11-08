import os
import json
from datetime import datetime
from urllib.parse import urlparse
import boto3
from io import StringIO

class IncrementalFileProcessor:
    def __init__(self, path, checkpoint_path, minio_config=None):
        self.path = path
        self.checkpoint_path = checkpoint_path
        self.parsed_url = urlparse(self.path)
        self.checkpoint_parsed_url = urlparse(self.checkpoint_path)
        self.client = self._get_client(minio_config)
        self.last_checkpoint_time = self._load_checkpoint()

    def _get_client(self, minio_config):
        if self.parsed_url.scheme in ['s3', 's3a'] or self.checkpoint_parsed_url.scheme in ['s3', 's3a']:
            if minio_config:
                return boto3.client('s3',
                                    endpoint_url=minio_config['endpoint_url'],
                                    aws_access_key_id=minio_config['access_key'],
                                    aws_secret_access_key=minio_config['secret_key'])
            else:
                return boto3.client('s3')
        return None

    def _load_checkpoint(self):
        if self.checkpoint_parsed_url.scheme in ['s3', 's3a']:
            try:
                bucket, key = self._parse_s3_path(self.checkpoint_path)
                response = self.client.get_object(Bucket=bucket, Key=key)
                return json.load(response['Body']).get('last_processed_time', 0)
            except self.client.exceptions.NoSuchKey:
                return 0
        else:
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r') as f:
                    return json.load(f).get('last_processed_time', 0)
            return 0

    def _parse_s3_path(self, s3_path):
        parsed = urlparse(s3_path)
        return parsed.netloc, parsed.path.lstrip('/')

    def _list_s3_files(self):
        bucket, prefix = self._parse_s3_path(self.path)
        files = []
        paginator = self.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['LastModified'].timestamp() > self.last_checkpoint_time:
                    files.append(f"s3://{bucket}/{obj['Key']}")
        return files

    def _list_local_files(self):
        files = []
        directory = self.parsed_url.path
        for root, _, filenames in os.walk(directory):
            for filename in filenames:
                file_path = os.path.join(root, filename)
                if os.path.getmtime(file_path) > self.last_checkpoint_time:
                    files.append(file_path)
        return files

    def get_new_files(self):
        if self.parsed_url.scheme in ['s3', 's3a']:
            return self._list_s3_files()
        elif self.parsed_url.scheme == 'file' or not self.parsed_url.scheme:
            return self._list_local_files()
        else:
            raise ValueError(f"Unsupported scheme: {self.parsed_url.scheme}")

    def commit_checkpoint(self):
        current_time = datetime.now().timestamp()
        checkpoint_data = json.dumps({'last_processed_time': current_time})

        if self.checkpoint_parsed_url.scheme in ['s3', 's3a']:
            bucket, key = self._parse_s3_path(self.checkpoint_path)
            self.client.put_object(Bucket=bucket, Key=key, Body=checkpoint_data)
        else:
            os.makedirs(os.path.dirname(self.checkpoint_path), exist_ok=True)
            with open(self.checkpoint_path, 'w') as f:
                f.write(checkpoint_data)

        print(f"Checkpoint updated to: {datetime.fromtimestamp(current_time)}")

