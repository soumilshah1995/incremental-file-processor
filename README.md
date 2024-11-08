# incremental-file-processor
incremental-file-processor

# How to use 
```
def test_minio():
    # MinIO path and configuration
    processor = IncrementalFileProcessor(
        "s3a://<bucket_name>/raw",  # Input path for MinIO (can be S3 or MinIO)
        "s3a://<bucket_name>/checkpoints/raw_checkpoint.json",  # Checkpoint path for MinIO
        minio_config={
            'endpoint_url': 'http://localhost:9000',
            'access_key': 'admin',
            'secret_key': 'password'
        }
    )

    # Fetch new files and commit checkpoint if any
    new_files = processor.get_new_files()
    if new_files:
        print(f"Found {len(new_files)} new or modified files:")
        for file in new_files:
            print(f"- {file}")
            # Add processing logic here (e.g., Spark or other processing engines)
        processor.commit_checkpoint()
    else:
        print("No new or modified files found.")


def test_local_file_system():
    # Local file system path and checkpoint
    processor = IncrementalFileProcessor(
        "file:///Users/sshah/IdeaProjects/poc-projects/aws/iceberg-inc/raw",  # Input local path
        "/Users/sshah/IdeaProjects/poc-projects/aws/iceberg-inc/checkpoints/raw_checkpoint.json"  # Checkpoint path
    )

    # Fetch new files and commit checkpoint if any
    new_files = processor.get_new_files()
    if new_files:
        print(f"Found {len(new_files)} new or modified files:")
        for file in new_files:
            print(f"- {file}")
            # Add processing logic here
        processor.commit_checkpoint()
    else:
        print("No new or modified files found.")


def test_s3():
    # S3 path and checkpoint
    processor = IncrementalFileProcessor(
        "s3://<bucket_name>/raw/",  # Input path for S3
        "s3://<bucket_name>/checkpoints/raw_checkpoint.json"  # Checkpoint path for S3
    )

    # Fetch new files and commit checkpoint if any
    new_files = processor.get_new_files()
    if new_files:
        print(f"Found {len(new_files)} new or modified files:")
        for file in new_files:
            print(f"- {file}")
            # Add processing logic here
        processor.commit_checkpoint()
    else:
        print("No new or modified files found.")


if __name__ == "__main__":
    # Uncomment the test functions as needed
    # test_s3()
    # test_local_file_system()
    # test_minio()

```
