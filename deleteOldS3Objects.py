import boto3
from datetime import datetime, timedelta, timezone


def delete_old_s3_files(bucket_name, days):
    """Deletes S3 objects older than X days from specified bucket. Does not delete prefixes"""
    past = datetime.now(timezone.utc) - timedelta(days=days)
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        PaginationConfig={
            'PageSize': 100
        }
    )
    counter = 1
    delete_candidates = []
    for page in response_iterator:
        print(f"--- page {counter} ---")
        for item in page["Contents"]:
            if(item['LastModified'] < past and not item['Key'].endswith('/')):
                delete_candidates.append({"Key": item['Key']})
        counter += 1
    if(delete_candidates):
        print("delete candidates:", delete_candidates)
        delete_response = client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': delete_candidates
            }
        )
        print(delete_response)
        return
    print("no delete candidates found")


if __name__ == "__main__":
    delete_old_s3_files("sftp-oregon.idomoo.com", 7)
