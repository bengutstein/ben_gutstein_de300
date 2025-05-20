import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

def check_aws_credentials():
    try:
        s3 = boto3.client('s3')
        s3.list_buckets()  # Attempt to list S3 buckets
        print("✅ AWS credentials are valid.")
        return True
    except NoCredentialsError:
        print("❌ No AWS credentials found.")
    except PartialCredentialsError:
        print("❌ Incomplete AWS credentials.")
    except ClientError as e:
        print(f"❌ AWS client error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    return False

check_aws_credentials()
