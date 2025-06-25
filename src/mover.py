# s3_to_azure_single.py
# pip install boto3 azure-storage-blob python-dotenv

import io
import logging
import os
from typing import Optional

import boto3
from azure.storage.blob import BlobServiceClient
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3ToAzureSingleMover:
    """
    Copy a single object from S3 to Azure Blob Storage.
    """

    def __init__(
        self,
        azure_connection_string: str,
        role_arn: str,
        aws_region: Optional[str] = None,
        session_name: str = "S3ToAzureSession",
    ) -> None:
        # Always assume role for AWS client
        creds = self.assume_role(role_arn, session_name)
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key"],
            aws_secret_access_key=creds["aws_secret_key"],
            aws_session_token=creds["aws_session_token"],
            region_name=aws_region,
        )

        # Azure client
        self.blob_service = BlobServiceClient.from_connection_string(
            azure_connection_string
        )

    def assume_role(
        self, role_arn: str, session_name: str = "S3ToAzureSession"
    ) -> dict:
        """
        Assume an AWS IAM role and return temporary credentials.
        """
        sts_client = boto3.client("sts")
        response = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName=session_name
        )
        credentials = response["Credentials"]
        return {
            "aws_access_key": credentials["AccessKeyId"],
            "aws_secret_key": credentials["SecretAccessKey"],
            "aws_session_token": credentials["SessionToken"],
        }

    def move_single_file(
        self,
        s3_bucket: str,
        s3_key: str,
        azure_container: str,
        azure_blob_name: Optional[str] = None,
        overwrite: bool = True,
    ) -> None:
        """
        Move a single file from S3 to Azure.

        Args:
            s3_bucket: Source S3 bucket name
            s3_key: Source S3 object key
            azure_container: Target Azure container
            azure_blob_name: Target blob name (defaults to s3_key basename)
            overwrite: Whether to overwrite existing blob
        """
        # Use basename of S3 key if no Azure blob name specified
        if azure_blob_name is None:
            azure_blob_name = os.path.basename(s3_key)

        logger.info(f"Downloading {s3_bucket}/{s3_key} from S3...")
        buffer = io.BytesIO()
        self.s3.download_fileobj(s3_bucket, s3_key, buffer)
        buffer.seek(0)

        logger.info(
            f"Uploading to Azure container '{azure_container}' as blob '{azure_blob_name}'..."
        )
        blob_client = self.blob_service.get_blob_client(
            azure_container, azure_blob_name
        )
        blob_client.upload_blob(buffer, overwrite=overwrite)

        logger.info(
            f"✓ Moved: {s3_bucket}/{s3_key} → {azure_container}/{azure_blob_name}"
        )


# Example usage
if __name__ == "__main__":
    # Load environment variables
    AZURE_CONN = os.getenv("AZURE_CONN")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    AWS_ROLE_ARN = os.getenv("AWS_ROLE_ARN")

    S3_BUCKET = os.getenv("S3_BUCKET", "my-source-bucket")
    S3_KEY = os.getenv("S3_KEY", "reports/2024/quarterly-report.pdf")
    AZURE_CONTAINER = os.getenv("AZURE_CONTAINER", "archive-container")
    AZURE_BLOB_NAME = os.getenv("AZURE_BLOB_NAME")

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") or None
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or None

    if not AZURE_CONN:
        raise EnvironmentError("AZURE_CONN environment variable is required!")
    if not AWS_ROLE_ARN:
        raise EnvironmentError("AWS_ROLE_ARN environment variable is required!")

    mover = S3ToAzureSingleMover(
        azure_connection_string=AZURE_CONN,
        role_arn=AWS_ROLE_ARN,
        aws_region=AWS_REGION,
    )

    mover.move_single_file(
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        azure_container=AZURE_CONTAINER,
        azure_blob_name=AZURE_BLOB_NAME,
    )
