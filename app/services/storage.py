# app/services/storage.py
#!/usr/bin/env python3
import mimetypes
import logging
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from app.config import Settings

logger = logging.getLogger(__name__)

class WasabiClient:
    def __init__(self, settings: Settings):
        if not all([
            settings.wasabi_access_key_id,
            settings.wasabi_secret_access_key,
            settings.wasabi_bucket
        ]):
            raise RuntimeError("Wasabi config incomplete")

        self.bucket  = settings.wasabi_bucket
        self.prefix  = (settings.wasabi_prefix or "").strip().strip("/")
        self.expires = settings.wasabi_presign_expire
        self.settings = settings  # Settings-i saxla

        self._s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.wasabi_access_key_id,
            aws_secret_access_key=settings.wasabi_secret_access_key,
            region_name=settings.wasabi_region,
            endpoint_url=settings.wasabi_endpoint or "https://s3.wasabisys.com",
            config=Config(s3={"addressing_style": "virtual"})
        )

    def _full_key(self, key: str) -> str:
        key = key.lstrip("/")
        return f"{self.prefix}/{key}" if self.prefix else key

    def upload_file(self, local_path: str, key: str, content_type: str | None = None) -> None:
        full_key = self._full_key(key)
        if not content_type:
            content_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"
        self._s3.upload_file(
            local_path,
            self.bucket,
            full_key,
            ExtraArgs={"ContentType": content_type}
        )
        logger.info("Wasabi upload OK s3://%s/%s", self.bucket, full_key)

    def presign_get(self, key: str, expires: int | None = None) -> str:
        full_key = self._full_key(key)
        return self._s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucket, "Key": full_key},
            ExpiresIn=expires or self.expires,
        )

    def exists(self, key: str) -> bool:
        full_key = self._full_key(key)
        try:
            self._s3.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            # başqa errorlar bubble-up etsin
            raise

    def delete_object(self, key: str) -> None:
        full_key = self._full_key(key)
        self._s3.delete_object(Bucket=self.bucket, Key=full_key)
        logger.info("Wasabi deleted s3://%s/%s", self.bucket, full_key)

    def list_files(self, prefix: str = "", max_keys: int = 1000) -> list:
        """Bucket-də faylları listələyir."""
        try:
            full_prefix = self._full_key(prefix) if prefix else self.prefix
            
            response = self._s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=full_prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' not in response:
                logger.info("No objects found with prefix: %s", full_prefix)
                return []
            
            files = [obj['Key'] for obj in response['Contents']]
            logger.info("Found %d files with prefix '%s'", len(files), full_prefix)
            return files
            
        except Exception as e:
            logger.error("Error listing files with prefix '%s': %s", prefix, e)
            return []

    def download_file(self, key: str, file_path: str):
        """Wasabi-dən fayl endirir."""
        try:
            full_key = self._full_key(key)
            self._s3.download_file(self.bucket, full_key, file_path)
            logger.info("Downloaded %s/%s to %s", self.bucket, full_key, file_path)
        except Exception as e:
            logger.error("Error downloading %s: %s", key, e)
            raise