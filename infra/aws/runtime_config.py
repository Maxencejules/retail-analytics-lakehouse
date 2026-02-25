"""Environment-driven AWS runtime configuration for jobs and scripts."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class AwsLakehouseConfig:
    """Non-secret AWS/S3 settings loaded from environment variables."""

    environment: str
    region: str
    bucket: str
    prefix: str
    spark_workload_profile: str
    compaction_target_file_size_mb: int
    profile: str | None = None

    @classmethod
    def from_env(cls) -> "AwsLakehouseConfig":
        bucket = os.getenv("LAKEHOUSE_BUCKET", "").strip()
        if not bucket:
            raise ValueError("LAKEHOUSE_BUCKET is required")

        raw_compaction_size = os.getenv("COMPACTION_TARGET_FILE_SIZE_MB", "256").strip()
        try:
            compaction_target_file_size_mb = int(raw_compaction_size)
        except ValueError as exc:
            raise ValueError(
                "COMPACTION_TARGET_FILE_SIZE_MB must be an integer"
            ) from exc
        if compaction_target_file_size_mb <= 0:
            raise ValueError("COMPACTION_TARGET_FILE_SIZE_MB must be > 0")

        return cls(
            environment=os.getenv("APP_ENV", "dev").strip(),
            region=os.getenv(
                "AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "ca-central-1")
            ).strip(),
            bucket=bucket,
            prefix=os.getenv("LAKEHOUSE_PREFIX", "").strip().strip("/"),
            spark_workload_profile=os.getenv(
                "SPARK_WORKLOAD_PROFILE", "balanced"
            ).strip(),
            compaction_target_file_size_mb=compaction_target_file_size_mb,
            profile=os.getenv("AWS_PROFILE", "").strip() or None,
        )

    @property
    def bucket_uri(self) -> str:
        if self.prefix:
            return f"s3://{self.bucket}/{self.prefix}"
        return f"s3://{self.bucket}"

    def path(self, layer: str) -> str:
        layer_clean = layer.strip("/").lower()
        if self.prefix:
            return f"s3://{self.bucket}/{self.prefix}/{layer_clean}"
        return f"s3://{self.bucket}/{layer_clean}"


def build_boto3_session(config: AwsLakehouseConfig):
    """
    Build a boto3 session using the AWS default credential chain.

    Credentials are never hardcoded here. boto3 resolves credentials from:
    environment variables, shared config/credentials files, container/instance IAM roles.
    """
    import boto3

    return boto3.Session(
        profile_name=config.profile,
        region_name=config.region,
    )
