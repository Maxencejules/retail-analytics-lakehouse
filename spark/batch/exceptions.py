"""Custom exceptions for the batch ETL pipeline."""


class DataQualityError(RuntimeError):
    """Raised when critical data quality rules are violated."""

