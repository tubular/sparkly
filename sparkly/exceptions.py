class SparklyException(Exception):
    """Base exception of sparkly lib."""


class UnsupportedDataType(SparklyException):
    """Happen when schema defines unsupported data type."""
    pass


class FixtureError(SparklyException):
    """Happen when testing data setup or teardown fails."""
    pass
