class SparkleException(Exception):
    """Base exception of sparkle lib."""


class UnsupportedDataType(SparkleException):
    """Happen when schema defines unsupported data type."""
    pass


class FixtureError(SparkleException):
    """Happen when testing data setup or teardown fails."""
    pass
