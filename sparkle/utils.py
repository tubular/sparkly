import logging
import os

logger = logging.getLogger(__name__)


def schema_has(dataframe, subset_of_fields):
    """Check if dataframe has required subset of fields.

    Args:
        dataframe (pyspark.sql.DataFrame)
        subset_of_fields (dict[str,pyspark.sql.types.DataType]): E.g. {'title': StringType}.

    Returns:
        bool
    """
    dataframe_types = {f.name: type(f.dataType) for f in dataframe.schema.fields}

    for field_name, expected_type_or_types in subset_of_fields.items():
        dataframe_field_type = dataframe_types.get(field_name)

        if dataframe_field_type is None:
            logger.error('field "%s" is missed', field_name)
            return False

        if not issubclass(dataframe_field_type, expected_type_or_types):
            logger.error('%s has type %s, but %s is expected', field_name,
                         dataframe_types.get(field_name), expected_type_or_types)
            return False

    return True


def context_has_package(hc, package_prefix):
    """Check if SparkleContext has a particular package.

    Args:
        hc (sparkle.SparkleContext)
        package_prefix (str): E.g. "org.elasticsearch:elasticsearch-spark"

    Returns:
        bool
    """
    return any(package for package in hc.packages if package.startswith(package_prefix))


def context_has_jar(hc, jar_name):
    """Check if SparkleContext has a particular package.

    Args:
        hc (sparkle.SparkleContext)
        jar_name (str): E.g. "mysql-connector-java"

    Returns:
        bool
    """
    return any(jar for jar in hc.jars if jar_name in jar)


def config_reader_writer(reader_or_writer, options):
    """Set options for Spark DataFrameReader or DataFrameWriter.

    Args:
        reader_or_writer (pyspark.sql.DataFrameReader | pyspark.sql.DataFrameWriter)
        options (dict[str,str])

    Returns:
        pyspark.sql.DataFrameReader | pyspark.sql.DataFrameWriter
    """
    if options:
        for key, value in options.items():
            reader_or_writer = reader_or_writer.option(key, value)

    return reader_or_writer


def absolute_path(file_path, *rel_path):
    """Returns absolute path to file.

    Usage:
        >>> absolute_path('/my/current/dir/x.txt', '..', 'x.txt')
        '/my/current/x.txt'

        >>> absolute_path('/my/current/dir/x.txt', 'relative', 'path')
        '/my/current/dir/relative/path'

        >>> import os
        >>> absolute_path('x.txt', 'relative/path') == os.getcwd() + '/relative/path'
        True

    Args:
        file_path (str): file
        rel_path (list[str]): path parts

    Returns:
        str
    """
    return os.path.abspath(
        os.path.join(
            os.path.dirname(
                os.path.realpath(file_path)
            ),
            *rel_path
        )
    )
