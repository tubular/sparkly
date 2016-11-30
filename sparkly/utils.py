import os


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
