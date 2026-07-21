################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import Optional, TYPE_CHECKING

from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = [
    "set_table_environment",
    "get_table_environment",
    "get_or_create_table_environment",
]

if TYPE_CHECKING:
    from pyflink.table import StreamTableEnvironment


_global_table_environment: Optional["StreamTableEnvironment"] = None


@PublicEvolving()
def set_table_environment(t_env: Optional["StreamTableEnvironment"]) -> None:
    """
    Set the environment used by DataFrame operations.

    :param t_env: Environment to use, or ``None`` to clear it.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.set_table_environment(None)
        >>> pf.get_table_environment() is None
        True

    .. versionadded:: 2.4.0
    """
    global _global_table_environment
    _global_table_environment = t_env


@PublicEvolving()
def get_table_environment() -> Optional["StreamTableEnvironment"]:
    """
    Return the environment used by DataFrame operations, if one is configured.

    :return: The configured environment, or ``None``.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.set_table_environment(None)
        >>> pf.get_table_environment() is None
        True

    .. versionadded:: 2.4.0
    """
    return _global_table_environment


@PublicEvolving()
def get_or_create_table_environment() -> "StreamTableEnvironment":
    """
    Return the configured environment, creating one when necessary.

    The created environment is retained for subsequent DataFrame operations and calls to
    :func:`get_table_environment`.

    :return: The configured or newly created environment.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.set_table_environment(None)
        >>> environment = pf.get_or_create_table_environment()
        >>> pf.get_table_environment() is environment
        True

    .. versionadded:: 2.4.0
    """
    global _global_table_environment

    if _global_table_environment is None:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import StreamTableEnvironment

        stream_environment = StreamExecutionEnvironment.get_execution_environment()
        _global_table_environment = StreamTableEnvironment.create(stream_environment)

    return _global_table_environment
