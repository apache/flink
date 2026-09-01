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

from typing import Dict, Optional

from pyflink.table import TableEnvironment
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = [
    "DataFrameConfig",
    "config",
]


@PublicEvolving()
class DataFrameConfig:
    """
    A unified entry point for Flink configuration in the DataFrame API.

    Accepts any Flink configuration key and buffers the value, so configuration can be set
    at any time -- even before an environment exists. Buffered values are applied
    automatically once the underlying :class:`~pyflink.table.TableEnvironment` is created
    by :func:`get_or_create_table_environment` or injected via
    :func:`set_table_environment`. While an environment is active, values are also written
    through to its configuration immediately.

    Use the module-level singleton :data:`config` instead of instantiating this class.

    Example::

        >>> import pyflink.dataframe as pf
        >>> _ = pf.config.set("parallelism.default", "4")
        >>> pf.config.get("parallelism.default")
        '4'

    .. versionadded:: 2.4.0
    """

    def __init__(self):
        self._buffered: Dict[str, str] = {}

    def set(self, key: str, value: str) -> "DataFrameConfig":
        """
        Sets a string-based value for the given string-based key.

        The value is buffered and applied to the underlying environment once it is created
        or injected; when an environment is already active, the value is applied to its
        configuration immediately as well.

        :param key: The configuration key.
        :param value: The configuration value. It will be parsed by the framework on access.
        :return: This object, to allow chaining of calls.
        :raises TypeError: If ``key`` or ``value`` is not a string.

        Example::

            >>> import pyflink.dataframe as pf
            >>> _ = pf.config.set("parallelism.default", "4") \\
            ...              .set("execution.runtime-mode", "batch")

        .. versionadded:: 2.4.0
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if not isinstance(value, str):
            raise TypeError("value must be a string")

        self._buffered[key] = value

        from pyflink.dataframe.context import get_table_environment

        t_env = get_table_environment()
        if t_env is not None:
            t_env.get_config().set(key, value)
        return self

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Returns the value associated with the given key as a string.

        When an environment is active, the value is read from its configuration, so values
        set outside this object are visible as well; otherwise the value is read from the
        buffered values.

        :param key: The configuration key.
        :param default: The value returned when there is no value associated with ``key``.
        :return: The (default) value associated with ``key``.
        :raises TypeError: If ``key`` is not a string, or ``default`` is neither a string
                           nor ``None``.

        Example::

            >>> import pyflink.dataframe as pf
            >>> _ = pf.config.set("parallelism.default", "4")
            >>> pf.config.get("parallelism.default")
            '4'
            >>> pf.config.get("pipeline.name", "unnamed")
            'unnamed'

        .. versionadded:: 2.4.0
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if default is not None and not isinstance(default, str):
            raise TypeError("default must be a string or None")

        from pyflink.dataframe.context import get_table_environment

        t_env = get_table_environment()
        if t_env is not None:
            return t_env.get_config().get(key, default)
        return self._buffered.get(key, default)

    def _apply_to(self, t_env: TableEnvironment) -> None:
        table_config = t_env.get_config()
        for key, value in self._buffered.items():
            table_config.set(key, value)


config = DataFrameConfig()
"""The singleton :class:`DataFrameConfig` used by the DataFrame API."""
