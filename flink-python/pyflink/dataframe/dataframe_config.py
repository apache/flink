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

from pyflink.common import Configuration
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = [
    "config",
]


class _DataFrameConfig:
    """
    A unified entry point for Flink configuration in the DataFrame API.

    Accepts any Flink configuration key and buffers the value until
    :func:`get_or_create_table_environment` creates the underlying
    :class:`~pyflink.table.TableEnvironment`. Because the values are supplied at creation
    time, options that can only be chosen then, such as ``execution.runtime-mode`` or
    ``table.builtin-catalog-name``, take effect.

    Configuration must therefore be set before the environment exists. Once an environment
    is active, whether created or injected via :func:`set_table_environment`, use its own
    :meth:`~pyflink.table.TableEnvironment.get_config` instead. An environment passed to
    :func:`set_table_environment` is treated as fully configured and does not receive
    buffered values. Buffered values survive clearing the environment with
    ``set_table_environment(None)`` and feed the next environment created.

    Use the module-level singleton :data:`config` instead of instantiating this class.

    Example::

        >>> import pyflink.dataframe as pf
        >>> _ = pf.config.set("parallelism.default", "4")
        >>> pf.config.get("parallelism.default")
        '4'

    .. versionadded:: 2.4.0
    """

    def __init__(self: "_DataFrameConfig"):
        self._buffered: Dict[str, str] = {}

    @PublicEvolving()
    def set(self, key: str, value: str) -> "_DataFrameConfig":
        """
        Sets a string-based value for the given string-based key.

        The value is buffered and supplied to the environment created by
        :func:`get_or_create_table_environment`. It cannot be called while an environment
        is active, because options consumed at creation time could no longer take effect;
        configure the active environment through its
        :meth:`~pyflink.table.TableEnvironment.get_config` instead.

        :param key: The configuration key.
        :param value: The configuration value. It will be parsed by the framework on access.
        :return: This object, to allow chaining of calls.
        :raises TypeError: If ``key`` or ``value`` is not a string.
        :raises RuntimeError: If an environment is already active.

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

        from pyflink.dataframe.context import get_table_environment

        if get_table_environment() is not None:
            raise RuntimeError(
                "DataFrame configuration must be set before the table environment exists. "
                "Configure the active environment through t_env.get_config(), or clear it "
                "with set_table_environment(None) before calling config.set()."
            )
        self._buffered[key] = value
        return self

    @PublicEvolving()
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

    def _to_configuration(self) -> Configuration:
        configuration = Configuration()
        for key, value in self._buffered.items():
            configuration.set_string(key, value)
        return configuration


config = _DataFrameConfig()
"""The singleton :class:`_DataFrameConfig` used by the DataFrame API."""
