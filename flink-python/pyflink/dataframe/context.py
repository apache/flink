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

from typing import Optional

from pyflink.common import Configuration
from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableEnvironment
from pyflink.util.api_stability_decorators import PublicEvolving
from pyflink.util.java_utils import get_j_env_configuration

__all__ = [
    "set_table_environment",
    "get_table_environment",
    "get_or_create_table_environment",
]

_global_table_environment: Optional[TableEnvironment] = None


@PublicEvolving()
def set_table_environment(t_env: Optional[TableEnvironment]) -> None:
    """
    Set the environment used by DataFrame operations.

    The injected environment is treated as fully configured: values buffered in
    :data:`config` are not applied to it. Injecting an environment while :data:`config`
    holds buffered values is rejected, because those values would otherwise be silently
    ignored. Passing ``None`` clears the environment and leaves the buffered values intact.

    :param t_env: Environment to use, or ``None`` to clear it.
    :raises TypeError: If ``t_env`` is neither a :class:`TableEnvironment` nor ``None``.
    :raises RuntimeError: If ``t_env`` is not ``None`` and :data:`config` holds buffered
                          values.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.set_table_environment(None)
        >>> pf.get_table_environment() is None
        True

    .. versionadded:: 2.4.0
    """
    global _global_table_environment
    if t_env is not None and not isinstance(t_env, TableEnvironment):
        raise TypeError("t_env must be a TableEnvironment or None")
    if t_env is not None:
        from pyflink.dataframe.dataframe_config import config

        if config._buffered:
            raise RuntimeError(
                "pf.config holds buffered values that only apply to an environment created by "
                "get_or_create_table_environment(); they would be ignored by the injected "
                "environment. Configure that environment through t_env.get_config() instead of "
                "pf.config, or call get_or_create_table_environment() to have the buffered "
                "values applied."
            )
    _global_table_environment = t_env


@PublicEvolving()
def get_table_environment() -> Optional[TableEnvironment]:
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
def get_or_create_table_environment() -> TableEnvironment:
    """
    Return the configured environment, creating one when necessary.

    The environment is created from the values buffered in :data:`config`, so options that
    can only be chosen at creation time, such as ``execution.runtime-mode``, take effect.
    It is retained for subsequent DataFrame operations and calls to
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
        from pyflink.dataframe.dataframe_config import config
        from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment

        configuration = config._to_configuration()
        stream_environment = StreamExecutionEnvironment.get_execution_environment(configuration)

        # The execution environment may merge deployment configuration (for example, from the
        # CLI or config.yaml) while it is being created. EnvironmentSettings must use this
        # effective configuration because some Table API options are consumed during creation.
        j_environment_configuration = get_j_env_configuration(
            stream_environment._j_stream_execution_environment
        )
        environment_configuration = Configuration(
            j_configuration=j_environment_configuration
        )
        settings_builder = EnvironmentSettings.new_instance().with_configuration(
            environment_configuration
        )

        # Match StreamTableEnvironment.create(executionEnvironment): Table API supports only an
        # explicit batch or streaming mode, so AUTOMATIC is treated as streaming.
        j_execution_options = (
            get_gateway().jvm.org.apache.flink.configuration.ExecutionOptions
        )
        runtime_mode = RuntimeExecutionMode._from_j_execution_mode(
            j_environment_configuration.get(j_execution_options.RUNTIME_MODE)
        )
        if runtime_mode == RuntimeExecutionMode.BATCH:
            settings_builder.in_batch_mode()
        else:
            settings_builder.in_streaming_mode()
        settings = settings_builder.build()
        _global_table_environment = StreamTableEnvironment.create(
            stream_environment, environment_settings=settings
        )

    return _global_table_environment
