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
from pyflink.ml.api.ml_environment import MLEnvironment
from pyflink.dataset.execution_environment import ExecutionEnvironment
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.table.table_environment import BatchTableEnvironment, StreamTableEnvironment
from pyflink.java_gateway import get_gateway
import threading


class MLEnvironmentFactory:
    """
    Factory to get the MLEnvironment using a MLEnvironmentId.

    .. versionadded:: 1.11.0
    """
    _lock = threading.RLock()
    _default_ml_environment_id = 0
    _next_id = 1
    _map = {_default_ml_environment_id: None}

    @staticmethod
    def get(ml_env_id: int) -> Optional[MLEnvironment]:
        """
        Get the MLEnvironment using a MLEnvironmentId.

        :param ml_env_id: the MLEnvironmentId
        :return: the MLEnvironment

        .. versionadded:: 1.11.0
        """
        with MLEnvironmentFactory._lock:
            if ml_env_id == 0:
                return MLEnvironmentFactory.get_default()
            elif ml_env_id not in MLEnvironmentFactory._map:
                raise ValueError(
                    "Cannot find MLEnvironment for MLEnvironmentId %s. "
                    "Did you get the MLEnvironmentId by calling "
                    "get_new_ml_environment_id?" % ml_env_id)
            return MLEnvironmentFactory._map[ml_env_id]

    @staticmethod
    def get_default() -> Optional[MLEnvironment]:
        """
        Get the MLEnvironment use the default MLEnvironmentId.

        :return: the default MLEnvironment.

        .. versionadded:: 1.11.0
        """
        with MLEnvironmentFactory._lock:
            if MLEnvironmentFactory._map[MLEnvironmentFactory._default_ml_environment_id] is None:
                j_ml_env = get_gateway().\
                    jvm.org.apache.flink.ml.common.MLEnvironmentFactory.getDefault()
                ml_env = MLEnvironment(
                    ExecutionEnvironment(j_ml_env.getExecutionEnvironment()),
                    StreamExecutionEnvironment(j_ml_env.getStreamExecutionEnvironment()),
                    BatchTableEnvironment(j_ml_env.getBatchTableEnvironment()),
                    StreamTableEnvironment(j_ml_env.getStreamTableEnvironment()))
                MLEnvironmentFactory._map[MLEnvironmentFactory._default_ml_environment_id] = ml_env

            return MLEnvironmentFactory._map[MLEnvironmentFactory._default_ml_environment_id]

    @staticmethod
    def get_new_ml_environment_id() -> int:
        """
        Create a unique MLEnvironment id and register a new MLEnvironment in the factory.

        :return: the MLEnvironment id.

        .. versionadded:: 1.11.0
        """
        with MLEnvironmentFactory._lock:
            return MLEnvironmentFactory.register_ml_environment(MLEnvironment())

    @staticmethod
    def register_ml_environment(ml_environment: MLEnvironment) -> int:
        """
        Register a new MLEnvironment to the factory and return a new MLEnvironment id.

        :param ml_environment: the MLEnvironment that will be stored in the factory.
        :return: the MLEnvironment id.

        .. versionadded:: 1.11.0
        """
        with MLEnvironmentFactory._lock:
            MLEnvironmentFactory._map[MLEnvironmentFactory._next_id] = ml_environment
            MLEnvironmentFactory._next_id += 1
            return MLEnvironmentFactory._next_id - 1

    @staticmethod
    def remove(ml_env_id: int) -> MLEnvironment:
        """
        Remove the MLEnvironment using the MLEnvironmentId.

        :param ml_env_id: the id.
        :return: the removed MLEnvironment

        .. versionadded:: 1.11.0
        """
        with MLEnvironmentFactory._lock:
            if ml_env_id is None:
                raise ValueError("The environment id cannot be null.")
            # Never remove the default MLEnvironment. Just return the default environment.
            if MLEnvironmentFactory._default_ml_environment_id == ml_env_id:
                return MLEnvironmentFactory.get_default()
            else:
                return MLEnvironmentFactory._map.pop(ml_env_id)
