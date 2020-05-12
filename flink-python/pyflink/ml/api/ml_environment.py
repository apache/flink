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

from pyflink.dataset.execution_environment import ExecutionEnvironment
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.table.table_environment import BatchTableEnvironment, StreamTableEnvironment


class MLEnvironment(object):
    """
    The MLEnvironment stores the necessary context in Flink. Each MLEnvironment
    will be associated with a unique ID. The operations associated with the same
    MLEnvironment ID will share the same Flink job context. Both MLEnvironment
    ID and MLEnvironment can only be retrieved from MLEnvironmentFactory.
    """

    def __init__(self, exe_env=None, stream_exe_env=None, batch_tab_env=None, stream_tab_env=None):
        self._exe_env = exe_env
        self._stream_exe_env = stream_exe_env
        self._batch_tab_env = batch_tab_env
        self._stream_tab_env = stream_tab_env

    def get_execution_environment(self) -> ExecutionEnvironment:
        """
        Get the ExecutionEnvironment. If the ExecutionEnvironment has not been set,
        it initial the ExecutionEnvironment with default Configuration.

        :return: the batch ExecutionEnvironment.
        """
        if self._exe_env is None:
            self._exe_env = ExecutionEnvironment.get_execution_environment()
        return self._exe_env

    def get_stream_execution_environment(self) -> StreamExecutionEnvironment:
        """
        Get the StreamExecutionEnvironment. If the StreamExecutionEnvironment has not been
        set, it initial the StreamExecutionEnvironment with default Configuration.

        :return: the StreamExecutionEnvironment.
        """
        if self._stream_exe_env is None:
            self._stream_exe_env = StreamExecutionEnvironment.get_execution_environment()
        return self._stream_exe_env

    def get_batch_table_environment(self) -> BatchTableEnvironment:
        """
        Get the BatchTableEnvironment. If the BatchTableEnvironment has not been set,
        it initial the BatchTableEnvironment with default Configuration.

        :return: the BatchTableEnvironment.
        """
        if self._batch_tab_env is None:
            self._batch_tab_env = BatchTableEnvironment.create(
                ExecutionEnvironment.get_execution_environment())
        return self._batch_tab_env

    def get_stream_table_environment(self) -> StreamTableEnvironment:
        """
        Get the StreamTableEnvironment. If the StreamTableEnvironment has not been set,
        it initial the StreamTableEnvironment with default Configuration.

        :return: the StreamTableEnvironment.
        """
        if self._stream_tab_env is None:
            self._stream_tab_env = StreamTableEnvironment.create(
                StreamExecutionEnvironment.get_execution_environment())
        return self._stream_tab_env
