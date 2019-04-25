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

__all__ = ['TableConfig']


class TableConfig(object):
    """
    A config to define the runtime behavior of the Table API.
    """

    def __init__(self):
        self._is_stream = None
        self._parallelism = None

    @property
    def is_stream(self):
        return self._is_stream

    @is_stream.setter
    def is_stream(self, is_stream):
        self._is_stream = is_stream

    @property
    def parallelism(self):
        return self._parallelism

    @parallelism.setter
    def parallelism(self, parallelism):
        self._parallelism = parallelism

    class Builder(object):

        def __init__(self):
            self._is_stream = None
            self._parallelism = None

        def as_streaming_execution(self):
            """
            Configures streaming execution mode.
            If this method is called, :class:`StreamTableEnvironment` will be created.

            :return: Builder
            """
            self._is_stream = True
            return self

        def as_batch_execution(self):
            """
            Configures batch execution mode.
            If this method is called, :class:`BatchTableEnvironment` will be created.

            :return: Builder
            """
            self._is_stream = False
            return self

        def set_parallelism(self, parallelism):
            """
            Sets the parallelism for all operations.

            :param parallelism: The parallelism.
            :return: Builder
            """
            self._parallelism = parallelism
            return self

        def build(self):
            """
            Builds :class:`TableConfig` object.

            :return: TableConfig
            """
            config = TableConfig()
            config.parallelism = self._parallelism
            config.is_stream = self._is_stream
            return config
