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
from abc import ABCMeta
from datetime import timedelta
from py4j.compat import long

from pyflink.java_gateway import get_gateway


class QueryConfig(object):
    """
    The :class:`QueryConfig` holds parameters to configure the behavior of queries.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_query_config):
        self._j_query_config = j_query_config


class StreamQueryConfig(QueryConfig):
    """
    The :class:`StreamQueryConfig` holds parameters to configure the behavior of streaming queries.
    """

    def __init__(self, j_stream_query_config=None):
        if j_stream_query_config is not None:
            self._j_stream_query_config = j_stream_query_config
        else:
            self._j_stream_query_config = get_gateway().jvm.StreamQueryConfig()
        super(StreamQueryConfig, self).__init__(self._j_stream_query_config)

    def with_idle_state_retention_time(self, min_time, max_time):
        """
        Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
        was not updated, will be retained.

        State will never be cleared until it was idle for less than the minimum time and will never
        be kept if it was idle for more than the maximum time.

        When new data arrives for previously cleaned-up state, the new data will be handled as if it
        was the first data. This can result in previous results being overwritten.

        Set to 0 (zero) to never clean-up the state.

        .. note::

            Cleaning up state requires additional bookkeeping which becomes less expensive for
            larger differences of minTime and maxTime. The difference between minTime and maxTime
            must be at least 5 minutes.

        :param min_time: The minimum time interval for which idle state is retained. Set to
                         0 (zero) to never clean-up the state.
        :param max_time: The maximum time interval for which idle state is retained. Must be at
                         least 5 minutes greater than minTime. Set to
                         0 (zero) to never clean-up the state.
        :return: :class:`StreamQueryConfig`
        """
        #  type: (timedelta, timedelta) -> StreamQueryConfig
        j_time_class = get_gateway().jvm.org.apache.flink.api.common.time.Time
        j_min_time = j_time_class.milliseconds(long(round(min_time.total_seconds() * 1000)))
        j_max_time = j_time_class.milliseconds(long(round(max_time.total_seconds() * 1000)))
        self._j_stream_query_config = \
            self._j_stream_query_config.withIdleStateRetentionTime(j_min_time, j_max_time)
        return self

    def get_min_idle_state_retention_time(self):
        """
        State might be cleared and removed if it was not updated for the defined period of time.

        :return: The minimum time until state which was not updated will be retained.
        """
        #  type: () -> int
        return self._j_stream_query_config.getMinIdleStateRetentionTime()

    def get_max_idle_state_retention_time(self):
        """
        State will be cleared and removed if it was not updated for the defined period of time.

        :return: The maximum time until state which was not updated will be retained.
        """
        #  type: () -> int
        return self._j_stream_query_config.getMaxIdleStateRetentionTime()


class BatchQueryConfig(QueryConfig):
    """
    The :class:`BatchQueryConfig` holds parameters to configure the behavior of batch queries.
    """

    def __init__(self, j_batch_query_config=None):
        self._jvm = get_gateway().jvm
        if j_batch_query_config is not None:
            self._j_batch_query_config = j_batch_query_config
        else:
            self._j_batch_query_config = self._jvm.BatchQueryConfig()
        super(BatchQueryConfig, self).__init__(self._j_batch_query_config)
