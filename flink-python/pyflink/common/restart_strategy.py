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

from py4j.java_gateway import get_java_class

from pyflink.java_gateway import get_gateway
from pyflink.util.utils import to_j_flink_time, from_j_flink_time

__all__ = ['RestartStrategies', 'RestartStrategyConfiguration']


class RestartStrategyConfiguration(object):
    """
    Abstract configuration for restart strategies.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_restart_strategy_configuration):
        self._j_restart_strategy_configuration = j_restart_strategy_configuration

    def get_description(self):
        """
        Returns a description which is shown in the web interface.

        :return: Description of the restart strategy.
        """
        return self._j_restart_strategy_configuration.getDescription()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self._j_restart_strategy_configuration == \
            other._j_restart_strategy_configuration

    def __hash__(self):
        return self._j_restart_strategy_configuration.hashCode()


class RestartStrategies(object):
    """
    This class defines methods to generate RestartStrategyConfigurations. These configurations are
    used to create RestartStrategies at runtime.

    The RestartStrategyConfigurations are used to decouple the core module from the runtime module.
    """

    class NoRestartStrategyConfiguration(RestartStrategyConfiguration):
        """
        Configuration representing no restart strategy.
        """

        def __init__(self, j_restart_strategy=None):
            if j_restart_strategy is None:
                gateway = get_gateway()
                self._j_restart_strategy_configuration = \
                    gateway.jvm.RestartStrategies.NoRestartStrategyConfiguration()
                super(RestartStrategies.NoRestartStrategyConfiguration, self)\
                    .__init__(self._j_restart_strategy_configuration)
            else:
                super(RestartStrategies.NoRestartStrategyConfiguration, self) \
                    .__init__(j_restart_strategy)

    class FixedDelayRestartStrategyConfiguration(RestartStrategyConfiguration):
        """
        Configuration representing a fixed delay restart strategy.
        """

        def __init__(self, restart_attempts=None, delay_between_attempts_interval=None,
                     j_restart_strategy=None):
            if j_restart_strategy is None:
                if not isinstance(delay_between_attempts_interval, (timedelta, int)):
                    raise TypeError("The delay_between_attempts_interval 'failure_interval' "
                                    "only supports integer and datetime.timedelta, current input "
                                    "type is %s." % type(delay_between_attempts_interval))
                gateway = get_gateway()
                self._j_restart_strategy_configuration = \
                    gateway.jvm.RestartStrategies\
                    .fixedDelayRestart(
                        restart_attempts, to_j_flink_time(delay_between_attempts_interval))
                super(RestartStrategies.FixedDelayRestartStrategyConfiguration, self)\
                    .__init__(self._j_restart_strategy_configuration)
            else:
                super(RestartStrategies.FixedDelayRestartStrategyConfiguration, self) \
                    .__init__(j_restart_strategy)

        def get_restart_attempts(self):
            return self._j_restart_strategy_configuration.getRestartAttempts()

        def get_delay_between_attempts_interval(self):
            return from_j_flink_time(
                self._j_restart_strategy_configuration.getDelayBetweenAttemptsInterval())

    class FailureRateRestartStrategyConfiguration(RestartStrategyConfiguration):
        """
        Configuration representing a failure rate restart strategy.
        """

        def __init__(self,
                     max_failure_rate=None,
                     failure_interval=None,
                     delay_between_attempts_interval=None,
                     j_restart_strategy=None):
            if j_restart_strategy is None:
                if not isinstance(failure_interval, (timedelta, int)):
                    raise TypeError("The parameter 'failure_interval' "
                                    "only supports integer and datetime.timedelta, current input "
                                    "type is %s." % type(failure_interval))
                if not isinstance(delay_between_attempts_interval, (timedelta, int)):
                    raise TypeError("The delay_between_attempts_interval 'failure_interval' "
                                    "only supports integer and datetime.timedelta, current input "
                                    "type is %s." % type(delay_between_attempts_interval))
                gateway = get_gateway()
                self._j_restart_strategy_configuration = \
                    gateway.jvm.RestartStrategies\
                    .FailureRateRestartStrategyConfiguration(max_failure_rate,
                                                             to_j_flink_time(failure_interval),
                                                             to_j_flink_time(
                                                                 delay_between_attempts_interval))
                super(RestartStrategies.FailureRateRestartStrategyConfiguration, self)\
                    .__init__(self._j_restart_strategy_configuration)
            else:
                super(RestartStrategies.FailureRateRestartStrategyConfiguration, self)\
                    .__init__(j_restart_strategy)

        def get_max_failure_rate(self):
            return self._j_restart_strategy_configuration.getMaxFailureRate()

        def get_failure_interval(self):
            return from_j_flink_time(self._j_restart_strategy_configuration.getFailureInterval())

        def get_delay_between_attempts_interval(self):
            return from_j_flink_time(self._j_restart_strategy_configuration
                                     .getDelayBetweenAttemptsInterval())

    class FallbackRestartStrategyConfiguration(RestartStrategyConfiguration):
        """
        Restart strategy configuration that could be used by jobs to use cluster level restart
        strategy. Useful especially when one has a custom implementation of restart strategy set via
        flink-conf.yaml.
        """

        def __init__(self, j_restart_strategy=None):
            if j_restart_strategy is None:
                gateway = get_gateway()
                self._j_restart_strategy_configuration = \
                    gateway.jvm.RestartStrategies.FallbackRestartStrategyConfiguration()
                super(RestartStrategies.FallbackRestartStrategyConfiguration, self)\
                    .__init__(self._j_restart_strategy_configuration)
            else:
                super(RestartStrategies.FallbackRestartStrategyConfiguration, self)\
                    .__init__(j_restart_strategy)

    @staticmethod
    def _from_j_restart_strategy(j_restart_strategy):
        if j_restart_strategy is None:
            return None
        gateway = get_gateway()
        NoRestartStrategyConfiguration = gateway.jvm.RestartStrategies\
            .NoRestartStrategyConfiguration
        FixedDelayRestartStrategyConfiguration = gateway.jvm.RestartStrategies\
            .FixedDelayRestartStrategyConfiguration
        FailureRateRestartStrategyConfiguration = gateway.jvm.RestartStrategies\
            .FailureRateRestartStrategyConfiguration
        FallbackRestartStrategyConfiguration = gateway.jvm.RestartStrategies\
            .FallbackRestartStrategyConfiguration
        clz = j_restart_strategy.getClass()
        if clz.getName() == get_java_class(NoRestartStrategyConfiguration).getName():
            return RestartStrategies.NoRestartStrategyConfiguration(
                j_restart_strategy=j_restart_strategy)
        elif clz.getName() == get_java_class(FixedDelayRestartStrategyConfiguration).getName():
            return RestartStrategies.FixedDelayRestartStrategyConfiguration(
                j_restart_strategy=j_restart_strategy)
        elif clz.getName() == get_java_class(FailureRateRestartStrategyConfiguration).getName():
            return RestartStrategies.FailureRateRestartStrategyConfiguration(
                j_restart_strategy=j_restart_strategy)
        elif clz.getName() == get_java_class(FallbackRestartStrategyConfiguration).getName():
            return RestartStrategies.FallbackRestartStrategyConfiguration(
                j_restart_strategy=j_restart_strategy)
        else:
            raise Exception("Unsupported java RestartStrategyConfiguration: %s" % clz.getName())

    @staticmethod
    def no_restart():
        """
        Generates NoRestartStrategyConfiguration.

        :return: The :class:`NoRestartStrategyConfiguration`.
        """
        return RestartStrategies.NoRestartStrategyConfiguration()

    @staticmethod
    def fall_back_restart():
        return RestartStrategies.FallbackRestartStrategyConfiguration()

    @staticmethod
    def fixed_delay_restart(restart_attempts, delay_between_attempts):
        """
        Generates a FixedDelayRestartStrategyConfiguration.

        :param restart_attempts: Number of restart attempts for the FixedDelayRestartStrategy.
        :param delay_between_attempts: Delay in-between restart attempts for the
                                       FixedDelayRestartStrategy, the input could be integer value
                                       in milliseconds or datetime.timedelta object.
        :return: The :class:`FixedDelayRestartStrategyConfiguration`.
        """
        return RestartStrategies.FixedDelayRestartStrategyConfiguration(restart_attempts,
                                                                        delay_between_attempts)

    @staticmethod
    def failure_rate_restart(failure_rate, failure_interval, delay_interval):
        """
        Generates a FailureRateRestartStrategyConfiguration.

        :param failure_rate: Maximum number of restarts in given interval ``failure_interval``
                             before failing a job.
        :param failure_interval: Time interval for failures, the input could be integer value
                                 in milliseconds or datetime.timedelta object.
        :param delay_interval: Delay in-between restart attempts, the input could be integer value
                               in milliseconds or datetime.timedelta object.
        """
        return RestartStrategies.FailureRateRestartStrategyConfiguration(failure_rate,
                                                                         failure_interval,
                                                                         delay_interval)
