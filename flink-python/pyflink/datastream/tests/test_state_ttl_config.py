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
import pickle
import unittest
from unittest.mock import patch

from pyflink.common.time import Duration, Time
from pyflink.datastream.state import StateTtlConfig


class StateTtlConfigTests(unittest.TestCase):

    def test_default_cleanup_without_gateway(self):
        with patch('pyflink.common.time.get_gateway',
                   side_effect=AssertionError('Gateway requested')):
            config = StateTtlConfig.new_builder(Time.days(1)).build()
            strategy = config.get_cleanup_strategies().get_rocksdb_compact_filter_cleanup_strategy()
            self.assertEqual(strategy.get_query_time_after_num_entries(), 1000)

    def test_explicit_cleanup_without_gateway(self):
        for arguments in [(), (None,), (0,), (False,), ('',)]:
            with self.subTest(arguments=arguments):
                with patch('pyflink.common.time.get_gateway',
                           side_effect=AssertionError('Gateway requested')):
                    config = (StateTtlConfig.new_builder(Time.days(1))
                              .cleanup_in_rocksdb_compact_filter(17, *arguments)
                              .build())
                    strategy = (config.get_cleanup_strategies()
                                .get_rocksdb_compact_filter_cleanup_strategy())
                    self.assertEqual(strategy.get_query_time_after_num_entries(), 17)

    def test_cleanup_round_trip_without_gateway(self):
        for explicit_cleanup in [False, True]:
            for serializer in ['pickle', 'protobuf']:
                with self.subTest(explicit_cleanup=explicit_cleanup, serializer=serializer):
                    with patch('pyflink.common.time.get_gateway',
                               side_effect=AssertionError('Gateway requested')):
                        builder = StateTtlConfig.new_builder(Time.days(1))
                        if explicit_cleanup:
                            builder.cleanup_in_rocksdb_compact_filter(17)
                        config = builder.build()
                        if serializer == 'pickle':
                            restored = pickle.loads(pickle.dumps(config))
                        else:
                            restored = StateTtlConfig._from_proto(config._to_proto())
                        strategy = (restored.get_cleanup_strategies()
                                    .get_rocksdb_compact_filter_cleanup_strategy())
                        self.assertEqual(restored.get_ttl(), Time.days(1))
                        self.assertEqual(strategy.get_query_time_after_num_entries(),
                                         17 if explicit_cleanup else 1000)

    def test_default_duration_is_created_by_accessor(self):
        with patch('pyflink.common.time.get_gateway') as gateway:
            strategy = StateTtlConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy(17)
            gateway.assert_not_called()

            duration = strategy.get_periodic_compaction_time()

            gateway.assert_called_once_with()
            java_duration = gateway.return_value.jvm.java.time.Duration
            java_duration.ofDays.assert_called_once_with(30)
            self.assertIs(duration._j_duration, java_duration.ofDays.return_value)

    def test_explicit_duration_is_preserved(self):
        for milliseconds in [0, 3600000]:
            with self.subTest(milliseconds=milliseconds):
                with patch('pyflink.common.time.get_gateway'):
                    duration = Duration.of_millis(milliseconds)
                with patch('pyflink.common.time.get_gateway',
                           side_effect=AssertionError('Gateway requested')):
                    config = (StateTtlConfig.new_builder(Time.days(1))
                              .cleanup_in_rocksdb_compact_filter(17, duration)
                              .build())
                    strategy = (config.get_cleanup_strategies()
                                .get_rocksdb_compact_filter_cleanup_strategy())
                    self.assertIs(strategy.get_periodic_compaction_time(), duration)
