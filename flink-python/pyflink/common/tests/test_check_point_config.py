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
from pyflink.common import CheckpointConfig, CheckpointingMode, ExternalizedCheckpointCleanup
from pyflink.java_gateway import get_gateway
from pyflink.streaming import StreamExecutionEnvironment
from pyflink.testing.test_case_utils import PyFlinkTestCase


class CheckpointConfigTests(PyFlinkTestCase):

    def setUp(self):
        self.env = StreamExecutionEnvironment\
            .get_execution_environment()

        self.checkpoint_config = self.env.get_checkpoint_config()

    def test_constant(self):
        gateway = get_gateway()
        JCheckpointConfig = gateway.jvm.org.apache.flink.streaming.api.environment.CheckpointConfig

        assert CheckpointConfig.DEFAULT_MAX_CONCURRENT_CHECKPOINTS == \
            JCheckpointConfig.DEFAULT_MAX_CONCURRENT_CHECKPOINTS

        assert CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS == \
            JCheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS

        assert CheckpointConfig.DEFAULT_TIMEOUT == JCheckpointConfig.DEFAULT_TIMEOUT

        assert CheckpointConfig.DEFAULT_MODE == \
            CheckpointingMode._from_j_checkpointing_mode(JCheckpointConfig.DEFAULT_MODE)

    def test_is_checkpointing_enabled(self):

        assert self.checkpoint_config.is_checkpointing_enabled() is False

        self.env.enable_checkpointing(1000)

        assert self.checkpoint_config.is_checkpointing_enabled() is True

    def test_get_set_checkpointing_mode(self):

        assert self.checkpoint_config.get_checkpointing_mode() == CheckpointingMode.EXACTLY_ONCE

        self.checkpoint_config.set_checkpointing_mode(CheckpointingMode.AT_LEAST_ONCE)

        assert self.checkpoint_config.get_checkpointing_mode() == CheckpointingMode.AT_LEAST_ONCE

        self.checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

        assert self.checkpoint_config.get_checkpointing_mode() == CheckpointingMode.EXACTLY_ONCE

    def test_get_set_checkpoint_interval(self):

        assert self.checkpoint_config.get_checkpoint_interval() == -1

        self.checkpoint_config.set_checkpoint_interval(1000)

        assert self.checkpoint_config.get_checkpoint_interval() == 1000

    def test_get_set_checkpoint_timeout(self):

        assert self.checkpoint_config.get_checkpoint_timeout() == 600000

        self.checkpoint_config.set_checkpoint_timeout(300000)

        assert self.checkpoint_config.get_checkpoint_timeout() == 300000

    def test_get_set_min_pause_between_checkpoints(self):

        assert self.checkpoint_config.get_min_pause_between_checkpoints() == 0

        self.checkpoint_config.set_min_pause_between_checkpoints(100000)

        assert self.checkpoint_config.get_min_pause_between_checkpoints() == 100000

    def test_get_set_max_concurrent_checkpoints(self):

        assert self.checkpoint_config.get_max_concurrent_checkpoints() == 1

        self.checkpoint_config.set_max_concurrent_checkpoints(2)

        assert self.checkpoint_config.get_max_concurrent_checkpoints() == 2

    def test_get_set_fail_on_checkpointing_errors(self):

        assert self.checkpoint_config.is_fail_on_checkpointing_errors() is True

        self.checkpoint_config.set_fail_on_checkpointing_errors(False)

        assert self.checkpoint_config.is_fail_on_checkpointing_errors() is False

    def test_get_set_externalized_checkpoints_cleanup(self):

        assert self.checkpoint_config.is_externalized_checkpoints_enabled() is False

        assert self.checkpoint_config.get_externalized_checkpoint_cleanup() is None

        self.checkpoint_config.enable_externalized_checkpoints(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        assert self.checkpoint_config.is_externalized_checkpoints_enabled() is True

        assert self.checkpoint_config.get_externalized_checkpoint_cleanup() == \
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION

        self.checkpoint_config.enable_externalized_checkpoints(
            ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

        assert self.checkpoint_config.get_externalized_checkpoint_cleanup() == \
            ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION

    def test_get_set_prefer_checkpoint_for_recovery(self):

        assert self.checkpoint_config.is_prefer_checkpoint_for_recovery() is False

        self.checkpoint_config.set_prefer_checkpoint_for_recovery(True)

        assert self.checkpoint_config.is_prefer_checkpoint_for_recovery() is True
