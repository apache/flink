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
from enum import Enum
from pyflink.java_gateway import get_gateway
__all__ = ['ExternalizedCheckpointRetention']


class ExternalizedCheckpointRetention(Enum):
    """
    Cleanup behaviour for externalized checkpoints when the job is cancelled.

    :data:`DELETE_ON_CANCELLATION`:

    Delete externalized checkpoints on job cancellation.

    All checkpoint state will be deleted when you cancel the owning
    job, both the meta data and actual program state. Therefore, you
    cannot resume from externalized checkpoints after the job has been
    cancelled.

    Note that checkpoint state is always kept if the job terminates
    with state ``FAILED``.

    :data:`RETAIN_ON_CANCELLATION`:

    Retain externalized checkpoints on job cancellation.

    All checkpoint state is kept when you cancel the owning job. You
    have to manually delete both the checkpoint meta data and actual
    program state after cancelling the job.

    Note that checkpoint state is always kept if the job terminates
    with state ``FAILED``.

    :data:`NO_EXTERNALIZED_CHECKPOINTS`:

    Externalized checkpoints are disabled completely.
    """

    DELETE_ON_CANCELLATION = 0

    RETAIN_ON_CANCELLATION = 1

    NO_EXTERNALIZED_CHECKPOINTS = 2

    @staticmethod
    def _from_j_externalized_checkpoint_retention(j_retention_mode) \
            -> 'ExternalizedCheckpointRetention':
        return ExternalizedCheckpointRetention[j_retention_mode.name()]

    def _to_j_externalized_checkpoint_retention(self):
        gateway = get_gateway()
        JExternalizedCheckpointRetention = \
            gateway.jvm.org.apache.flink.streaming.api.environment.CheckpointConfig \
            .ExternalizedCheckpointRetention
        return getattr(JExternalizedCheckpointRetention, self.name)
