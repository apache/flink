/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the default checkpoint properties. */
class CheckpointPropertiesTest {

    /** Tests the external checkpoints properties. */
    @Test
    void testCheckpointProperties() {
        CheckpointProperties props =
                CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE);

        assertThat(props.forceCheckpoint()).isFalse();
        assertThat(props.discardOnSubsumed()).isTrue();
        assertThat(props.discardOnJobFinished()).isTrue();
        assertThat(props.discardOnJobCancelled()).isTrue();
        assertThat(props.discardOnJobFailed()).isFalse();
        assertThat(props.discardOnJobSuspended()).isTrue();

        props =
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION);

        assertThat(props.forceCheckpoint()).isFalse();
        assertThat(props.discardOnSubsumed()).isTrue();
        assertThat(props.discardOnJobFinished()).isTrue();
        assertThat(props.discardOnJobCancelled()).isFalse();
        assertThat(props.discardOnJobFailed()).isFalse();
        assertThat(props.discardOnJobSuspended()).isFalse();
    }

    /** Tests the default (manually triggered) savepoint properties. */
    @Test
    void testSavepointProperties() {
        CheckpointProperties props =
                CheckpointProperties.forSavepoint(true, SavepointFormatType.CANONICAL);

        assertThat(props.forceCheckpoint()).isTrue();
        assertThat(props.discardOnSubsumed()).isFalse();
        assertThat(props.discardOnJobFinished()).isFalse();
        assertThat(props.discardOnJobCancelled()).isFalse();
        assertThat(props.discardOnJobFailed()).isFalse();
        assertThat(props.discardOnJobSuspended()).isFalse();
    }

    /** Tests the isSavepoint utility works as expected. */
    @Test
    void testIsSavepoint() throws Exception {
        {
            CheckpointProperties props =
                    CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE);
            assertThat(props.isSavepoint()).isFalse();
        }

        {
            CheckpointProperties props =
                    CheckpointProperties.forCheckpoint(
                            CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION);
            assertThat(props.isSavepoint()).isFalse();
        }

        {
            CheckpointProperties props =
                    CheckpointProperties.forSavepoint(true, SavepointFormatType.CANONICAL);
            assertThat(props.isSavepoint()).isTrue();

            CheckpointProperties deserializedCheckpointProperties =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(props), getClass().getClassLoader());
            assertThat(deserializedCheckpointProperties.isSavepoint()).isTrue();
        }
    }
}
