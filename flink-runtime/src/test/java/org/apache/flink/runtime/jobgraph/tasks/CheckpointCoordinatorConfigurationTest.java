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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.COORDINATOR_STATE_INLINE_THRESHOLD_NOT_SET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CheckpointCoordinatorConfiguration}. */
class CheckpointCoordinatorConfigurationTest {

    @Test
    void testCoordinatorStateInlineThresholdIsAtLeastZeroOrNotSet() {
        assertThatThrownBy(
                        () ->
                                CheckpointCoordinatorConfiguration.builder()
                                        .setCoordinatorStateInlineThreshold(-2)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCoordinatorStateInlineThreshold(
                                        COORDINATOR_STATE_INLINE_THRESHOLD_NOT_SET)
                                .build()
                                .getCoordinatorStateInlineThreshold())
                .isEqualTo(COORDINATOR_STATE_INLINE_THRESHOLD_NOT_SET);
        assertThat(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCoordinatorStateInlineThreshold(0)
                                .build()
                                .getCoordinatorStateInlineThreshold())
                .isZero();
    }

    /**
     * The job graph Java-serializes this configuration, for example in a high availability store.
     * One serialized by an earlier version, which lacks the coordinator state options, has the
     * option disabled. The file checkpoint-coordinator-configuration-flink2.3 holds such a
     * configuration, written with Flink 2.3.0 on the classpath by:
     *
     * <pre>{@code
     * CheckpointCoordinatorConfiguration configuration =
     *         CheckpointCoordinatorConfiguration.builder()
     *                 .setCheckpointInterval(1234L)
     *                 .setCheckpointTimeout(5678L)
     *                 .build();
     * Files.write(file, InstantiationUtil.serializeObject(configuration));
     * }</pre>
     */
    @Test
    void testConfigurationSerializedByAnEarlierVersionHasTheOptionDisabled() throws Exception {
        final CheckpointCoordinatorConfiguration configuration;
        try (InputStream in =
                getClass()
                        .getClassLoader()
                        .getResourceAsStream("checkpoint-coordinator-configuration-flink2.3")) {
            configuration = InstantiationUtil.deserializeObject(in, getClass().getClassLoader());
        }

        assertThat(configuration.getCheckpointInterval()).isEqualTo(1234L);
        assertThat(configuration.getCheckpointTimeout()).isEqualTo(5678L);
        assertThat(configuration.isCoordinatorStateInFileEnabled()).isFalse();
    }

    @Test
    void testCoordinatorStateOptionsTakePartInEquality() {
        final CheckpointCoordinatorConfiguration defaults =
                CheckpointCoordinatorConfiguration.builder().build();

        assertThat(CheckpointCoordinatorConfiguration.builder().build())
                .isEqualTo(defaults)
                .hasSameHashCodeAs(defaults);
        assertThat(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCoordinatorStateInFileEnabled(true)
                                .build())
                .isNotEqualTo(defaults);
        assertThat(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCoordinatorStateInlineThreshold(1024)
                                .build())
                .isNotEqualTo(defaults);
    }
}
