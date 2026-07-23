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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests (un)marshalling of the {@link SubtaskExecutionAttemptDetailsInfo}. */
@ExtendWith(NoOpTestExtension.class)
class SubtaskExecutionAttemptDetailsInfoTest
        extends RestResponseMarshallingTestBase<SubtaskExecutionAttemptDetailsInfo> {

    @Override
    protected Class<SubtaskExecutionAttemptDetailsInfo> getTestResponseClass() {
        return SubtaskExecutionAttemptDetailsInfo.class;
    }

    @Override
    protected SubtaskExecutionAttemptDetailsInfo getTestResponseInstance() throws Exception {
        final Random random = new Random();

        final Map<String, Long> recordsWrittenPerTarget = new HashMap<>();
        recordsWrittenPerTarget.put(
                "abcdef0123456789abcdef0123456789", Math.abs(random.nextLong()));
        recordsWrittenPerTarget.put(
                "0123456789abcdef0123456789abcdef", Math.abs(random.nextLong()));

        final IOMetricsInfo ioMetricsInfo =
                new IOMetricsInfo(
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        Math.abs(random.nextLong()),
                        Math.abs(random.nextDouble()),
                        recordsWrittenPerTarget);

        final Map<ExecutionState, Long> statusDuration = new HashMap<>();
        statusDuration.put(ExecutionState.CREATED, 10L);
        statusDuration.put(ExecutionState.SCHEDULED, 20L);
        statusDuration.put(ExecutionState.DEPLOYING, 30L);
        statusDuration.put(ExecutionState.INITIALIZING, 40L);
        statusDuration.put(ExecutionState.RUNNING, 50L);

        return new SubtaskExecutionAttemptDetailsInfo(
                Math.abs(random.nextInt()),
                ExecutionState.values()[random.nextInt(ExecutionState.values().length)],
                Math.abs(random.nextInt()),
                "localhost:" + random.nextInt(65536),
                Math.abs(random.nextLong()),
                Math.abs(random.nextLong()),
                Math.abs(random.nextLong()),
                ioMetricsInfo,
                "taskmanagerId",
                statusDuration,
                null);
    }

    /**
     * Verifies that an {@link IOMetricsInfo} constructed via the legacy (back-compat) constructor
     * round-trips through Jackson with an empty {@code write-records-per-target} map rather than
     * {@code null}. Guards against NPEs on the consumer side (e.g. autoscaler) when talking to a
     * Flink version that pre-dates the per-target metric.
     */
    @Test
    void testIOMetricsInfoMarshallingWithEmptyPerTargetMap() throws Exception {
        final IOMetricsInfo original =
                new IOMetricsInfo(1L, true, 2L, true, 3L, true, 4L, true, 5L, 6L, 7.0);

        final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        final String json = mapper.writeValueAsString(original);
        final IOMetricsInfo deserialized = mapper.readValue(json, IOMetricsInfo.class);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getRecordsWrittenPerTarget()).isNotNull().isEmpty();
        assertThat(json).contains(IOMetricsInfo.FIELD_NAME_RECORDS_WRITTEN_PER_TARGET);
    }

    /**
     * Verifies that a populated {@code write-records-per-target} map survives a Jackson round-trip
     * intact, including the JSON field name used by external consumers.
     */
    @Test
    void testIOMetricsInfoMarshallingWithPerTargetEntries() throws Exception {
        final Map<String, Long> perTarget = new HashMap<>();
        perTarget.put("abcdef0123456789abcdef0123456789", 42L);
        perTarget.put("0123456789abcdef0123456789abcdef", 7L);

        final IOMetricsInfo original =
                new IOMetricsInfo(1L, true, 2L, true, 3L, true, 4L, true, 5L, 6L, 7.0, perTarget);

        final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        final String json = mapper.writeValueAsString(original);
        final IOMetricsInfo deserialized = mapper.readValue(json, IOMetricsInfo.class);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getRecordsWrittenPerTarget())
                .containsExactlyInAnyOrderEntriesOf(perTarget);
        assertThat(json)
                .contains("\"" + IOMetricsInfo.FIELD_NAME_RECORDS_WRITTEN_PER_TARGET + "\"");
    }
}
