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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.rest.messages.checkpoints.SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics.CheckpointAlignment;
import org.apache.flink.runtime.rest.messages.checkpoints.SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics.CheckpointDuration;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for regional checkpoint fields (ref_checkpoint_id, oldest_ref_checkpoint_id). */
class CheckpointStatisticsRegionalTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testSubtaskRefCheckpointIdIncludedWhenSet() throws Exception {
        final CompletedSubtaskCheckpointStatistics stats =
                new CompletedSubtaskCheckpointStatistics(
                        0,
                        1000L,
                        500L,
                        1024L,
                        512L,
                        new CheckpointDuration(10L, 20L),
                        new CheckpointAlignment(0L, 100L, 50L, 30L),
                        5L,
                        false,
                        false,
                        42L);

        final JsonNode json = MAPPER.valueToTree(stats);
        assertThat(json.has("ref_checkpoint_id")).isTrue();
        assertThat(json.get("ref_checkpoint_id").asLong()).isEqualTo(42L);
    }

    @Test
    void testSubtaskRefCheckpointIdOmittedWhenNull() throws Exception {
        final CompletedSubtaskCheckpointStatistics stats =
                new CompletedSubtaskCheckpointStatistics(
                        0,
                        1000L,
                        500L,
                        1024L,
                        512L,
                        new CheckpointDuration(10L, 20L),
                        new CheckpointAlignment(0L, 100L, 50L, 30L),
                        5L,
                        false,
                        false,
                        null);

        final JsonNode json = MAPPER.valueToTree(stats);
        assertThat(json.has("ref_checkpoint_id")).isFalse();
    }

    @Test
    void testTaskOldestRefCheckpointIdIncludedWhenSet() throws Exception {
        final TaskCheckpointStatistics stats =
                new TaskCheckpointStatistics(
                        1L,
                        CheckpointStatsStatus.COMPLETED,
                        1000L,
                        2048L,
                        1024L,
                        500L,
                        0L,
                        100L,
                        50L,
                        4,
                        4,
                        7L);

        final JsonNode json = MAPPER.valueToTree(stats);
        assertThat(json.has("oldest_ref_checkpoint_id")).isTrue();
        assertThat(json.get("oldest_ref_checkpoint_id").asLong()).isEqualTo(7L);
    }

    @Test
    void testTaskOldestRefCheckpointIdOmittedWhenNull() throws Exception {
        final TaskCheckpointStatistics stats =
                new TaskCheckpointStatistics(
                        1L,
                        CheckpointStatsStatus.COMPLETED,
                        1000L,
                        2048L,
                        1024L,
                        500L,
                        0L,
                        100L,
                        50L,
                        4,
                        4,
                        null);

        final JsonNode json = MAPPER.valueToTree(stats);
        assertThat(json.has("oldest_ref_checkpoint_id")).isFalse();
    }

    @Test
    void testSubtaskRefCheckpointIdRoundTrip() throws Exception {
        final CompletedSubtaskCheckpointStatistics original =
                new CompletedSubtaskCheckpointStatistics(
                        0,
                        1000L,
                        500L,
                        1024L,
                        512L,
                        new CheckpointDuration(10L, 20L),
                        new CheckpointAlignment(0L, 100L, 50L, 30L),
                        5L,
                        false,
                        false,
                        99L);

        final String json = MAPPER.writeValueAsString(original);
        final SubtaskCheckpointStatistics deserialized =
                MAPPER.readValue(json, SubtaskCheckpointStatistics.class);

        assertThat(deserialized).isInstanceOf(CompletedSubtaskCheckpointStatistics.class);
        final CompletedSubtaskCheckpointStatistics completed =
                (CompletedSubtaskCheckpointStatistics) deserialized;
        assertThat(completed.getRefCheckpointId()).isEqualTo(99L);
    }

    @Test
    void testTaskOldestRefCheckpointIdRoundTrip() throws Exception {
        final TaskCheckpointStatistics original =
                new TaskCheckpointStatistics(
                        1L,
                        CheckpointStatsStatus.COMPLETED,
                        1000L,
                        2048L,
                        1024L,
                        500L,
                        0L,
                        100L,
                        50L,
                        4,
                        4,
                        15L);

        final String json = MAPPER.writeValueAsString(original);
        final TaskCheckpointStatistics deserialized =
                MAPPER.readValue(json, TaskCheckpointStatistics.class);

        assertThat(deserialized.getOldestRefCheckpointId()).isEqualTo(15L);
    }

    @Test
    void testCheckpointLevelOldestRefCheckpointIdRoundTrip() throws Exception {
        // Use generateCheckpointStatistics to produce a valid CompletedCheckpointStatistics
        // with oldest_ref_checkpoint_id set, then verify it survives JSON round-trip.
        // This reuses the same aggregation path tested in
        // RegionalCheckpointStatisticsAggregationTest.
        // Here we focus on serialization integrity of the top-level field.
        final org.apache.flink.runtime.checkpoint.CheckpointStatsStatus status =
                org.apache.flink.runtime.checkpoint.CheckpointStatsStatus.COMPLETED;
        final org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics stats =
                new org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics
                        .CompletedCheckpointStatistics(
                        1L,
                        status,
                        false,
                        null,
                        1000L,
                        500L,
                        1024L,
                        2048L,
                        0L,
                        0L,
                        0L,
                        0L,
                        4,
                        4,
                        org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics
                                .RestAPICheckpointType.CHECKPOINT,
                        java.util.Collections.emptyMap(),
                        null,
                        false,
                        42L);

        final String json = MAPPER.writeValueAsString(stats);
        final JsonNode jsonNode = MAPPER.readTree(json);
        assertThat(jsonNode.has("oldest_ref_checkpoint_id")).isTrue();
        assertThat(jsonNode.get("oldest_ref_checkpoint_id").asLong()).isEqualTo(42L);
    }

    @Test
    void testCheckpointLevelOldestRefCheckpointIdOmittedWhenNull() throws Exception {
        final org.apache.flink.runtime.checkpoint.CheckpointStatsStatus status =
                org.apache.flink.runtime.checkpoint.CheckpointStatsStatus.COMPLETED;
        final org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics stats =
                new org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics
                        .CompletedCheckpointStatistics(
                        1L,
                        status,
                        false,
                        null,
                        1000L,
                        500L,
                        1024L,
                        2048L,
                        0L,
                        0L,
                        0L,
                        0L,
                        4,
                        4,
                        org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics
                                .RestAPICheckpointType.CHECKPOINT,
                        java.util.Collections.emptyMap(),
                        null,
                        false,
                        null);

        final JsonNode jsonNode = MAPPER.valueToTree(stats);
        assertThat(jsonNode.has("oldest_ref_checkpoint_id")).isFalse();
    }
}
