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

package org.apache.flink.client.program;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JobNameBasedJobIdManager}. */
class JobNameBasedJobIdManagerTest {

    static Stream<Arguments> provideJobIndexTestCases() {
        return Stream.of(
                // simple cases
                Arguments.of(0, new JobID(100L, 200L), new JobID(100L, 200L)),
                Arguments.of(1, new JobID(100L, 200L), new JobID(101L, 201L)),
                Arguments.of(-1, new JobID(100L, 200L), new JobID(99L, 199L)),
                // overflow
                Arguments.of(
                        1,
                        new JobID(Long.MAX_VALUE, Long.MAX_VALUE),
                        new JobID(Long.MIN_VALUE, Long.MIN_VALUE)),
                Arguments.of(
                        -1,
                        new JobID(Long.MIN_VALUE, Long.MIN_VALUE),
                        new JobID(Long.MAX_VALUE, Long.MAX_VALUE)),
                Arguments.of(
                        Integer.MAX_VALUE,
                        new JobID(Long.MAX_VALUE, Long.MAX_VALUE),
                        new JobID(
                                Long.MIN_VALUE + Integer.MAX_VALUE - 1,
                                Long.MIN_VALUE + Integer.MAX_VALUE - 1)),
                Arguments.of(
                        Integer.MIN_VALUE,
                        new JobID(Long.MIN_VALUE, Long.MIN_VALUE),
                        new JobID(
                                Long.MAX_VALUE + Integer.MIN_VALUE + 1,
                                Long.MAX_VALUE + Integer.MIN_VALUE + 1)));
    }

    static Stream<Arguments> provideInvalidJobIndexTestCases() {
        return Stream.of(
                // differences don't match between lower and upper parts
                Arguments.of(new JobID(100L, 200L), new JobID(101L, 202L)),
                // differences out of integer range
                Arguments.of(
                        new JobID(100L, 200L),
                        new JobID(100L + Long.MAX_VALUE, 200L + Long.MAX_VALUE)),
                Arguments.of(
                        new JobID(-100L, -200L),
                        new JobID(-100L + Long.MIN_VALUE, -200L + Long.MIN_VALUE)));
    }

    static Stream<Arguments> provideJobIdComparatorTestCases() {
        return Stream.of(
                Arguments.of(
                        0,
                        new JobID(100L, 200L),
                        List.of(1, 5, 3, 2, 4, 0),
                        List.of(0, 1, 2, 3, 4, 5)),
                Arguments.of(
                        0,
                        new JobID(100L, 200L),
                        List.of(1, 5, -3, -2, -4, 0),
                        List.of(0, 1, 5, -4, -3, -2)),
                Arguments.of(
                        Integer.MAX_VALUE,
                        new JobID(100L, 200L),
                        List.of(0, Integer.MAX_VALUE, -2, 1),
                        List.of(Integer.MAX_VALUE, -2, 0, 1)),
                Arguments.of(
                        Integer.MIN_VALUE,
                        new JobID(100L, 200L),
                        List.of(0, Integer.MAX_VALUE, 2, -1),
                        List.of(-1, 0, 2, Integer.MAX_VALUE)));
    }

    @ParameterizedTest
    @MethodSource("provideJobIndexTestCases")
    void testGetJobIndex(int index, JobID baseId, JobID expectedId) {
        JobID jobId = JobNameBasedJobIdManager.fromJobIndex(index, baseId);

        assertThat(jobId).isEqualTo(expectedId);

        assertThat(JobNameBasedJobIdManager.getJobIndex(jobId, baseId)).isEqualTo(index);
    }

    @ParameterizedTest
    @MethodSource("provideInvalidJobIndexTestCases")
    void testGetInvalidJobIndexThrows(JobID baseId, JobID wrongId) {
        assertThatThrownBy(() -> JobNameBasedJobIdManager.getJobIndex(wrongId, baseId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not derived from the base job ID");
    }

    @ParameterizedTest
    @MethodSource("provideJobIdComparatorTestCases")
    void testJobIdComparator(
            int initialIndex,
            JobID baseId,
            List<Integer> indices,
            List<Integer> expectedSortedIndices) {
        List<JobID> jobIds =
                indices.stream()
                        .map(index -> JobNameBasedJobIdManager.fromJobIndex(index, baseId))
                        .sorted(
                                JobNameBasedJobIdManager.createJobIdComparator(
                                        initialIndex, baseId))
                        .collect(Collectors.toList());

        List<Integer> sortedIndices =
                jobIds.stream()
                        .map(jobId -> JobNameBasedJobIdManager.getJobIndex(jobId, baseId))
                        .collect(Collectors.toList());

        assertThat(sortedIndices).isEqualTo(expectedSortedIndices);
    }

    @Test
    void testFirstJob() {
        String jobName = "job1";
        JobID baseJobId = new JobID(1L, 2L);
        JobNameBasedJobIdManager manager =
                new JobNameBasedJobIdManager(baseJobId, Collections.emptyList());

        StreamGraph graph = createStreamGraph(jobName);
        manager.updateJobId(graph);

        assertThat(graph.getJobID()).isEqualTo(baseJobId);
        assertThat(manager.getFirstJobName()).isEqualTo(jobName);
        assertThat(manager.getInitialIndex(jobName)).isEqualTo(0);
        assertThat(manager.getInitialIndex("some-job")).isEqualTo(Objects.hashCode("some-job"));
    }

    @Test
    void testUpdateJobIdWithSameJobName() {
        String jobName = "job1";
        JobID baseJobId = new JobID(1L, 2L);
        JobID jobId2 = new JobID(2L, 3L);
        JobID jobId3 = new JobID(3L, 4L);
        JobNameBasedJobIdManager manager =
                new JobNameBasedJobIdManager(baseJobId, Collections.emptyList());

        StreamGraph graph1 = createStreamGraph(jobName);
        manager.updateJobId(graph1);

        StreamGraph graph2 = createStreamGraph(jobName);
        manager.updateJobId(graph2);

        StreamGraph graph3 = createStreamGraph(jobName);
        manager.updateJobId(graph3);

        assertThat(graph1.getJobID()).isEqualTo(baseJobId);
        assertThat(graph2.getJobID()).isEqualTo(jobId2);
        assertThat(graph3.getJobID()).isEqualTo(jobId3);
    }

    @Test
    void testUpdateJobIdWithRecoveredSameJobName() {
        String jobName = "job1";
        JobID baseJobId = new JobID(1L, 2L);
        JobID jobId2 = new JobID(2L, 3L);
        JobID jobId3 = new JobID(3L, 4L);
        JobNameBasedJobIdManager manager =
                new JobNameBasedJobIdManager(
                        baseJobId,
                        List.of(
                                new JobInfoImpl(jobId2, jobName),
                                new JobInfoImpl(jobId3, jobName),
                                new JobInfoImpl(baseJobId, jobName)));

        StreamGraph graph1 = createStreamGraph(jobName);
        manager.updateJobId(graph1);

        StreamGraph graph2 = createStreamGraph(jobName);
        manager.updateJobId(graph2);

        StreamGraph graph3 = createStreamGraph(jobName);
        manager.updateJobId(graph3);

        assertThat(graph1.getJobID()).isEqualTo(baseJobId);
        assertThat(graph2.getJobID()).isEqualTo(jobId2);
        assertThat(graph3.getJobID()).isEqualTo(jobId3);
    }

    @Test
    void testUpdateJobIdWithDifferentJobNames() {
        String jobName = "job1";
        String jobName2 = "job2";
        String jobName3 = "job3";
        JobID baseJobId = new JobID(1L, 2L);
        JobID jobId2 = new JobID(1L + jobName2.hashCode(), 2L + jobName2.hashCode());
        JobID jobId3 = new JobID(1L + jobName3.hashCode(), 2L + jobName3.hashCode());
        JobNameBasedJobIdManager manager =
                new JobNameBasedJobIdManager(baseJobId, Collections.emptyList());

        StreamGraph graph1 = createStreamGraph(jobName);
        manager.updateJobId(graph1);

        StreamGraph graph3 = createStreamGraph(jobName3);
        manager.updateJobId(graph3);

        StreamGraph graph2 = createStreamGraph(jobName2);
        manager.updateJobId(graph2);

        assertThat(graph1.getJobID()).isEqualTo(baseJobId);
        assertThat(graph2.getJobID()).isEqualTo(jobId2);
        assertThat(graph3.getJobID()).isEqualTo(jobId3);
    }

    @Test
    void testUpdateJobIdWithRecoveredDifferentJobNames() {
        String jobName = "job1";
        String jobName2 = "job2";
        String jobName3 = "job3";
        JobID baseJobId = new JobID(1L, 2L);
        JobID jobId2 = new JobID(1L + jobName2.hashCode(), 2L + jobName2.hashCode());
        JobID jobId3 = new JobID(1L + jobName3.hashCode(), 2L + jobName3.hashCode());
        JobNameBasedJobIdManager manager =
                new JobNameBasedJobIdManager(
                        baseJobId,
                        List.of(
                                new JobInfoImpl(jobId2, jobName2),
                                new JobInfoImpl(jobId3, jobName3),
                                new JobInfoImpl(baseJobId, jobName)));

        StreamGraph graph3 = createStreamGraph(jobName3);
        manager.updateJobId(graph3);

        StreamGraph graph1 = createStreamGraph(jobName);
        manager.updateJobId(graph1);

        StreamGraph graph2 = createStreamGraph(jobName2);
        manager.updateJobId(graph2);

        assertThat(graph1.getJobID()).isEqualTo(baseJobId);
        assertThat(graph2.getJobID()).isEqualTo(jobId2);
        assertThat(graph3.getJobID()).isEqualTo(jobId3);
    }

    @Test
    void testBaseJobIdMissingInRecoveredJobsThrows() {
        JobID baseJobId = new JobID(100L, 200L);
        JobID otherJobId = new JobID(300L, 400L);

        Collection<JobInfo> recoveredJobs =
                Collections.singletonList(new JobInfoImpl(otherJobId, "some-job"));

        assertThatThrownBy(() -> new JobNameBasedJobIdManager(baseJobId, recoveredJobs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Base job ID " + baseJobId + " is not found in the recovered jobs.");
    }

    private StreamGraph createStreamGraph(String jobName) {
        StreamGraph graph =
                new StreamGraph(
                        new Configuration(),
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        SavepointRestoreSettings.none());
        graph.setJobName(jobName);
        return graph;
    }
}
