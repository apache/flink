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

package org.apache.flink.client.cli;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.util.MockedCliFrontend;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the LIST command. */
class CliFrontendListTest extends CliFrontendTestBase {

    @BeforeAll
    static void init() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterAll
    static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @Test
    void testListOptions() throws Exception {
        // test configure all job
        {
            String[] parameters = {"-a"};
            ListOptions options =
                    new ListOptions(
                            CliFrontendParser.parse(
                                    CliFrontendParser.getListCommandOptions(), parameters, true));
            assertThat(options.showAll()).isTrue();
            assertThat(options.showRunning()).isFalse();
            assertThat(options.showScheduled()).isFalse();
        }

        // test configure running job
        {
            String[] parameters = {"-r"};
            ListOptions options =
                    new ListOptions(
                            CliFrontendParser.parse(
                                    CliFrontendParser.getListCommandOptions(), parameters, true));
            assertThat(options.showAll()).isFalse();
            assertThat(options.showRunning()).isTrue();
            assertThat(options.showScheduled()).isFalse();
        }

        // test configure scheduled job
        {
            String[] parameters = {"-s"};
            ListOptions options =
                    new ListOptions(
                            CliFrontendParser.parse(
                                    CliFrontendParser.getListCommandOptions(), parameters, true));
            assertThat(options.showAll()).isFalse();
            assertThat(options.showRunning()).isFalse();
            assertThat(options.showScheduled()).isTrue();
        }
    }

    @Test
    void testUnrecognizedOption() {
        assertThatThrownBy(
                        () -> {
                            String[] parameters = {"-v", "-k"};
                            Configuration configuration = getConfiguration();
                            CliFrontend testFrontend =
                                    new CliFrontend(
                                            configuration, Collections.singletonList(getCli()));
                            testFrontend.list(parameters);
                        })
                .isInstanceOf(CliArgsException.class);
    }

    @Test
    void testList() throws Exception {
        // test list properly
        {
            String[] parameters = {"-r", "-s", "-a"};
            ClusterClient<String> clusterClient = createClusterClient();
            MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
            testFrontend.list(parameters);
            verify(clusterClient, times(1)).listJobs();
        }
    }

    @Test
    void testSorting() {
        List<JobStatusMessage> sortedJobs =
                CliFrontend.sortJobStatusMessages(getJobStatusMessages())
                        .collect(Collectors.toList());
        assertThat(sortedJobs)
                .isSortedAccordingTo(Comparator.comparing(JobStatusMessage::getStartTime));
    }

    private static ClusterClient<String> createClusterClient() throws Exception {
        final ClusterClient<String> clusterClient = mock(ClusterClient.class);
        when(clusterClient.listJobs())
                .thenReturn(CompletableFuture.completedFuture(getJobStatusMessages()));
        return clusterClient;
    }

    private static List<JobStatusMessage> getJobStatusMessages() {
        return Arrays.asList(
                new JobStatusMessage(new JobID(), "job1", JobStatus.RUNNING, 1665197322962L),
                new JobStatusMessage(new JobID(), "job2", JobStatus.CREATED, 1660904115054L),
                new JobStatusMessage(new JobID(), "job3", JobStatus.RUNNING, 1664177946934L),
                new JobStatusMessage(new JobID(), "job4", JobStatus.FINISHED, 1665738051581L));
    }
}
