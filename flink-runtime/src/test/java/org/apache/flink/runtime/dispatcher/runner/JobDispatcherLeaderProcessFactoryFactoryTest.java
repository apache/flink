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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.TestingPartialDispatcherServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.StandaloneJobGraphStore;
import org.apache.flink.runtime.jobmanager.TestingJobPersistenceComponentFactory;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(TestLoggerExtension.class)
class JobDispatcherLeaderProcessFactoryFactoryTest {

    @TempDir private Path temporaryFolder;

    @Test
    public void testJobGraphWithoutDirtyJobResult() throws IOException {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();

        final JobDispatcherLeaderProcessFactory factory =
                createDispatcherLeaderProcessFactoryFromTestInstance(
                        jobGraph, null, temporaryFolder);

        assertThat(factory.getJobGraph()).isEqualTo(jobGraph);
        assertThat(factory.getRecoveredDirtyJobResult()).isNull();
    }

    @Test
    public void testJobGraphWithMatchingDirtyJobResult() throws IOException {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        final JobResult jobResult =
                TestingJobResultStore.createSuccessfulJobResult(jobGraph.getJobID());

        final JobDispatcherLeaderProcessFactory factory =
                createDispatcherLeaderProcessFactoryFromTestInstance(
                        jobGraph, jobResult, temporaryFolder);

        assertThat(factory.getJobGraph()).isNull();
        assertThat(factory.getRecoveredDirtyJobResult()).isEqualTo(jobResult);
    }

    @Test
    public void testJobGraphWithNotMatchingDirtyJobResult() throws IOException {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        final JobResult jobResult = TestingJobResultStore.createSuccessfulJobResult(new JobID());

        final JobDispatcherLeaderProcessFactory factory =
                createDispatcherLeaderProcessFactoryFromTestInstance(
                        jobGraph, jobResult, temporaryFolder);

        assertThat(factory.getJobGraph()).isEqualTo(jobGraph);
        assertThat(factory.getRecoveredDirtyJobResult()).isNull();
    }

    @Test
    public void testMissingJobGraph() throws IOException {
        assertThatThrownBy(
                        () ->
                                createDispatcherLeaderProcessFactoryFromTestInstance(
                                        null,
                                        TestingJobResultStore.createSuccessfulJobResult(
                                                new JobID()),
                                        temporaryFolder))
                .isInstanceOf(NullPointerException.class);
    }

    private static JobDispatcherLeaderProcessFactory
            createDispatcherLeaderProcessFactoryFromTestInstance(
                    @Nullable JobGraph jobGraph,
                    @Nullable JobResult dirtyJobResult,
                    Path storageDir)
                    throws IOException {
        final JobDispatcherLeaderProcessFactoryFactory testInstance =
                new JobDispatcherLeaderProcessFactoryFactory(ignoredConfig -> jobGraph);

        final TestingJobResultStore jobResultStore =
                TestingJobResultStore.builder()
                        .withGetDirtyResultsSupplier(
                                () -> CollectionUtil.ofNullable(dirtyJobResult))
                        .build();
        final JobGraphStore jobGraphStore = new StandaloneJobGraphStore();
        return testInstance.createFactory(
                new TestingJobPersistenceComponentFactory(jobGraphStore, jobResultStore),
                Executors.directExecutor(),
                new TestingRpcService(),
                TestingPartialDispatcherServices.builder()
                        .withHighAvailabilityServices(
                                new TestingHighAvailabilityServicesBuilder()
                                        .setJobGraphStore(jobGraphStore)
                                        .setJobResultStore(jobResultStore)
                                        .build())
                        .build(storageDir.toFile(), new Configuration()),
                NoOpFatalErrorHandler.INSTANCE);
    }
}
