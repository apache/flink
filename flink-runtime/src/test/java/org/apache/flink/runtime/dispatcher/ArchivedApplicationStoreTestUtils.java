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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava33.com.google.common.base.Ticker;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils class for {@link ArchivedApplicationStore}. */
public class ArchivedApplicationStoreTestUtils {

    static final List<JobStatus> GLOBALLY_TERMINAL_JOB_STATUS =
            Arrays.stream(JobStatus.values())
                    .filter(JobStatus::isGloballyTerminalState)
                    .collect(Collectors.toList());

    static final List<ApplicationState> TERMINAL_APPLICATION_STATUS =
            Arrays.stream(ApplicationState.values())
                    .filter(ApplicationState::isTerminalState)
                    .collect(Collectors.toList());

    /**
     * Generate a specified of ExecutionGraphInfo.
     *
     * @param number the given number
     * @return the result ExecutionGraphInfo collection
     */
    static Collection<ExecutionGraphInfo> generateTerminalExecutionGraphInfos(int number) {
        final Collection<ExecutionGraphInfo> executionGraphInfos = new ArrayList<>(number);

        for (int i = 0; i < number; i++) {
            final JobStatus state =
                    GLOBALLY_TERMINAL_JOB_STATUS.get(
                            ThreadLocalRandom.current()
                                    .nextInt(GLOBALLY_TERMINAL_JOB_STATUS.size()));
            executionGraphInfos.add(
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder().setState(state).build()));
        }

        return executionGraphInfos;
    }

    /**
     * Generate a specified of ArchivedApplication.
     *
     * @param number the given number
     * @return the result ArchivedApplication collection
     */
    static Collection<ArchivedApplication> generateTerminalArchivedApplications(int number) {
        final Collection<ArchivedApplication> archivedApplications = new ArrayList<>(number);

        for (int i = 0; i < number; i++) {
            final ApplicationState state =
                    TERMINAL_APPLICATION_STATUS.get(
                            ThreadLocalRandom.current()
                                    .nextInt(TERMINAL_APPLICATION_STATUS.size()));
            final Map<JobID, ExecutionGraphInfo> jobs =
                    generateTerminalExecutionGraphInfos(1).stream()
                            .collect(
                                    Collectors.toMap(
                                            ExecutionGraphInfo::getJobId,
                                            executionGraphInfo -> executionGraphInfo));
            archivedApplications.add(
                    new ArchivedApplication(
                            ApplicationID.generate(),
                            "test-application-" + i,
                            state,
                            new long[] {1L, 1L, 1L, 1L, 1L, 1L, 1L},
                            jobs,
                            Collections.emptyList()));
        }

        return archivedApplications;
    }

    /**
     * Returns a requirement that an {@link ExecutionGraphInfo} is equivalent to the expected one in
     * the fields that the {@link ArchivedApplicationStore} keeps.
     */
    static Consumer<ExecutionGraphInfo> partiallyEqualTo(ExecutionGraphInfo expected) {
        return actual -> {
            final ArchivedExecutionGraph actualGraph = actual.getArchivedExecutionGraph();
            final ArchivedExecutionGraph expectedGraph = expected.getArchivedExecutionGraph();
            assertThat(actualGraph.isStoppable()).isEqualTo(expectedGraph.isStoppable());
            assertThat(actualGraph.getJobID()).isEqualTo(expectedGraph.getJobID());
            assertThat(actualGraph.getJobName()).isEqualTo(expectedGraph.getJobName());
            assertThat(actualGraph.getState()).isEqualTo(expectedGraph.getState());
            assertThat(actualGraph.getPlan()).isEqualTo(expectedGraph.getPlan());
            assertThat(actualGraph.getAccumulatorsSerialized())
                    .isEqualTo(expectedGraph.getAccumulatorsSerialized());
            assertThat(actualGraph.getCheckpointCoordinatorConfiguration())
                    .isEqualTo(expectedGraph.getCheckpointCoordinatorConfiguration());
            assertThat(actualGraph.getAllVertices()).hasSameSizeAs(expectedGraph.getAllVertices());
            assertThat(actual.getExceptionHistory()).isEqualTo(expected.getExceptionHistory());
        };
    }

    /**
     * Returns a requirement that an {@link ArchivedApplication} has the same ID, name, status and
     * number of jobs as the expected one.
     */
    static Consumer<ArchivedApplication> partiallyEqualTo(ArchivedApplication expected) {
        return actual -> {
            assertThat(actual.getApplicationId()).isEqualTo(expected.getApplicationId());
            assertThat(actual.getApplicationName()).isEqualTo(expected.getApplicationName());
            assertThat(actual.getApplicationStatus()).isEqualTo(expected.getApplicationStatus());
            assertThat(actual.getJobs()).hasSameSizeAs(expected.getJobs());
        };
    }

    static Collection<JobDetails> generateJobDetails(
            Collection<ArchivedApplication> archivedApplications) {
        return archivedApplications.stream()
                .flatMap(archivedApplication -> archivedApplication.getJobs().values().stream())
                .map(JobDetails::createDetailsForJob)
                .collect(Collectors.toList());
    }

    static Collection<ApplicationDetails> generateApplicationDetails(
            Collection<ArchivedApplication> archivedApplications) {
        return archivedApplications.stream()
                .map(ApplicationDetails::fromArchivedApplication)
                .collect(Collectors.toList());
    }

    /**
     * Invokable which signals with {@link
     * ArchivedApplicationStoreTestUtils.SignallingBlockingNoOpInvokable#LATCH} when it is invoked
     * and blocks forever afterward.
     */
    public static class SignallingBlockingNoOpInvokable extends AbstractInvokable {

        /** Latch used to signal an initial invocation. */
        public static final OneShotLatch LATCH = new OneShotLatch();

        public SignallingBlockingNoOpInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            LATCH.trigger();
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    /** MiniCluster with specified {@link ArchivedApplicationStore}. */
    static class PersistingMiniCluster extends MiniCluster {
        @Nullable private final File rootDir;
        private final ScheduledExecutor scheduledExecutor;

        PersistingMiniCluster(
                MiniClusterConfiguration miniClusterConfiguration,
                ScheduledExecutor scheduledExecutor) {
            this(miniClusterConfiguration, null, scheduledExecutor);
        }

        PersistingMiniCluster(
                MiniClusterConfiguration miniClusterConfiguration,
                @Nullable File rootDir,
                ScheduledExecutor scheduledExecutor) {
            super(miniClusterConfiguration);
            this.rootDir = rootDir;
            this.scheduledExecutor = scheduledExecutor;
        }

        @Override
        protected Collection<? extends DispatcherResourceManagerComponent>
                createDispatcherResourceManagerComponents(
                        Configuration configuration,
                        RpcServiceFactory rpcServiceFactory,
                        BlobServer blobServer,
                        HeartbeatServices heartbeatServices,
                        DelegationTokenManager delegationTokenManager,
                        MetricRegistry metricRegistry,
                        MetricQueryServiceRetriever metricQueryServiceRetriever,
                        FatalErrorHandler fatalErrorHandler)
                        throws Exception {
            final DispatcherResourceManagerComponentFactory
                    dispatcherResourceManagerComponentFactory =
                            DefaultDispatcherResourceManagerComponentFactory
                                    .createSessionComponentFactory(
                                            StandaloneResourceManagerFactory.getInstance());

            JobManagerOptions.ArchivedApplicationStoreType archivedApplicationStoreType =
                    configuration.get(JobManagerOptions.COMPLETED_APPLICATION_STORE_TYPE);
            final ArchivedApplicationStore archivedApplicationStore;
            switch (archivedApplicationStoreType) {
                case File:
                    {
                        archivedApplicationStore =
                                createFileArchivedApplicationStore(rootDir, scheduledExecutor);
                        break;
                    }
                case Memory:
                    {
                        archivedApplicationStore = new MemoryArchivedApplicationStore();
                        break;
                    }
                default:
                    {
                        throw new UnsupportedOperationException(
                                "Unsupported archived application store type "
                                        + archivedApplicationStoreType);
                    }
            }

            return Collections.singleton(
                    dispatcherResourceManagerComponentFactory.create(
                            configuration,
                            ResourceID.generate(),
                            getIOExecutor(),
                            rpcServiceFactory.createRpcService(),
                            getHaServices(),
                            blobServer,
                            heartbeatServices,
                            delegationTokenManager,
                            metricRegistry,
                            archivedApplicationStore,
                            metricQueryServiceRetriever,
                            Collections.emptySet(),
                            fatalErrorHandler));
        }
    }

    static FileArchivedApplicationStore createFileArchivedApplicationStore(
            File storageDirectory, ScheduledExecutor scheduledExecutor) throws IOException {
        return new FileArchivedApplicationStore(
                storageDirectory,
                Duration.ofHours(1L),
                Integer.MAX_VALUE,
                10000L,
                scheduledExecutor,
                Ticker.systemTicker());
    }
}
