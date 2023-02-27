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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
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

import org.apache.flink.shaded.guava30.com.google.common.base.Ticker;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/** Test utils class for {@link FileExecutionGraphInfoStore}. */
public class ExecutionGraphInfoStoreTestUtils {

    static final List<JobStatus> GLOBALLY_TERMINAL_JOB_STATUS =
            Arrays.stream(JobStatus.values())
                    .filter(JobStatus::isGloballyTerminalState)
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

    /** Compare whether two ExecutionGraphInfo instances are equivalent. */
    public static boolean areExecutionGraphInfoMathPartially(
            ExecutionGraphInfo left, ExecutionGraphInfo right) {
        if (left == right) {
            return true;
        }

        if (right == null || left == null) {
            return false;
        }

        ArchivedExecutionGraph leftExecutionGraph = left.getArchivedExecutionGraph();
        ArchivedExecutionGraph rightExecutionGraph = right.getArchivedExecutionGraph();
        return leftExecutionGraph.isStoppable() == rightExecutionGraph.isStoppable()
                && Objects.equals(leftExecutionGraph.getJobID(), rightExecutionGraph.getJobID())
                && Objects.equals(leftExecutionGraph.getJobName(), rightExecutionGraph.getJobName())
                && leftExecutionGraph.getState() == rightExecutionGraph.getState()
                && Objects.equals(
                        leftExecutionGraph.getJsonPlan(), rightExecutionGraph.getJsonPlan())
                && Objects.equals(
                        leftExecutionGraph.getAccumulatorsSerialized(),
                        rightExecutionGraph.getAccumulatorsSerialized())
                && Objects.equals(
                        leftExecutionGraph.getCheckpointCoordinatorConfiguration(),
                        rightExecutionGraph.getCheckpointCoordinatorConfiguration())
                && leftExecutionGraph.getAllVertices().size()
                        == rightExecutionGraph.getAllVertices().size()
                && Objects.equals(left.getExceptionHistory(), right.getExceptionHistory());
    }

    static Collection<JobDetails> generateJobDetails(
            Collection<ExecutionGraphInfo> executionGraphInfos) {
        return executionGraphInfos.stream()
                .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                .map(JobDetails::createDetailsForJob)
                .collect(Collectors.toList());
    }

    /**
     * Invokable which signals with {@link
     * ExecutionGraphInfoStoreTestUtils.SignallingBlockingNoOpInvokable#LATCH} when it is invoked
     * and blocks forever afterwards.
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

    /** MiniCluster with specified {@link ExecutionGraphInfoStore}. */
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

            JobManagerOptions.JobStoreType jobStoreType =
                    configuration.get(JobManagerOptions.JOB_STORE_TYPE);
            final ExecutionGraphInfoStore executionGraphInfoStore;
            switch (jobStoreType) {
                case File:
                    {
                        executionGraphInfoStore =
                                createDefaultExecutionGraphInfoStore(rootDir, scheduledExecutor);
                        break;
                    }
                case Memory:
                    {
                        executionGraphInfoStore = new MemoryExecutionGraphInfoStore();
                        break;
                    }
                default:
                    {
                        throw new UnsupportedOperationException(
                                "Unsupported job store type " + jobStoreType);
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
                            executionGraphInfoStore,
                            metricQueryServiceRetriever,
                            fatalErrorHandler));
        }
    }

    static FileExecutionGraphInfoStore createDefaultExecutionGraphInfoStore(
            File storageDirectory, ScheduledExecutor scheduledExecutor) throws IOException {
        return new FileExecutionGraphInfoStore(
                storageDirectory,
                Time.hours(1L),
                Integer.MAX_VALUE,
                10000L,
                scheduledExecutor,
                Ticker.systemTicker());
    }
}
