/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.benchmark;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.NoOpJobGraphListener;
import org.apache.flink.runtime.dispatcher.TestingDispatcher;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.dispatcher.benchmark.TestingDispatcherHighAvailabilityService.createAndGetFile;

/** Benchmark for submitting job to dispatcher with {@link HighAvailabilityServices}. */
public class DispatcherHighAvailabilityServiceBenchmark {
    private TestingDispatcherHighAvailabilityService highAvailabilityServices;
    private JobGraphStore jobGraphStore;
    private TestingDispatcher dispatcher;
    private DispatcherGateway dispatcherGateway;
    private RpcService rpcService;
    private BlobServer blobServer;

    public void setup(Configuration configuration, File highAvailabilityPath) throws Exception {
        this.highAvailabilityServices =
                TestingDispatcherHighAvailabilityService.fromConfiguration(
                        configuration, highAvailabilityPath);
        this.jobGraphStore = highAvailabilityServices.highAvailabilityService().getJobGraphStore();
        this.jobGraphStore.start(NoOpJobGraphListener.INSTANCE);

        this.rpcService = new TestingRpcService();
        this.blobServer =
                new BlobServer(
                        configuration,
                        createAndGetFile(highAvailabilityPath, "blobserver"),
                        BlobUtils.createBlobStoreFromConfig(configuration));
        this.dispatcher =
                TestingDispatcher.builder()
                        .setJobGraphWriter(jobGraphStore)
                        .setJobResultStore(
                                highAvailabilityServices
                                        .highAvailabilityService()
                                        .getJobResultStore())
                        .setBlobServer(blobServer)
                        .setJobManagerRunnerFactory(new FinishedJobManagerRunnerFactory())
                        .build(rpcService);

        this.dispatcher.start();
        this.dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
    }

    public void submitJob() throws Exception {
        JobGraph job = JobGraphTestUtils.singleNoOpJobGraph();
        dispatcherGateway.submitJob(job, Time.of(1, TimeUnit.MINUTES)).get();
        dispatcher.cleanupJob(job.getJobID());
    }

    public void close() throws Exception {
        this.dispatcher.shutDownCluster();
        this.rpcService.close();
        this.dispatcher.close();
        this.blobServer.close();
        this.jobGraphStore.stop();
        this.highAvailabilityServices.close();
    }

    /** Finished job manager runner. */
    private static class FinishedJobManagerRunnerFactory implements JobManagerRunnerFactory {

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp) {
            return TestingJobManagerRunner.newBuilder().setJobId(jobGraph.getJobID()).build();
        }
    }
}
