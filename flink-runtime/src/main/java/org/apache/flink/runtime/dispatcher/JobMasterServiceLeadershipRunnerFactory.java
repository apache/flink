/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.DefaultSlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.JobMasterServiceLeadershipRunner;
import org.apache.flink.runtime.jobmaster.SlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Factory which creates a {@link JobMasterServiceLeadershipRunner}. */
public enum JobMasterServiceLeadershipRunnerFactory implements JobManagerRunnerFactory {
    INSTANCE;

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
            long initializationTimestamp)
            throws Exception {

        checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

        final JobMasterConfiguration jobMasterConfiguration =
                JobMasterConfiguration.fromConfiguration(configuration);

        final RunningJobsRegistry runningJobsRegistry =
                highAvailabilityServices.getRunningJobsRegistry();
        final LeaderElectionService jobManagerLeaderElectionService =
                highAvailabilityServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

        final SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        configuration, jobGraph.getJobType());

        if (jobMasterConfiguration.getConfiguration().get(JobManagerOptions.SCHEDULER_MODE)
                == SchedulerExecutionMode.REACTIVE) {
            Preconditions.checkState(
                    slotPoolServiceSchedulerFactory.getSchedulerType()
                            == JobManagerOptions.SchedulerType.Adaptive,
                    "Adaptive Scheduler is required for reactive mode");
        }

        final ShuffleMaster<?> shuffleMaster =
                ShuffleServiceLoader.loadShuffleServiceFactory(configuration)
                        .createShuffleMaster(configuration);

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                jobManagerServices
                        .getLibraryCacheManager()
                        .registerClassLoaderLease(jobGraph.getJobID());

        final ClassLoader userCodeClassLoader =
                classLoaderLease
                        .getOrResolveClassLoader(
                                jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths())
                        .asClassLoader();

        final DefaultJobMasterServiceFactory jobMasterServiceFactory =
                new DefaultJobMasterServiceFactory(
                        jobManagerServices.getScheduledExecutorService(),
                        rpcService,
                        jobMasterConfiguration,
                        jobGraph,
                        highAvailabilityServices,
                        slotPoolServiceSchedulerFactory,
                        jobManagerServices,
                        heartbeatServices,
                        jobManagerJobMetricGroupFactory,
                        fatalErrorHandler,
                        userCodeClassLoader,
                        shuffleMaster,
                        initializationTimestamp);

        final DefaultJobMasterServiceProcessFactory jobMasterServiceProcessFactory =
                new DefaultJobMasterServiceProcessFactory(
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        jobGraph.getCheckpointingSettings(),
                        initializationTimestamp,
                        jobMasterServiceFactory);

        return new JobMasterServiceLeadershipRunner(
                jobMasterServiceProcessFactory,
                jobManagerLeaderElectionService,
                runningJobsRegistry,
                classLoaderLease,
                fatalErrorHandler);
    }
}
