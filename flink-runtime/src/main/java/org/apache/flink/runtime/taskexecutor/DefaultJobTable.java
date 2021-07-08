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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Default implementation of the {@link JobTable}. */
public final class DefaultJobTable implements JobTable {
    private final Map<JobID, JobOrConnection> jobs;

    private final Map<ResourceID, JobID> resourceIdJobIdIndex;

    private DefaultJobTable() {
        this.jobs = new HashMap<>();
        this.resourceIdJobIdIndex = new HashMap<>();
    }

    @Override
    public <E extends Exception> Job getOrCreateJob(
            JobID jobId,
            SupplierWithException<? extends JobTable.JobServices, E> jobServicesSupplier)
            throws E {
        JobOrConnection job = jobs.get(jobId);

        if (job == null) {
            job = new JobOrConnection(jobId, jobServicesSupplier.get());
            jobs.put(jobId, job);
        }

        return job;
    }

    @Override
    public Optional<Job> getJob(JobID jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public Optional<Connection> getConnection(JobID jobId) {
        return getJob(jobId).flatMap(Job::asConnection);
    }

    @Override
    public Optional<Connection> getConnection(ResourceID resourceId) {
        final JobID jobId = resourceIdJobIdIndex.get(resourceId);

        if (jobId != null) {
            return getConnection(jobId);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Collection<Job> getJobs() {
        return new ArrayList<>(jobs.values());
    }

    @Override
    public boolean isEmpty() {
        return jobs.isEmpty();
    }

    public static DefaultJobTable create() {
        return new DefaultJobTable();
    }

    @Override
    public void close() {
        for (JobTable.Job job : getJobs()) {
            job.close();
        }
    }

    private final class JobOrConnection implements JobTable.Job, JobTable.Connection {

        private final JobID jobId;

        private final JobTable.JobServices jobServices;

        @Nullable private EstablishedConnection connection;

        private boolean isClosed;

        private JobOrConnection(JobID jobId, JobTable.JobServices jobServices) {
            this.jobId = jobId;
            this.jobServices = jobServices;
            this.connection = null;
            this.isClosed = false;
        }

        @Override
        public boolean isConnected() {
            verifyJobIsNotClosed();
            return connection != null;
        }

        @Override
        public JobTable.Job disconnect() {
            resourceIdJobIdIndex.remove(verifyContainsEstablishedConnection().getResourceID());
            connection = null;

            return this;
        }

        @Override
        public JobMasterId getJobMasterId() {
            return verifyContainsEstablishedConnection().getJobMasterId();
        }

        @Override
        public JobMasterGateway getJobManagerGateway() {
            return verifyContainsEstablishedConnection().getJobMasterGateway();
        }

        @Override
        public TaskManagerActions getTaskManagerActions() {
            return verifyContainsEstablishedConnection().getTaskManagerActions();
        }

        @Override
        public CheckpointResponder getCheckpointResponder() {
            return verifyContainsEstablishedConnection().getCheckpointResponder();
        }

        @Override
        public GlobalAggregateManager getGlobalAggregateManager() {
            return verifyContainsEstablishedConnection().getGlobalAggregateManager();
        }

        @Override
        public LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle() {
            verifyJobIsNotClosed();
            return jobServices.getClassLoaderHandle();
        }

        @Override
        public ResultPartitionConsumableNotifier getResultPartitionConsumableNotifier() {
            return verifyContainsEstablishedConnection().getResultPartitionConsumableNotifier();
        }

        @Override
        public PartitionProducerStateChecker getPartitionStateChecker() {
            return verifyContainsEstablishedConnection().getPartitionStateChecker();
        }

        @Override
        public JobID getJobId() {
            return jobId;
        }

        @Override
        public ResourceID getResourceId() {
            return verifyContainsEstablishedConnection().getResourceID();
        }

        @Override
        public Optional<JobTable.Connection> asConnection() {
            verifyJobIsNotClosed();
            if (connection != null) {
                return Optional.of(this);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public JobTable.Connection connect(
                ResourceID resourceId,
                JobMasterGateway jobMasterGateway,
                TaskManagerActions taskManagerActions,
                CheckpointResponder checkpointResponder,
                GlobalAggregateManager aggregateManager,
                ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
                PartitionProducerStateChecker partitionStateChecker) {
            verifyJobIsNotClosed();
            Preconditions.checkState(connection == null);

            connection =
                    new EstablishedConnection(
                            resourceId,
                            jobMasterGateway,
                            taskManagerActions,
                            checkpointResponder,
                            aggregateManager,
                            resultPartitionConsumableNotifier,
                            partitionStateChecker);
            resourceIdJobIdIndex.put(resourceId, jobId);

            return this;
        }

        @Override
        public void close() {
            if (!isClosed) {
                if (isConnected()) {
                    disconnect();
                }

                jobServices.close();
                jobs.remove(jobId);

                isClosed = true;
            }
        }

        private void verifyJobIsNotClosed() {
            Preconditions.checkState(!isClosed, "The job has been closed.");
        }

        private EstablishedConnection verifyContainsEstablishedConnection() {
            verifyJobIsNotClosed();
            Preconditions.checkState(
                    connection != null, "The job has not been connected to a JobManager.");
            return connection;
        }
    }

    private static final class EstablishedConnection {

        // The unique id used for identifying the job manager
        private final ResourceID resourceID;

        // Gateway to the job master
        private final JobMasterGateway jobMasterGateway;

        // Task manager actions with respect to the connected job manager
        private final TaskManagerActions taskManagerActions;

        // Checkpoint responder for the specific job manager
        private final CheckpointResponder checkpointResponder;

        // GlobalAggregateManager interface to job manager
        private final GlobalAggregateManager globalAggregateManager;

        // Result partition consumable notifier for the specific job manager
        private final ResultPartitionConsumableNotifier resultPartitionConsumableNotifier;

        // Partition state checker for the specific job manager
        private final PartitionProducerStateChecker partitionStateChecker;

        private EstablishedConnection(
                ResourceID resourceID,
                JobMasterGateway jobMasterGateway,
                TaskManagerActions taskManagerActions,
                CheckpointResponder checkpointResponder,
                GlobalAggregateManager globalAggregateManager,
                ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
                PartitionProducerStateChecker partitionStateChecker) {
            this.resourceID = Preconditions.checkNotNull(resourceID);
            this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
            this.taskManagerActions = Preconditions.checkNotNull(taskManagerActions);
            this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
            this.globalAggregateManager = Preconditions.checkNotNull(globalAggregateManager);
            this.resultPartitionConsumableNotifier =
                    Preconditions.checkNotNull(resultPartitionConsumableNotifier);
            this.partitionStateChecker = Preconditions.checkNotNull(partitionStateChecker);
        }

        public ResourceID getResourceID() {
            return resourceID;
        }

        public JobMasterId getJobMasterId() {
            return jobMasterGateway.getFencingToken();
        }

        public JobMasterGateway getJobMasterGateway() {
            return jobMasterGateway;
        }

        public TaskManagerActions getTaskManagerActions() {
            return taskManagerActions;
        }

        public CheckpointResponder getCheckpointResponder() {
            return checkpointResponder;
        }

        public GlobalAggregateManager getGlobalAggregateManager() {
            return globalAggregateManager;
        }

        public ResultPartitionConsumableNotifier getResultPartitionConsumableNotifier() {
            return resultPartitionConsumableNotifier;
        }

        public PartitionProducerStateChecker getPartitionStateChecker() {
            return partitionStateChecker;
        }
    }
}
