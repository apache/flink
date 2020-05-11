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
import org.apache.flink.util.function.SupplierWithException;

import java.util.Collection;
import java.util.Optional;

/**
 * A {@link JobTable JobTable's} task is to manage the lifecycle
 * of a job on the {@link TaskExecutor}.
 *
 * <p>There can always only be at most one job per {@link JobID}. In order
 * to create a {@link Job} one needs to provide a {@link JobTable.JobServices}
 * instance which is owned by the job.
 *
 * <p>A job can be connected to a leading JobManager or can be disconnected.
 * In order to establish a connection, one needs to call {@link Job#connect}.
 * Once a job is connected, the respective {@link Connection} can be retrieved
 * via its {@link JobID} or via the {@link ResourceID} of the leader. A connection
 * can be disconnected via {@link Connection#disconnect}.
 *
 * <p>In order to clean up a {@link Job} one first needs to disconnect from the
 * leading JobManager. In order to completely remove the {@link Job} from the
 * {@link JobTable}, one needs to call {@link Job#close} which also closes the
 * associated {@link JobTable.JobServices} instance.
 */
public interface JobTable extends AutoCloseable {

	/**
	 * Gets a registered {@link Job} or creates one if not present.
	 *
	 * @param jobId jobId identifies the job to get
	 * @param jobServicesSupplier jobServicesSupplier create new
	 * {@link JobTable.JobServices} if a new job needs to be created
	 * @return the current job (existing or created) registered under jobId
	 * @throws E if the job services could not be created
	 */
	<E extends Exception> Job getOrCreateJob(
		JobID jobId,
		SupplierWithException<? extends JobTable.JobServices, E> jobServicesSupplier) throws E;

	/**
	 * Gets the job registered under jobId.
	 *
	 * @param jobId jobId identifying the job to get
	 * @return an {@code Optional} containing the {@link Job} registered under jobId, or
	 * an empty {@code Optional} if no job has been registered
	 */
	Optional<Job> getJob(JobID jobId);

	/**
	 * Gets the connection registered under jobId.
	 *
	 * @param jobId jobId identifying the connection to get
	 * @return an {@code Optional} containing the {@link Connection} registered under jobId, or
	 * an empty {@code Optional} if no connection has been registered (this could also mean that
	 * a job which has not been connected exists)
	 */
	Optional<Connection> getConnection(JobID jobId);

	/**
	 * Gets the connection registered under resourceId.
	 *
	 * @param resourceId resourceId identifying the connection to get
	 * @return an {@code Optional} containing the {@link Connection} registered under resourceId,
	 * or an empty {@code Optional} if no connection has been registered
	 */
	Optional<Connection> getConnection(ResourceID resourceId);

	/**
	 * Gets all registered jobs.
	 *
	 * @return collection of registered jobs
	 */
	Collection<Job> getJobs();

	/**
	 * Returns {@code true} if the job table does not contain any jobs.
	 *
	 * @return {@code true} if the job table does not contain any jobs, otherwise {@code false}
	 */
	boolean isEmpty();

	/**
	 * A job contains services which are bound to the lifetime of a Flink job. Moreover, it can
	 * be connected to a leading JobManager and then store further services which are bound to the
	 * lifetime of the JobManager connection.
	 *
	 * <p>Accessing any methods after a job has been closed will throw an {@link IllegalStateException}.
	 */
	interface Job {

		/**
		 * Returns {@code true} if the job is connected to a JobManager.
		 *
		 * @return {@code true} if the job is connected to a JobManager, otherwise {@code false}
		 */
		boolean isConnected();

		/**
		 * Returns the {@link JobID} which is associated with this job.
		 *
		 * @return job id which is associated with this job
		 */
		JobID getJobId();

		/**
		 * Returns the associated {@link Connection} if the job is connected to a JobManager.
		 *
		 * @return an {@code Optional} containing the associated {@link Connection} instance if the job
		 * is connected to a leading JobManager, or an empty {@code Optional} if the job is not connected
		 */
		Optional<Connection> asConnection();

		/**
		 * Connects the job to a JobManager and associates the provided services with this connection.
		 *
		 * <p>A job can only be connected iff {@code Job#isConnected() == false}.
		 *
		 * @param resourceId resourceId of the JobManager to connect to
		 * @param jobMasterGateway jobMasterGateway of the JobManager to connect to
		 * @param taskManagerActions taskManagerActions associated with this connection
		 * @param checkpointResponder checkpointResponder associated with this connection
		 * @param aggregateManager aggregateManager associated with this connection
		 * @param resultPartitionConsumableNotifier resultPartitionConsumableNotifier associated with this connection
		 * @param partitionStateChecker partitionStateChecker associated with this connection
		 * @return the established {@link Connection}
		 * @throws IllegalStateException if the job is already connected
		 */
		Connection connect(
			ResourceID resourceId,
			JobMasterGateway jobMasterGateway,
			TaskManagerActions taskManagerActions,
			CheckpointResponder checkpointResponder,
			GlobalAggregateManager aggregateManager,
			ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
			PartitionProducerStateChecker partitionStateChecker);

		/**
		 * Closes this job and removes it from the owning {@link JobTable}.
		 */
		void close();
	}

	/**
	 * A connection contains services bound to the lifetime of a connection with a JobManager.
	 *
	 * <p>A connection can be disconnected by calling {@link #disconnect()}. Disconnecting a connection
	 * will close all services bound to the connection and return the remaining job instance.
	 *
	 * <p>Accessing any methods after a connection has been disconnected will throw an {@link IllegalStateException}.
	 */
	interface Connection {

		/**
		 * Disconnects the connection, closing all associated services thereby and returning the remaining
		 * job.
		 *
		 * @return the remaining job belonging to this connection
		 */
		Job disconnect();

		JobMasterId getJobMasterId();

		JobMasterGateway getJobManagerGateway();

		TaskManagerActions getTaskManagerActions();

		CheckpointResponder getCheckpointResponder();

		GlobalAggregateManager getGlobalAggregateManager();

		LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle();

		ResultPartitionConsumableNotifier getResultPartitionConsumableNotifier();

		PartitionProducerStateChecker getPartitionStateChecker();

		JobID getJobId();

		ResourceID getResourceId();
	}

	/**
	 * Services associated with a job. The services need to provide a
	 * {@link LibraryCacheManager.ClassLoaderHandle} and will be closed once the associated
	 * {@link JobTable.Job} is being closed.
	 */
	interface JobServices {

		/**
		 * Gets the {@link LibraryCacheManager.ClassLoaderHandle} for the associated job.
		 *
		 * @return {@link LibraryCacheManager.ClassLoaderHandle} for the associated job
		 */
		LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle();

		/**
		 * Closes the job services.
		 *
		 * <p>This method is called once the {@link JobTable.Job} is being closed.
		 */
		void close();
	}
}
