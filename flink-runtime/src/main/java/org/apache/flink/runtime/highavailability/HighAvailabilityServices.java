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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.dispatcher.cleanup.GloballyCleanableResource;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The HighAvailabilityServices give access to all services needed for a highly-available setup. In
 * particular, the services provide access to highly available storage and registries, as well as
 * distributed counters and leader election.
 *
 * <ul>
 *   <li>ResourceManager leader election and leader retrieval
 *   <li>JobManager leader election and leader retrieval
 *   <li>Persistence for checkpoint metadata
 *   <li>Registering the latest completed checkpoint(s)
 *   <li>Persistence for the BLOB store
 *   <li>Registry that marks a job's status
 *   <li>Naming of RPC endpoints
 * </ul>
 */
public interface HighAvailabilityServices
        extends ClientHighAvailabilityServices, GloballyCleanableResource {

    // ------------------------------------------------------------------------
    //  Constants
    // ------------------------------------------------------------------------

    /**
     * This UUID should be used when no proper leader election happens, but a simple pre-configured
     * leader is used. That is for example the case in non-highly-available standalone setups.
     */
    UUID DEFAULT_LEADER_ID = new UUID(0, 0);

    /**
     * This JobID should be used to identify the old JobManager when using the {@link
     * HighAvailabilityServices}. With the new mode every JobMaster will have a distinct JobID
     * assigned.
     */
    JobID DEFAULT_JOB_ID = new JobID(0L, 0L);

    // ------------------------------------------------------------------------
    //  Services
    // ------------------------------------------------------------------------

    /** Gets the leader retriever for the cluster's resource manager. */
    LeaderRetrievalService getResourceManagerLeaderRetriever();

    /**
     * Gets the leader retriever for the dispatcher. This leader retrieval service is not always
     * accessible.
     */
    LeaderRetrievalService getDispatcherLeaderRetriever();

    /**
     * Gets the leader retriever for the job JobMaster which is responsible for the given job.
     *
     * @param jobID The identifier of the job.
     * @return Leader retrieval service to retrieve the job manager for the given job
     * @deprecated This method should only be used by the legacy code where the JobManager acts as
     *     the master.
     */
    @Deprecated
    LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID);

    /**
     * Gets the leader retriever for the job JobMaster which is responsible for the given job.
     *
     * @param jobID The identifier of the job.
     * @param defaultJobManagerAddress JobManager address which will be returned by a static leader
     *     retrieval service.
     * @return Leader retrieval service to retrieve the job manager for the given job
     */
    LeaderRetrievalService getJobManagerLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress);

    /**
     * This retriever should no longer be used on the cluster side. The web monitor retriever is
     * only required on the client-side and we have a dedicated high-availability services for the
     * client, named {@link ClientHighAvailabilityServices}. See also FLINK-13750.
     *
     * @return the leader retriever for web monitor
     * @deprecated just use {@link #getClusterRestEndpointLeaderRetriever()} instead of this method.
     */
    @Deprecated
    default LeaderRetrievalService getWebMonitorLeaderRetriever() {
        throw new UnsupportedOperationException(
                "getWebMonitorLeaderRetriever should no longer be used. Instead use "
                        + "#getClusterRestEndpointLeaderRetriever to instantiate the cluster "
                        + "rest endpoint leader retriever. If you called this method, then "
                        + "make sure that #getClusterRestEndpointLeaderRetriever has been "
                        + "implemented by your HighAvailabilityServices implementation.");
    }

    /** Gets the {@link LeaderElection} for the cluster's resource manager. */
    LeaderElection getResourceManagerLeaderElection();

    /** Gets the {@link LeaderElection} for the cluster's dispatcher. */
    LeaderElection getDispatcherLeaderElection();

    /** Gets the {@link LeaderElection} for the job with the given {@link JobID}. */
    LeaderElection getJobManagerLeaderElection(JobID jobID);

    /**
     * Gets the {@link LeaderElection} for the cluster's rest endpoint.
     *
     * @deprecated Use {@link #getClusterRestEndpointLeaderElection()} instead.
     */
    @Deprecated
    default LeaderElection getWebMonitorLeaderElection() {
        throw new UnsupportedOperationException(
                "getWebMonitorLeaderElectionService should no longer be used. Instead use "
                        + "#getClusterRestEndpointLeaderElectionService to instantiate the cluster "
                        + "rest endpoint's leader election service. If you called this method, then "
                        + "make sure that #getClusterRestEndpointLeaderElectionService has been "
                        + "implemented by your HighAvailabilityServices implementation.");
    }

    /**
     * Gets the checkpoint recovery factory for the job manager.
     *
     * @return Checkpoint recovery factory
     */
    CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception;

    /**
     * Gets the submitted job graph store for the job manager.
     *
     * @return Submitted job graph store
     * @throws Exception if the submitted job graph store could not be created
     */
    JobGraphStore getJobGraphStore() throws Exception;

    /**
     * Gets the store that holds information about the state of finished jobs.
     *
     * @return Store of finished job results
     * @throws Exception if job result store could not be created
     */
    JobResultStore getJobResultStore() throws Exception;

    /**
     * Creates the BLOB store in which BLOBs are stored in a highly-available fashion.
     *
     * @return Blob store
     * @throws IOException if the blob store could not be created
     */
    BlobStore createBlobStore() throws IOException;

    /** Gets the {@link LeaderElection} for the cluster's rest endpoint. */
    default LeaderElection getClusterRestEndpointLeaderElection() {
        // for backwards compatibility we delegate to getWebMonitorLeaderElectionService
        // all implementations of this interface should override
        // getClusterRestEndpointLeaderElectionService, though
        return getWebMonitorLeaderElection();
    }

    @Override
    default LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        // for backwards compatibility we delegate to getWebMonitorLeaderRetriever
        // all implementations of this interface should override
        // getClusterRestEndpointLeaderRetriever, though
        return getWebMonitorLeaderRetriever();
    }

    // ------------------------------------------------------------------------
    //  Shutdown and Cleanup
    // ------------------------------------------------------------------------

    /**
     * Closes the high availability services, releasing all resources.
     *
     * <p>This method <b>does not delete or clean up</b> any data stored in external stores (file
     * systems, ZooKeeper, etc). Another instance of the high availability services will be able to
     * recover the job.
     *
     * <p>If an exception occurs during closing services, this method will attempt to continue
     * closing other services and report exceptions only after all services have been attempted to
     * be closed.
     *
     * @throws Exception Thrown, if an exception occurred while closing these services.
     */
    @Override
    void close() throws Exception;

    /**
     * Deletes all data stored by high availability services in external stores.
     *
     * <p>After this method was called, any job or session that was managed by these high
     * availability services will be unrecoverable.
     *
     * <p>If an exception occurs during cleanup, this method will attempt to continue the cleanup
     * and report exceptions only after all cleanup steps have been attempted.
     *
     * @throws Exception if an error occurred while cleaning up data stored by them.
     */
    void cleanupAllData() throws Exception;

    /**
     * Calls {@link #cleanupAllData()} (if {@code true} is passed as a parameter) before calling
     * {@link #close()} on this instance. Any error that appeared during the cleanup will be
     * propagated after calling {@code close()}.
     */
    default void closeWithOptionalClean(boolean cleanupData) throws Exception {
        Throwable exception = null;
        if (cleanupData) {
            try {
                cleanupAllData();
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }
        }
        try {
            close();
        } catch (Throwable t) {
            exception = ExceptionUtils.firstOrSuppressed(t, exception);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    @Override
    default CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        return FutureUtils.completedVoidFuture();
    }
}
