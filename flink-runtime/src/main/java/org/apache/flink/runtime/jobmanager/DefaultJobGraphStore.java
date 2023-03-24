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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation for {@link JobGraphStore}. Combined with different {@link
 * StateHandleStore}, we could persist the job graphs to various distributed storage. Also combined
 * with different {@link JobGraphStoreWatcher}, we could get all the changes on the job graph store
 * and do the response.
 */
public class DefaultJobGraphStore<R extends ResourceVersion<R>>
        implements JobGraphStore, JobGraphStore.JobGraphListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobGraphStore.class);

    /** Lock to synchronize with the {@link JobGraphListener}. */
    private final Object lock = new Object();

    /** The set of IDs of all added job graphs. */
    @GuardedBy("lock")
    private final Set<JobID> addedJobGraphs = new HashSet<>();

    /** Submitted job graphs handle store. */
    private final StateHandleStore<JobGraph, R> jobGraphStateHandleStore;

    @GuardedBy("lock")
    private final JobGraphStoreWatcher jobGraphStoreWatcher;

    private final JobGraphStoreUtil jobGraphStoreUtil;

    /** The external listener to be notified on races. */
    @GuardedBy("lock")
    private JobGraphListener jobGraphListener;

    /** Flag indicating whether this instance is running. */
    @GuardedBy("lock")
    private volatile boolean running;

    public DefaultJobGraphStore(
            StateHandleStore<JobGraph, R> stateHandleStore,
            JobGraphStoreWatcher jobGraphStoreWatcher,
            JobGraphStoreUtil jobGraphStoreUtil) {
        this.jobGraphStateHandleStore = checkNotNull(stateHandleStore);
        this.jobGraphStoreWatcher = checkNotNull(jobGraphStoreWatcher);
        this.jobGraphStoreUtil = checkNotNull(jobGraphStoreUtil);

        this.running = false;
    }

    @Override
    public void start(JobGraphListener jobGraphListener) throws Exception {
        synchronized (lock) {
            if (!running) {
                this.jobGraphListener = checkNotNull(jobGraphListener);
                jobGraphStoreWatcher.start(this);
                running = true;
            }
        }
    }

    @Override
    public void stop() throws Exception {
        synchronized (lock) {
            if (running) {
                running = false;
                LOG.info("Stopping DefaultJobGraphStore.");
                Exception exception = null;

                try {
                    jobGraphStateHandleStore.releaseAll();
                } catch (Exception e) {
                    exception = e;
                }

                try {
                    jobGraphStoreWatcher.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }

                if (exception != null) {
                    throw new FlinkException(
                            "Could not properly stop the DefaultJobGraphStore.", exception);
                }
            }
        }
    }

    @Nullable
    @Override
    public JobGraph recoverJobGraph(JobID jobId) throws Exception {
        checkNotNull(jobId, "Job ID");

        LOG.debug("Recovering job graph {} from {}.", jobId, jobGraphStateHandleStore);

        final String name = jobGraphStoreUtil.jobIDToName(jobId);

        synchronized (lock) {
            verifyIsRunning();

            boolean success = false;

            RetrievableStateHandle<JobGraph> jobGraphRetrievableStateHandle;

            try {
                try {
                    jobGraphRetrievableStateHandle = jobGraphStateHandleStore.getAndLock(name);
                } catch (StateHandleStore.NotExistException ignored) {
                    success = true;
                    return null;
                } catch (Exception e) {
                    throw new FlinkException(
                            "Could not retrieve the submitted job graph state handle "
                                    + "for "
                                    + name
                                    + " from the submitted job graph store.",
                            e);
                }

                JobGraph jobGraph;
                try {
                    jobGraph = jobGraphRetrievableStateHandle.retrieveState();
                } catch (ClassNotFoundException cnfe) {
                    throw new FlinkException(
                            "Could not retrieve submitted JobGraph from state handle under "
                                    + name
                                    + ". This indicates that you are trying to recover from state written by an "
                                    + "older Flink version which is not compatible. Try cleaning the state handle store.",
                            cnfe);
                } catch (IOException ioe) {
                    throw new FlinkException(
                            "Could not retrieve submitted JobGraph from state handle under "
                                    + name
                                    + ". This indicates that the retrieved state handle is broken. Try cleaning the state handle "
                                    + "store.",
                            ioe);
                }

                addedJobGraphs.add(jobGraph.getJobID());

                LOG.info("Recovered {}.", jobGraph);

                success = true;
                return jobGraph;
            } finally {
                if (!success) {
                    jobGraphStateHandleStore.release(name);
                }
            }
        }
    }

    @Override
    public void putJobGraph(JobGraph jobGraph) throws Exception {
        checkNotNull(jobGraph, "Job graph");

        final JobID jobID = jobGraph.getJobID();
        final String name = jobGraphStoreUtil.jobIDToName(jobID);

        LOG.debug("Adding job graph {} to {}.", jobID, jobGraphStateHandleStore);

        boolean success = false;

        while (!success) {
            synchronized (lock) {
                verifyIsRunning();

                final R currentVersion = jobGraphStateHandleStore.exists(name);

                if (!currentVersion.isExisting()) {
                    try {
                        jobGraphStateHandleStore.addAndLock(name, jobGraph);

                        addedJobGraphs.add(jobID);

                        success = true;
                    } catch (StateHandleStore.AlreadyExistException ignored) {
                        LOG.warn("{} already exists in {}.", jobGraph, jobGraphStateHandleStore);
                    }
                } else if (addedJobGraphs.contains(jobID)) {
                    try {
                        jobGraphStateHandleStore.replace(name, currentVersion, jobGraph);
                        LOG.info("Updated {} in {}.", jobGraph, getClass().getSimpleName());

                        success = true;
                    } catch (StateHandleStore.NotExistException ignored) {
                        LOG.warn("{} does not exists in {}.", jobGraph, jobGraphStateHandleStore);
                    }
                } else {
                    throw new IllegalStateException(
                            "Trying to update a graph you didn't "
                                    + "#getAllSubmittedJobGraphs() or #putJobGraph() yourself before.");
                }
            }
        }

        LOG.info("Added {} to {}.", jobGraph, jobGraphStateHandleStore);
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) throws Exception {
        synchronized (lock) {
            @Nullable final JobGraph jobGraph = recoverJobGraph(jobId);
            if (jobGraph == null) {
                throw new NoSuchElementException(
                        String.format(
                                "JobGraph for job [%s] was not found in JobGraphStore and is needed for attaching JobResourceRequirements.",
                                jobId));
            }
            JobResourceRequirements.writeToJobGraph(jobGraph, jobResourceRequirements);
            putJobGraph(jobGraph);
        }
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        checkNotNull(jobId, "Job ID");

        return runAsyncWithLockAssertRunning(
                () -> {
                    LOG.debug("Removing job graph {} from {}.", jobId, jobGraphStateHandleStore);

                    final String name = jobGraphStoreUtil.jobIDToName(jobId);
                    releaseAndRemoveOrThrowCompletionException(jobId, name);

                    addedJobGraphs.remove(jobId);

                    LOG.info("Removed job graph {} from {}.", jobId, jobGraphStateHandleStore);
                },
                executor);
    }

    @GuardedBy("lock")
    private void releaseAndRemoveOrThrowCompletionException(JobID jobId, String jobName) {
        boolean success;
        try {
            success = jobGraphStateHandleStore.releaseAndTryRemove(jobName);
        } catch (Exception e) {
            throw new CompletionException(e);
        }

        if (!success) {
            throw new CompletionException(
                    new FlinkException(
                            String.format(
                                    "Could not remove job graph with job id %s from %s.",
                                    jobId, jobGraphStateHandleStore)));
        }
    }

    /**
     * Releases the locks on the specified {@link JobGraph}.
     *
     * <p>Releasing the locks allows that another instance can delete the job from the {@link
     * JobGraphStore}.
     *
     * @param jobId specifying the job to release the locks for
     * @param executor the executor being used for the asynchronous execution of the local cleanup.
     * @returns The cleanup result future.
     */
    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        checkNotNull(jobId, "Job ID");

        return runAsyncWithLockAssertRunning(
                () -> {
                    LOG.debug("Releasing job graph {} from {}.", jobId, jobGraphStateHandleStore);

                    jobGraphStateHandleStore.release(jobGraphStoreUtil.jobIDToName(jobId));
                    addedJobGraphs.remove(jobId);

                    LOG.info("Released job graph {} from {}.", jobId, jobGraphStateHandleStore);
                },
                executor);
    }

    private CompletableFuture<Void> runAsyncWithLockAssertRunning(
            ThrowingRunnable<Exception> runnable, Executor executor) {
        return CompletableFuture.runAsync(
                () -> {
                    synchronized (lock) {
                        verifyIsRunning();
                        try {
                            runnable.run();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }
                },
                executor);
    }

    @Override
    public Collection<JobID> getJobIds() throws Exception {
        LOG.debug("Retrieving all stored job ids from {}.", jobGraphStateHandleStore);

        final Collection<String> names;
        try {
            names = jobGraphStateHandleStore.getAllHandles();
        } catch (Exception e) {
            throw new Exception(
                    "Failed to retrieve all job ids from " + jobGraphStateHandleStore + ".", e);
        }

        final List<JobID> jobIds = new ArrayList<>(names.size());

        for (String name : names) {
            try {
                jobIds.add(jobGraphStoreUtil.nameToJobID(name));
            } catch (Exception exception) {
                LOG.warn(
                        "Could not parse job id from {}. This indicates a malformed name.",
                        name,
                        exception);
            }
        }

        LOG.info("Retrieved job ids {} from {}", jobIds, jobGraphStateHandleStore);

        return jobIds;
    }

    @Override
    public void onAddedJobGraph(JobID jobId) {
        synchronized (lock) {
            if (running) {
                if (!addedJobGraphs.contains(jobId)) {
                    try {
                        // This has been added by someone else. Or we were fast to remove it (false
                        // positive).
                        jobGraphListener.onAddedJobGraph(jobId);
                    } catch (Throwable t) {
                        LOG.error(
                                "Failed to notify job graph listener onAddedJobGraph event for {}",
                                jobId,
                                t);
                    }
                }
            }
        }
    }

    @Override
    public void onRemovedJobGraph(JobID jobId) {
        synchronized (lock) {
            if (running) {
                if (addedJobGraphs.contains(jobId)) {
                    try {
                        // Someone else removed one of our job graphs. Mean!
                        jobGraphListener.onRemovedJobGraph(jobId);
                    } catch (Throwable t) {
                        LOG.error(
                                "Failed to notify job graph listener onRemovedJobGraph event for {}",
                                jobId,
                                t);
                    }
                }
            }
        }
    }

    /** Verifies that the state is running. */
    private void verifyIsRunning() {
        checkState(running, "Not running. Forgot to call start()?");
    }
}
