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
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
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
 * Default implementation for {@link ExecutionPlanStore}. Combined with different {@link
 * StateHandleStore}, we could persist the execution plans to various distributed storage. Also
 * combined with different {@link ExecutionPlanStoreWatcher}, we could get all the changes on the
 * execution plan store and do the response.
 */
public class DefaultExecutionPlanStore<R extends ResourceVersion<R>>
        implements ExecutionPlanStore, ExecutionPlanStore.ExecutionPlanListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionPlanStore.class);

    /** Lock to synchronize with the {@link ExecutionPlanListener}. */
    private final Object lock = new Object();

    /** The set of IDs of all added execution plans. */
    @GuardedBy("lock")
    private final Set<JobID> addedExecutionPlans = new HashSet<>();

    /** Submitted execution plans handle store. */
    private final StateHandleStore<ExecutionPlan, R> executionPlanStateHandleStore;

    @GuardedBy("lock")
    private final ExecutionPlanStoreWatcher executionPlanStoreWatcher;

    private final ExecutionPlanStoreUtil executionPlanStoreUtil;

    /** The external listener to be notified on races. */
    @GuardedBy("lock")
    private ExecutionPlanListener executionPlanListener;

    /** Flag indicating whether this instance is running. */
    @GuardedBy("lock")
    private volatile boolean running;

    public DefaultExecutionPlanStore(
            StateHandleStore<ExecutionPlan, R> stateHandleStore,
            ExecutionPlanStoreWatcher executionPlanStoreWatcher,
            ExecutionPlanStoreUtil executionPlanStoreUtil) {
        this.executionPlanStateHandleStore = checkNotNull(stateHandleStore);
        this.executionPlanStoreWatcher = checkNotNull(executionPlanStoreWatcher);
        this.executionPlanStoreUtil = checkNotNull(executionPlanStoreUtil);

        this.running = false;
    }

    @Override
    public void start(ExecutionPlanListener executionPlanListener) throws Exception {
        synchronized (lock) {
            if (!running) {
                this.executionPlanListener = checkNotNull(executionPlanListener);
                executionPlanStoreWatcher.start(this);
                running = true;
            }
        }
    }

    @Override
    public void stop() throws Exception {
        synchronized (lock) {
            if (running) {
                running = false;
                LOG.info("Stopping DefaultExecutionPlanStore.");
                Exception exception = null;

                try {
                    executionPlanStateHandleStore.releaseAll();
                } catch (Exception e) {
                    exception = e;
                }

                try {
                    executionPlanStoreWatcher.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }

                if (exception != null) {
                    throw new FlinkException(
                            "Could not properly stop the DefaultExecutionPlanStore.", exception);
                }
            }
        }
    }

    @Nullable
    @Override
    public ExecutionPlan recoverExecutionPlan(JobID jobId) throws Exception {
        checkNotNull(jobId, "Job ID");

        LOG.debug("Recovering execution plan {} from {}.", jobId, executionPlanStateHandleStore);

        final String name = executionPlanStoreUtil.jobIDToName(jobId);

        synchronized (lock) {
            verifyIsRunning();

            boolean success = false;

            RetrievableStateHandle<ExecutionPlan> executionPlanRetrievableStateHandle;

            try {
                try {
                    executionPlanRetrievableStateHandle =
                            executionPlanStateHandleStore.getAndLock(name);
                } catch (StateHandleStore.NotExistException ignored) {
                    success = true;
                    return null;
                } catch (Exception e) {
                    throw new FlinkException(
                            "Could not retrieve the submitted execution plan state handle "
                                    + "for "
                                    + name
                                    + " from the submitted execution plan store.",
                            e);
                }

                ExecutionPlan executionPlan;
                try {
                    executionPlan = executionPlanRetrievableStateHandle.retrieveState();
                } catch (ClassNotFoundException cnfe) {
                    throw new FlinkException(
                            "Could not retrieve submitted ExecutionPlan from state handle under "
                                    + name
                                    + ". This indicates that you are trying to recover from state written by an "
                                    + "older Flink version which is not compatible. Try cleaning the state handle store.",
                            cnfe);
                } catch (IOException ioe) {
                    throw new FlinkException(
                            "Could not retrieve submitted ExecutionPlan from state handle under "
                                    + name
                                    + ". This indicates that the retrieved state handle is broken. Try cleaning the state handle "
                                    + "store.",
                            ioe);
                }

                addedExecutionPlans.add(executionPlan.getJobID());

                LOG.info("Recovered {}.", executionPlan);

                success = true;
                return executionPlan;
            } finally {
                if (!success) {
                    executionPlanStateHandleStore.release(name);
                }
            }
        }
    }

    @Override
    public void putExecutionPlan(ExecutionPlan executionPlan) throws Exception {
        checkNotNull(executionPlan, "Execution Plan");

        final JobID jobID = executionPlan.getJobID();
        final String name = executionPlanStoreUtil.jobIDToName(jobID);

        LOG.debug("Adding execution plan {} to {}.", jobID, executionPlanStateHandleStore);

        boolean success = false;

        while (!success) {
            synchronized (lock) {
                verifyIsRunning();

                final R currentVersion = executionPlanStateHandleStore.exists(name);

                if (!currentVersion.isExisting()) {
                    try {
                        executionPlanStateHandleStore.addAndLock(name, executionPlan);

                        addedExecutionPlans.add(jobID);

                        success = true;
                    } catch (StateHandleStore.AlreadyExistException ignored) {
                        LOG.warn(
                                "{} already exists in {}.",
                                executionPlan,
                                executionPlanStateHandleStore);
                    }
                } else if (addedExecutionPlans.contains(jobID)) {
                    try {
                        executionPlanStateHandleStore.replace(name, currentVersion, executionPlan);
                        LOG.info("Updated {} in {}.", executionPlan, getClass().getSimpleName());

                        success = true;
                    } catch (StateHandleStore.NotExistException ignored) {
                        LOG.warn(
                                "{} does not exists in {}.",
                                executionPlan,
                                executionPlanStateHandleStore);
                    }
                } else {
                    throw new IllegalStateException(
                            "Trying to update an execution plan you didn't "
                                    + "#getAllSubmittedExecutionPlans() or #putExecutionPlan() yourself before.");
                }
            }
        }

        LOG.info("Added {} to {}.", executionPlan, executionPlanStateHandleStore);
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) throws Exception {
        synchronized (lock) {
            @Nullable final ExecutionPlan executionPlan = recoverExecutionPlan(jobId);
            if (executionPlan == null) {
                throw new NoSuchElementException(
                        String.format(
                                "ExecutionPlan for job [%s] was not found in ExecutionPlanStore and is needed for attaching JobResourceRequirements.",
                                jobId));
            }

            JobResourceRequirements.writeToExecutionPlan(executionPlan, jobResourceRequirements);
            putExecutionPlan(executionPlan);
        }
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        checkNotNull(jobId, "Job ID");

        return runAsyncWithLockAssertRunning(
                () -> {
                    LOG.debug(
                            "Removing execution plan {} from {}.",
                            jobId,
                            executionPlanStateHandleStore);

                    final String name = executionPlanStoreUtil.jobIDToName(jobId);
                    releaseAndRemoveOrThrowCompletionException(jobId, name);

                    addedExecutionPlans.remove(jobId);

                    LOG.info(
                            "Removed execution plan {} from {}.",
                            jobId,
                            executionPlanStateHandleStore);
                },
                executor);
    }

    @GuardedBy("lock")
    private void releaseAndRemoveOrThrowCompletionException(JobID jobId, String jobName) {
        boolean success;
        try {
            success = executionPlanStateHandleStore.releaseAndTryRemove(jobName);
        } catch (Exception e) {
            throw new CompletionException(e);
        }

        if (!success) {
            throw new CompletionException(
                    new FlinkException(
                            String.format(
                                    "Could not remove execution plan with job id %s from %s.",
                                    jobId, executionPlanStateHandleStore)));
        }
    }

    /**
     * Releases the locks on the specified {@link ExecutionPlan}.
     *
     * <p>Releasing the locks allows that another instance can delete the job from the {@link
     * ExecutionPlanStore}.
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
                    LOG.debug(
                            "Releasing execution plan {} from {}.",
                            jobId,
                            executionPlanStateHandleStore);

                    executionPlanStateHandleStore.release(
                            executionPlanStoreUtil.jobIDToName(jobId));
                    addedExecutionPlans.remove(jobId);

                    LOG.info(
                            "Released execution plan {} from {}.",
                            jobId,
                            executionPlanStateHandleStore);
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
        LOG.debug("Retrieving all stored job ids from {}.", executionPlanStateHandleStore);

        final Collection<String> names;
        try {
            names = executionPlanStateHandleStore.getAllHandles();
        } catch (Exception e) {
            throw new Exception(
                    "Failed to retrieve all job ids from " + executionPlanStateHandleStore + ".",
                    e);
        }

        final List<JobID> jobIds = new ArrayList<>(names.size());

        for (String name : names) {
            try {
                jobIds.add(executionPlanStoreUtil.nameToJobID(name));
            } catch (Exception exception) {
                LOG.warn(
                        "Could not parse job id from {}. This indicates a malformed name.",
                        name,
                        exception);
            }
        }

        LOG.info("Retrieved job ids {} from {}", jobIds, executionPlanStateHandleStore);

        return jobIds;
    }

    @Override
    public void onAddedExecutionPlan(JobID jobId) {
        synchronized (lock) {
            if (running) {
                if (!addedExecutionPlans.contains(jobId)) {
                    try {
                        // This has been added by someone else. Or we were fast to remove it (false
                        // positive).
                        executionPlanListener.onAddedExecutionPlan(jobId);
                    } catch (Throwable t) {
                        LOG.error(
                                "Failed to notify execution plan listener onAddedExecutionPlan event for {}",
                                jobId,
                                t);
                    }
                }
            }
        }
    }

    @Override
    public void onRemovedExecutionPlan(JobID jobId) {
        synchronized (lock) {
            if (running) {
                if (addedExecutionPlans.contains(jobId)) {
                    try {
                        // Someone else removed one of our execution plans. Mean!
                        executionPlanListener.onRemovedExecutionPlan(jobId);
                    } catch (Throwable t) {
                        LOG.error(
                                "Failed to notify execution plan listener onRemovedExecutionPlan event for {}",
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
