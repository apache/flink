/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** This class holds the all {@link StateChangelogStorage} objects for a task executor (manager). */
@ThreadSafe
public class TaskExecutorStateChangelogStoragesManager {

    /** Logger for this class. */
    private static final Logger LOG =
            LoggerFactory.getLogger(TaskExecutorStateChangelogStoragesManager.class);

    /**
     * This map holds all state changelog storages for tasks running on the task manager / executor
     * that own the instance of this. Maps from job id to all the subtask's state changelog
     * storages. Value type Optional is for containing the null value.
     */
    @GuardedBy("lock")
    private final Map<JobID, Optional<StateChangelogStorage<?>>> changelogStoragesByJobId;

    /**
     * This map holds all state changelog storage views of {@link ChangelogStateHandleStreamImpl}
     * for tasks running on the task manager / executor that own the instance of this. Value type
     * Optional is for containing the null value.
     */
    @GuardedBy("lock")
    private final Map<JobID, StateChangelogStorageView<ChangelogStateHandleStreamImpl>>
            changelogStorageViewsByJobId;

    @GuardedBy("lock")
    private boolean closed;

    private final Object lock = new Object();

    /** shutdown hook for this manager. */
    private final Thread shutdownHook;

    public TaskExecutorStateChangelogStoragesManager() {
        this.changelogStoragesByJobId = new HashMap<>();
        this.changelogStorageViewsByJobId = new HashMap<>();
        this.closed = false;

        // register a shutdown hook
        this.shutdownHook =
                ShutdownHookUtil.addShutdownHook(this::shutdown, getClass().getSimpleName(), LOG);
    }

    @Nullable
    public StateChangelogStorage<?> stateChangelogStorageForJob(
            @Nonnull JobID jobId,
            Configuration configuration,
            TaskManagerJobMetricGroup metricGroup,
            LocalRecoveryConfig localRecoveryConfig)
            throws IOException {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException(
                        "TaskExecutorStateChangelogStoragesManager is already closed and cannot "
                                + "register a new StateChangelogStorage.");
            }

            Optional<StateChangelogStorage<?>> stateChangelogStorage =
                    changelogStoragesByJobId.get(jobId);

            if (stateChangelogStorage == null) {
                StateChangelogStorage<?> loaded =
                        StateChangelogStorageLoader.load(
                                jobId, configuration, metricGroup, localRecoveryConfig);
                stateChangelogStorage = Optional.ofNullable(loaded);
                changelogStoragesByJobId.put(jobId, stateChangelogStorage);

                if (loaded != null) {
                    LOG.debug(
                            "Registered new state changelog storage for job {} : {}.",
                            jobId,
                            loaded);
                } else {
                    LOG.info(
                            "Try to registered new state changelog storage for job {},"
                                    + " but result is null.",
                            jobId);
                }
            } else if (stateChangelogStorage.isPresent()) {
                LOG.debug(
                        "Found existing state changelog storage for job {}: {}.",
                        jobId,
                        stateChangelogStorage.get());
            } else {
                LOG.debug(
                        "Found a previously loaded NULL state changelog storage for job {}.",
                        jobId);
            }

            return stateChangelogStorage.orElse(null);
        }
    }

    private void releaseStateChangelogStorageForJob(@Nonnull JobID jobId) {
        LOG.debug("Releasing state changelog storage under job id {}.", jobId);
        Optional<StateChangelogStorage<?>> cleanupChangelogStorage;
        synchronized (lock) {
            if (closed) {
                return;
            }
            cleanupChangelogStorage = changelogStoragesByJobId.remove(jobId);
        }

        if (cleanupChangelogStorage != null) {
            cleanupChangelogStorage.ifPresent(this::doRelease);
        }
    }

    @Nullable
    StateChangelogStorageView<?> stateChangelogStorageViewForJob(
            @Nonnull JobID jobID,
            Configuration configuration,
            ChangelogStateHandle changelogStateHandle)
            throws IOException {
        if (closed) {
            throw new IllegalStateException(
                    "TaskExecutorStateChangelogStoragesManager is already closed and cannot "
                            + "register a new StateChangelogStorageView.");
        }

        // This implementation assume there is only one production implementation of DSTL
        // (FsStateChangelogStorage). Maybe we should change the type of
        // changelogStorageViewsByJobId to map<jobId, map<dstl-identifier, dstl>> when there is
        // another implementation.

        synchronized (lock) {
            StateChangelogStorageView<ChangelogStateHandleStreamImpl> storageView =
                    changelogStorageViewsByJobId.get(jobID);

            if (storageView == null) {
                StateChangelogStorageView<?> loaded =
                        StateChangelogStorageLoader.loadFromStateHandle(
                                configuration, changelogStateHandle);
                storageView = (StateChangelogStorageView<ChangelogStateHandleStreamImpl>) loaded;
                changelogStorageViewsByJobId.put(jobID, storageView);

                LOG.debug(
                        "Registered new state changelog storage view for job {} : {}.",
                        jobID,
                        loaded);
            } else {
                LOG.debug(
                        "Found existing state changelog storage view for job {}: {}.",
                        jobID,
                        storageView);
            }

            return storageView;
        }
    }

    private void releaseStateChangelogStorageViewForJob(@Nonnull JobID jobID) {
        LOG.debug("Releasing state changelog storage view under job id {}.", jobID);
        StateChangelogStorageView<ChangelogStateHandleStreamImpl> cleanupStorageView;
        synchronized (lock) {
            if (closed) {
                return;
            }
            cleanupStorageView = changelogStorageViewsByJobId.remove(jobID);
        }

        if (cleanupStorageView != null) {
            doRelease(cleanupStorageView);
        }
    }

    public void releaseResourcesForJob(@Nonnull JobID jobID) {
        releaseStateChangelogStorageForJob(jobID);
        releaseStateChangelogStorageViewForJob(jobID);
    }

    public void shutdown() {
        HashMap<JobID, Optional<StateChangelogStorage<?>>> toReleaseStorage;
        HashMap<JobID, StateChangelogStorageView<ChangelogStateHandleStreamImpl>>
                toReleaseStorageView;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;

            toReleaseStorage = new HashMap<>(changelogStoragesByJobId);
            toReleaseStorageView = new HashMap<>(changelogStorageViewsByJobId);
            changelogStoragesByJobId.clear();
            changelogStorageViewsByJobId.clear();
        }

        ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

        LOG.info("Shutting down TaskExecutorStateChangelogStoragesManager.");

        for (Map.Entry<JobID, Optional<StateChangelogStorage<?>>> entry :
                toReleaseStorage.entrySet()) {
            entry.getValue().ifPresent(this::doRelease);
        }
        for (Map.Entry<JobID, StateChangelogStorageView<ChangelogStateHandleStreamImpl>> entry :
                toReleaseStorageView.entrySet()) {
            doRelease(entry.getValue());
        }
    }

    private void doRelease(StateChangelogStorageView<?> storage) {
        if (storage != null) {
            try {
                storage.close();
            } catch (Exception e) {
                LOG.warn("Exception while disposing state changelog storage {}.", storage, e);
            }
        }
    }

    @VisibleForTesting
    @Nullable
    public Optional<StateChangelogStorage<?>> getChangelogStoragesByJobId(JobID jobId) {
        synchronized (lock) {
            return changelogStoragesByJobId.get(jobId);
        }
    }
}
