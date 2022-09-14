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
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.configuration.StateChangelogOptionsInternal;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Reference;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * This class holds the all {@link TaskLocalStateStoreImpl} objects for a task executor (manager).
 */
public class TaskExecutorLocalStateStoresManager {

    /** Logger for this class. */
    private static final Logger LOG =
            LoggerFactory.getLogger(TaskExecutorLocalStateStoresManager.class);

    public static final String ALLOCATION_DIR_PREFIX = "aid_";

    /**
     * This map holds all local state stores for tasks running on the task manager / executor that
     * own the instance of this. Maps from allocation id to all the subtask's local state stores.
     */
    @GuardedBy("lock")
    private final Map<AllocationID, Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore>>
            taskStateStoresByAllocationID;

    /** The configured mode for local recovery on this task manager. */
    private final boolean localRecoveryEnabled;

    /** This is the root directory for all local state of this task manager / executor. */
    private final Reference<File[]> localStateRootDirectories;

    /** Executor that runs the discarding of released state objects. */
    private final Executor discardExecutor;

    /** Guarding lock for taskStateStoresByAllocationID and closed-flag. */
    private final Object lock;

    private final Thread shutdownHook;

    @GuardedBy("lock")
    private boolean closed;

    public TaskExecutorLocalStateStoresManager(
            boolean localRecoveryEnabled,
            @Nonnull Reference<File[]> localStateRootDirectories,
            @Nonnull Executor discardExecutor)
            throws IOException {

        LOG.debug(
                "Start {} with local state root directories {}.",
                getClass().getSimpleName(),
                localStateRootDirectories);

        this.taskStateStoresByAllocationID = new HashMap<>();
        this.localRecoveryEnabled = localRecoveryEnabled;
        this.localStateRootDirectories = localStateRootDirectories;
        this.discardExecutor = discardExecutor;
        this.lock = new Object();
        this.closed = false;

        for (File localStateRecoveryRootDir : localStateRootDirectories.deref()) {

            if (!localStateRecoveryRootDir.exists()
                    && !localStateRecoveryRootDir.mkdirs()
                    // we double check for exists in case another task created the directory
                    // concurrently.
                    && !localStateRecoveryRootDir.exists()) {
                throw new IOException(
                        "Could not create root directory for local recovery: "
                                + localStateRecoveryRootDir);
            }
        }

        // register a shutdown hook
        this.shutdownHook =
                ShutdownHookUtil.addShutdownHook(this::shutdown, getClass().getSimpleName(), LOG);
    }

    @Nonnull
    public TaskLocalStateStore localStateStoreForSubtask(
            @Nonnull JobID jobId,
            @Nonnull AllocationID allocationID,
            @Nonnull JobVertexID jobVertexID,
            @Nonnegative int subtaskIndex,
            Configuration clusterConfiguration,
            Configuration jobConfiguration) {

        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException(
                        "TaskExecutorLocalStateStoresManager is already closed and cannot "
                                + "register a new TaskLocalStateStore.");
            }

            Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore> taskStateManagers =
                    this.taskStateStoresByAllocationID.get(allocationID);

            if (taskStateManagers == null) {
                taskStateManagers = new HashMap<>();
                this.taskStateStoresByAllocationID.put(allocationID, taskStateManagers);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Registered new allocation id {} for local state stores for job {}.",
                            allocationID.toHexString(),
                            jobId);
                }
            }

            final JobVertexSubtaskKey taskKey = new JobVertexSubtaskKey(jobVertexID, subtaskIndex);

            OwnedTaskLocalStateStore taskLocalStateStore = taskStateManagers.get(taskKey);

            if (taskLocalStateStore == null) {

                LocalRecoveryDirectoryProviderImpl directoryProvider = null;
                if (localRecoveryEnabled) {
                    // create the allocation base dirs, one inside each root dir.
                    File[] allocationBaseDirectories = allocationBaseDirectories(allocationID);
                    directoryProvider =
                            new LocalRecoveryDirectoryProviderImpl(
                                    allocationBaseDirectories, jobId, jobVertexID, subtaskIndex);
                }

                LocalRecoveryConfig localRecoveryConfig =
                        new LocalRecoveryConfig(directoryProvider);

                boolean changelogEnabled =
                        jobConfiguration
                                .getOptional(
                                        StateChangelogOptionsInternal
                                                .ENABLE_CHANGE_LOG_FOR_APPLICATION)
                                .orElse(
                                        clusterConfiguration.getBoolean(
                                                StateChangelogOptions.ENABLE_STATE_CHANGE_LOG));

                if (localRecoveryConfig.isLocalRecoveryEnabled() && changelogEnabled) {
                    taskLocalStateStore =
                            new ChangelogTaskLocalStateStore(
                                    jobId,
                                    allocationID,
                                    jobVertexID,
                                    subtaskIndex,
                                    localRecoveryConfig,
                                    discardExecutor);
                } else if (localRecoveryConfig.isLocalRecoveryEnabled()) {
                    taskLocalStateStore =
                            new TaskLocalStateStoreImpl(
                                    jobId,
                                    allocationID,
                                    jobVertexID,
                                    subtaskIndex,
                                    localRecoveryConfig,
                                    discardExecutor);
                } else {
                    // NOP implementation if local recovery is disabled
                    taskLocalStateStore = new NoOpTaskLocalStateStoreImpl(localRecoveryConfig);
                }

                taskStateManagers.put(taskKey, taskLocalStateStore);

                LOG.debug(
                        "Registered new local state store with configuration {} for {} - {} - {} under allocation "
                                + "id {}.",
                        localRecoveryConfig,
                        jobId,
                        jobVertexID,
                        subtaskIndex,
                        allocationID);

            } else {
                LOG.debug(
                        "Found existing local state store for {} - {} - {} under allocation id {}: {}",
                        jobId,
                        jobVertexID,
                        subtaskIndex,
                        allocationID,
                        taskLocalStateStore);
            }

            return taskLocalStateStore;
        }
    }

    public void releaseLocalStateForAllocationId(@Nonnull AllocationID allocationID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Releasing local state under allocation id {}.", allocationID);
        }

        Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore> cleanupLocalStores;

        synchronized (lock) {
            if (closed) {
                return;
            }
            cleanupLocalStores = taskStateStoresByAllocationID.remove(allocationID);
        }

        if (cleanupLocalStores != null) {
            doRelease(cleanupLocalStores.values());
        }

        cleanupAllocationBaseDirs(allocationID);
    }

    /**
     * Retains the given set of allocations. All other allocations will be released.
     *
     * @param allocationsToRetain
     */
    public void retainLocalStateForAllocations(Set<AllocationID> allocationsToRetain) {
        final Collection<AllocationID> allocationIds = findStoredAllocations();

        allocationIds.stream()
                .filter(allocationId -> !allocationsToRetain.contains(allocationId))
                .forEach(this::releaseLocalStateForAllocationId);
    }

    private Collection<AllocationID> findStoredAllocations() {
        final Set<AllocationID> storedAllocations = new HashSet<>();
        for (File localStateRootDirectory : localStateRootDirectories.deref()) {
            try {
                final Collection<Path> allocationDirectories =
                        listAllocationDirectoriesIn(localStateRootDirectory);

                for (Path allocationDirectory : allocationDirectories) {
                    final String hexString =
                            allocationDirectory
                                    .getFileName()
                                    .toString()
                                    .substring(ALLOCATION_DIR_PREFIX.length());
                    storedAllocations.add(AllocationID.fromHexString(hexString));
                }
            } catch (IOException ioe) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Could not list local state directory {}. This entails that some orphaned local state might not be cleaned up properly.",
                            localStateRootDirectory,
                            ioe);
                } else {
                    LOG.info(
                            "Could not list local state directory {}. This entails that some orphaned local state might not be cleaned up properly.",
                            localStateRootDirectory);
                }
            }
        }

        return storedAllocations;
    }

    @VisibleForTesting
    @Nonnull
    static Collection<Path> listAllocationDirectoriesIn(File localStateRootDirectory)
            throws IOException {
        return Files.list(localStateRootDirectory.toPath())
                .filter(path -> path.getFileName().toString().startsWith(ALLOCATION_DIR_PREFIX))
                .collect(Collectors.toList());
    }

    public void shutdown() {
        synchronized (lock) {
            if (closed) {
                return;
            }

            closed = true;
            taskStateStoresByAllocationID.clear();
        }

        LOG.info("Shutting down TaskExecutorLocalStateStoresManager.");

        ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

        if (localStateRootDirectories.isOwned()) {
            for (File localStateRootDirectory : localStateRootDirectories.deref()) {
                try {
                    FileUtils.deleteDirectory(localStateRootDirectory);
                } catch (IOException ioe) {
                    LOG.warn(
                            "Could not delete local state directory {}.",
                            localStateRootDirectory,
                            ioe);
                }
            }
        }
    }

    @VisibleForTesting
    boolean isLocalRecoveryEnabled() {
        return localRecoveryEnabled;
    }

    @VisibleForTesting
    File[] getLocalStateRootDirectories() {
        return localStateRootDirectories.deref();
    }

    @VisibleForTesting
    String allocationSubDirString(AllocationID allocationID) {
        return ALLOCATION_DIR_PREFIX + allocationID.toHexString();
    }

    private File[] allocationBaseDirectories(AllocationID allocationID) {
        final String allocationSubDirString = allocationSubDirString(allocationID);
        final File[] derefLocalStateRootDirectories = localStateRootDirectories.deref();
        final File[] allocationDirectories = new File[derefLocalStateRootDirectories.length];
        for (int i = 0; i < derefLocalStateRootDirectories.length; ++i) {
            allocationDirectories[i] =
                    new File(derefLocalStateRootDirectories[i], allocationSubDirString);
        }
        return allocationDirectories;
    }

    private void doRelease(Iterable<OwnedTaskLocalStateStore> toRelease) {

        if (toRelease != null) {

            for (OwnedTaskLocalStateStore stateStore : toRelease) {
                try {
                    stateStore.dispose();
                } catch (Exception disposeEx) {
                    LOG.warn(
                            "Exception while disposing local state store {}.",
                            stateStore,
                            disposeEx);
                }
            }
        }
    }

    /** Deletes the base dirs for this allocation id (recursively). */
    private void cleanupAllocationBaseDirs(AllocationID allocationID) {
        // clear the base dirs for this allocation id.
        File[] allocationDirectories = allocationBaseDirectories(allocationID);
        for (File directory : allocationDirectories) {
            try {
                FileUtils.deleteFileOrDirectory(directory);
            } catch (IOException e) {
                LOG.warn(
                        "Exception while deleting local state directory for allocation id {}.",
                        allocationID,
                        e);
            }
        }
    }

    /**
     * Composite key of {@link JobVertexID} and subtask index that describes the subtask of a job
     * vertex.
     */
    private static final class JobVertexSubtaskKey {

        /** The job vertex id. */
        @Nonnull final JobVertexID jobVertexID;

        /** The subtask index. */
        @Nonnegative final int subtaskIndex;

        JobVertexSubtaskKey(@Nonnull JobVertexID jobVertexID, @Nonnegative int subtaskIndex) {
            this.jobVertexID = jobVertexID;
            this.subtaskIndex = subtaskIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JobVertexSubtaskKey that = (JobVertexSubtaskKey) o;

            return subtaskIndex == that.subtaskIndex && jobVertexID.equals(that.jobVertexID);
        }

        @Override
        public int hashCode() {
            int result = jobVertexID.hashCode();
            result = 31 * result + subtaskIndex;
            return result;
        }
    }
}
