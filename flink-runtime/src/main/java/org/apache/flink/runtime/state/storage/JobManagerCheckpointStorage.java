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

package org.apache.flink.runtime.state.storage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableCheckpointStorage;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link CheckpointStorage} checkpoints state directly to the JobManager's memory (hence the
 * name), but savepoints will be persisted to a file system.
 *
 * <p>This checkpoint storage is primarily for experimentation, quick local setups, or for streaming
 * applications that have very small state: Because it requires checkpoints to go through the
 * JobManager's memory, larger state will occupy larger portions of the JobManager's main memory,
 * reducing operational stability. For any other setup, the {@link FileSystemCheckpointStorage}
 * should be used. The {@code FileSystemCheckpointStorage} but checkpoints state directly to files
 * rather than to the JobManager's memory, thus supporting larger state sizes and more highly
 * available recovery.
 *
 * <h1>State Size Considerations</h1>
 *
 * <p>State checkpointing with this storage is subject to the following conditions:
 *
 * <ul>
 *   <li>Each individual state must not exceed the configured maximum state size (see {@link
 *       #getMaxStateSize()}.
 *   <li>All state from one task (i.e., the sum of all operator states and keyed states from all
 *       chained operators of the task) must not exceed what the RPC system supports, which is be
 *       default < 10 MB. That limit can be configured up, but that is typically not advised.
 *   <li>The sum of all states in the application times all retained checkpoints must comfortably
 *       fit into the JobManager's JVM heap space.
 * </ul>
 *
 * <h1>Persistence Guarantees</h1>
 *
 * <p>For the use cases where the state sizes can be handled by this checkpoint storage, the storage
 * does guarantee persistence for savepoints, externalized checkpoints (if configured), and
 * checkpoints (when high-availability is configured).
 *
 * <h1>Configuration</h1>
 *
 * <p>As for all checkpoint storage, this storage policy can either be configured within the
 * application (by creating the storage with the respective constructor parameters and setting it on
 * the execution environment) or by specifying it in the Flink configuration.
 *
 * <p>If the checkpoint storage was specified in the application, it may pick up additional
 * configuration parameters from the Flink configuration. For example, if the storage if configured
 * in the application without a default savepoint directory, it will pick up a default savepoint
 * directory specified in the Flink configuration of the running job/cluster. That behavior is
 * implemented via the {@link #configure(ReadableConfig, ClassLoader)} method.
 */
@PublicEvolving
public class JobManagerCheckpointStorage
        implements CheckpointStorage, ConfigurableCheckpointStorage {

    private static final long serialVersionUID = 4109305377809414635L;

    /** The default maximal size that the snapshotted memory state may have (5 MiBytes). */
    public static final int DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024;

    /** The maximal size that the snapshotted memory state may have. */
    private final int maxStateSize;

    /** The optional locations where snapshots will be externalized. */
    private final ExternalizedSnapshotLocation location;

    // ------------------------------------------------------------------------

    /**
     * Creates a new job manager checkpoint storage that accepts states whose serialized forms are
     * up to the default state size (5 MB).
     *
     * <p>Checkpoint and default savepoint locations are used as specified in the runtime
     * configuration.
     */
    public JobManagerCheckpointStorage() {
        this(null, DEFAULT_MAX_STATE_SIZE);
    }

    /**
     * Creates a new JobManagerCheckpointStorage, setting optionally the paths to persist checkpoint
     * metadata and savepoints to, as well as configuring state thresholds and asynchronous
     * operations.
     *
     * <p><b>WARNING:</b> Increasing the size of this value beyond the default value ({@value
     * #DEFAULT_MAX_STATE_SIZE}) should be done with care. The checkpointed state needs to be send
     * to the JobManager via limited size RPC messages, and there and the JobManager needs to be
     * able to hold all aggregated state in its memory.
     *
     * @param maxStateSize The maximal size of the serialized state.
     */
    public JobManagerCheckpointStorage(int maxStateSize) {
        this(null, maxStateSize);
    }

    /**
     * Creates a new JobManagerCheckpointStorage, setting optionally the paths to persist checkpoint
     * metadata and savepoints to, as well as configuring state thresholds and asynchronous
     * operations.
     *
     * <p><b>WARNING:</b> Increasing the size of this value beyond the default value ({@value
     * #DEFAULT_MAX_STATE_SIZE}) should be done with care. The checkpointed state needs to be send
     * to the JobManager via limited size RPC messages, and there and the JobManager needs to be
     * able to hold all aggregated state in its memory.
     *
     * @param checkpointPath path to write checkpoint metadata to. If null, the value from the
     *     runtime configuration will be used.
     */
    public JobManagerCheckpointStorage(String checkpointPath) {
        this(new Path(checkpointPath), DEFAULT_MAX_STATE_SIZE);
    }

    /**
     * Creates a new JobManagerCheckpointStorage, setting optionally the paths to persist checkpoint
     * metadata and savepoints to, as well as configuring state thresholds and asynchronous
     * operations.
     *
     * <p><b>WARNING:</b> Increasing the size of this value beyond the default value ({@value
     * #DEFAULT_MAX_STATE_SIZE}) should be done with care. The checkpointed state needs to be send
     * to the JobManager via limited size RPC messages, and there and the JobManager needs to be
     * able to hold all aggregated state in its memory.
     *
     * @param maxStateSize The maximal size of the serialized state.
     */
    public JobManagerCheckpointStorage(Path checkpointPath, int maxStateSize) {
        checkArgument(maxStateSize > 0, "maxStateSize must be > 0");
        this.maxStateSize = maxStateSize;
        this.location =
                ExternalizedSnapshotLocation.newBuilder()
                        .withCheckpointPath(checkpointPath)
                        .build();
    }

    /**
     * Private constructor that creates a re-configured copy of the checkpoint storage.
     *
     * @param original The checkpoint storage to re-configure.
     */
    private JobManagerCheckpointStorage(
            JobManagerCheckpointStorage original, ReadableConfig config) {
        this.maxStateSize = original.maxStateSize;
        this.location =
                ExternalizedSnapshotLocation.newBuilder()
                        .withCheckpointPath(original.location.getBaseCheckpointPath())
                        .withSavepointPath(original.location.getBaseSavepointPath())
                        .withConfiguration(config)
                        .build();
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    /**
     * Gets the maximum size that an individual state can have, as configured in the constructor (by
     * default {@value #DEFAULT_MAX_STATE_SIZE}).
     *
     * @return The maximum size that an individual state can have
     */
    public int getMaxStateSize() {
        return maxStateSize;
    }

    /** @return The location where checkpoints will be externalized if set. */
    @Nullable
    public Path getCheckpointPath() {
        return location.getBaseCheckpointPath();
    }

    /** @return The default location where savepoints will be externalized if set. */
    @Nullable
    public Path getSavepointPath() {
        return location.getBaseSavepointPath();
    }

    // ------------------------------------------------------------------------
    //  Reconfiguration
    // ------------------------------------------------------------------------

    /**
     * Creates a copy of this checkpoint storage that uses the values defined in the configuration
     * for fields where that were not specified in this checkpoint storage.
     *
     * @param config The configuration
     * @return The re-configured variant of the checkpoint storage
     */
    @Override
    public JobManagerCheckpointStorage configure(ReadableConfig config, ClassLoader classLoader) {
        return new JobManagerCheckpointStorage(this, config);
    }

    /**
     * Creates a new {@link JobManagerCheckpointStorage} using the given configuration.
     *
     * @param config The Flink configuration (loaded by the TaskManager).
     * @param classLoader The clsas loader that should be used to load the checkpoint storage.
     * @return The created checkpoint storage.
     * @throws IllegalConfigurationException If the configuration misses critical values, or
     *     specifies invalid values
     */
    public static JobManagerCheckpointStorage createFromConfig(
            ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
        try {
            return new JobManagerCheckpointStorage().configure(config, classLoader);
        } catch (IllegalArgumentException e) {
            throw new IllegalConfigurationException(
                    "Invalid configuration for the state backend", e);
        }
    }

    // ------------------------------------------------------------------------
    //  checkpoint state persistence
    // ------------------------------------------------------------------------

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
        return AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(pointer);
    }

    @Override
    public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
        return new MemoryBackendCheckpointStorageAccess(
                jobId,
                location.getBaseCheckpointPath(),
                location.getBaseSavepointPath(),
                maxStateSize);
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "JobManagerCheckpointStorage (checkpoints to JobManager) "
                + "( maxStateSize: "
                + maxStateSize
                + ")";
    }
}
