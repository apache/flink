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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;

/**
 * A base class for all checkpoint storage instances that store their metadata (and data) in files.
 *
 * <p>This class takes the base checkpoint- and savepoint directory paths, but also accepts null for
 * both of then, in which case creating externalized checkpoint is not possible, and it is not
 * possible to create a savepoint with a default path. Null is accepted to enable implementations
 * that only optionally support default savepoints.
 *
 * <h1>Checkpoint Layout</h1>
 *
 * <p>The checkpoint storage is configured with a base directory and persists the checkpoint data of
 * specific checkpoints in specific subdirectories. For example, if the base directory was set to
 * {@code hdfs://namenode:port/flink-checkpoints/}, the state backend will create a subdirectory
 * with the job's ID that will contain the actual checkpoints: ({@code
 * hdfs://namenode:port/flink-checkpoints/1b080b6e710aabbef8993ab18c6de98b})
 *
 * <p>Each checkpoint individually will store all its files in a subdirectory that includes the
 * checkpoint number, such as {@code
 * hdfs://namenode:port/flink-checkpoints/1b080b6e710aabbef8993ab18c6de98b/chk-17/}.
 *
 * <h1>Savepoint Layout</h1>
 *
 * <p>A savepoint that is set to be stored in path {@code hdfs://namenode:port/flink-savepoints/},
 * will create a subdirectory {@code savepoint-jobId(0, 6)-randomDigits} in which it stores all
 * savepoint data. The random digits are added as "entropy" to avoid directory collisions.
 *
 * <h1>Metadata File</h1>
 *
 * <p>A completed checkpoint writes its metadata into a file '{@value
 * AbstractFsCheckpointStorageAccess#METADATA_FILE_NAME}'.
 */
@PublicEvolving
public abstract class AbstractFileCheckpointStorage implements CheckpointStorage {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    //  Checkpoint Storage Properties
    // ------------------------------------------------------------------------

    /** The path where checkpoints will be stored, or null, if none has been configured. */
    @Nullable private Path baseCheckpointPath;

    /** The path where savepoints will be stored, or null, if none has been configured. */
    @Nullable private Path baseSavepointPath;

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
        return AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(pointer);
    }

    /**
     * Gets the directory where savepoints are stored by default (when no custom path is given to
     * the savepoint trigger command).
     *
     * @return The default directory for savepoints, or null, if no default directory has been
     *     configured.
     */
    @Nullable
    public Path getSavepointPath() {
        return baseSavepointPath;
    }

    /** @param baseSavepointPath The base directory for savepoints. */
    public void setSavepointPath(String baseSavepointPath) {
        setSavepointPath(new Path(baseSavepointPath));
    }

    /** @param baseSavepointPath The base directory for savepoints. */
    public void setSavepointPath(@Nullable Path baseSavepointPath) {
        this.baseSavepointPath = baseSavepointPath == null ? null : validatePath(baseSavepointPath);
    }

    /**
     * Sets the given savepoint directory, or the values defined in the given configuration. If a
     * checkpoint parameter is not null, that value takes precedence over the value in the
     * configuration. If the configuration does not specify a value, it is possible that the
     * savepoint directory in the savepoint storage will be null.
     *
     * @param baseSavepointPath The checkpoint base directory to use (or null).
     * @param config The configuration to read values from.
     */
    public void setSavepointPath(Path baseSavepointPath, ReadableConfig config) {
        this.baseSavepointPath =
                parameterOrConfigured(
                        baseSavepointPath, config, CheckpointingOptions.SAVEPOINT_DIRECTORY);
    }

    /**
     * Gets the directory where savepoints are stored by default (when no custom path is given to
     * the savepoint trigger command).
     *
     * @return The default directory for savepoints, or null, if no default directory has been
     *     configured.
     */
    @Nullable
    public Path getCheckpointPath() {
        return baseCheckpointPath;
    }

    /** @param baseCheckpointPath The base directory for checkpoints. */
    public void setCheckpointPath(String baseCheckpointPath) {
        setCheckpointPath(new Path(baseCheckpointPath));
    }

    /** @param baseCheckpointPath The base directory for checkpoints. */
    public void setCheckpointPath(@Nullable Path baseCheckpointPath) {
        this.baseCheckpointPath =
                baseCheckpointPath == null ? null : validatePath(baseCheckpointPath);
    }

    /**
     * Sets the given checkpoint directory, or the values defined in the given configuration. If a
     * checkpoint parameter is not null, that value takes precedence over the value in the
     * configuration. If the configuration does not specify a value, it is possible that the
     * checkpoint directory in the checkpoint storage will be null.
     *
     * @param baseCheckpointPath The checkpoint base directory to use (or null).
     * @param config The configuration to read values from.
     */
    public void setCheckpointPath(Path baseCheckpointPath, ReadableConfig config) {
        this.baseCheckpointPath =
                parameterOrConfigured(
                        baseCheckpointPath, config, CheckpointingOptions.CHECKPOINTS_DIRECTORY);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Checks the validity of the path's scheme and path.
     *
     * @param path The path to check.
     * @return The URI as a Path.
     * @throws IllegalArgumentException Thrown, if the URI misses scheme or path.
     */
    protected static Path validatePath(Path path) {
        final URI uri = path.toUri();
        final String scheme = uri.getScheme();
        final String pathPart = uri.getPath();

        // some validity checks
        if (scheme == null) {
            throw new IllegalArgumentException(
                    "The scheme (hdfs://, file://, etc) is null. "
                            + "Please specify the file system scheme explicitly in the URI.");
        }
        if (pathPart == null) {
            throw new IllegalArgumentException(
                    "The path to store the checkpoint data in is null. "
                            + "Please specify a directory path for the checkpoint data.");
        }
        if (pathPart.length() == 0 || pathPart.equals("/")) {
            throw new IllegalArgumentException("Cannot use the root directory for checkpoints.");
        }

        return path;
    }

    @Nullable
    protected static Path parameterOrConfigured(
            @Nullable Path path, ReadableConfig config, ConfigOption<String> option) {
        if (path != null) {
            return path;
        } else {
            String configValue = config.get(option);
            try {
                return configValue == null ? null : new Path(configValue);
            } catch (IllegalArgumentException e) {
                throw new IllegalConfigurationException(
                        "Cannot parse value for "
                                + option.key()
                                + " : "
                                + configValue
                                + " . Not a valid path.");
            }
        }
    }
}
