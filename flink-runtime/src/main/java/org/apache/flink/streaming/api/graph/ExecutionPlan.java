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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * An interface representing a general execution plan, which can be implemented by different types
 * of graphs such as JobGraph and StreamGraph.
 *
 * <p>This interface provides methods for accessing general properties of execution plans, such as
 * the job id and job name.
 */
@Internal
public interface ExecutionPlan extends Serializable {

    /**
     * Gets the unique identifier of the job.
     *
     * @return the job id
     */
    JobID getJobID();

    /**
     * Gets the name of the job.
     *
     * @return the job name
     */
    String getName();

    /**
     * Gets the type of the job.
     *
     * @return the job type
     */
    JobType getJobType();

    /**
     * Checks if the execution plan is dynamic.
     *
     * @return true if the execution plan is dynamic; false otherwise
     */
    boolean isDynamic();

    /**
     * Gets the settings for job checkpointing.
     *
     * @return the checkpointing settings
     */
    JobCheckpointingSettings getCheckpointingSettings();

    /**
     * Checks if the execution plan is empty.
     *
     * @return true if the plan is empty; false otherwise
     */
    boolean isEmpty();

    /**
     * Gets the initial client heartbeat timeout.
     *
     * @return the timeout duration in milliseconds
     */
    long getInitialClientHeartbeatTimeout();

    /**
     * Checks if partial resource configuration is specified.
     *
     * @return true if partial resource configuration is set; false otherwise
     */
    boolean isPartialResourceConfigured();

    /**
     * Gets the maximum parallelism level for the job.
     *
     * @return the maximum parallelism
     */
    int getMaximumParallelism();

    /**
     * Gets the job configuration.
     *
     * @return the job configuration
     */
    Configuration getJobConfiguration();

    /**
     * Gets the user-defined JAR files required for the job.
     *
     * @return a list of paths to user JAR files
     */
    List<Path> getUserJars();

    /**
     * Gets the user-defined blob keys corresponding to the JAR files.
     *
     * @return a list of permanent blob keys for user JARs
     */
    List<PermanentBlobKey> getUserJarBlobKeys();

    /**
     * Gets the classpath required for the job.
     *
     * @return a list of classpath URLs
     */
    List<URL> getClasspaths();

    /**
     * Gets the user artifacts associated with the job.
     *
     * @return a map of user artifacts
     */
    Map<String, DistributedCache.DistributedCacheEntry> getUserArtifacts();

    /**
     * Adds a blob key corresponding to a user JAR.
     *
     * @param permanentBlobKey the blob key to add
     */
    void addUserJarBlobKey(PermanentBlobKey permanentBlobKey);

    /**
     * Sets a user artifact blob key for a specified user artifact.
     *
     * @param artifactName the name of the user artifact
     * @param blobKey the blob key corresponding to the user artifact
     * @throws IOException if an error occurs during the operation
     */
    void setUserArtifactBlobKey(String artifactName, PermanentBlobKey blobKey) throws IOException;

    /** Writes user artifact entries to the job configuration. */
    void writeUserArtifactEntriesToConfiguration();

    /**
     * Gets the settings for restoring from a savepoint.
     *
     * @return the savepoint restore settings
     */
    SavepointRestoreSettings getSavepointRestoreSettings();

    /**
     * Sets the settings for restoring from a savepoint.
     *
     * @param savepointRestoreSettings the settings for savepoint restoration
     */
    void setSavepointRestoreSettings(SavepointRestoreSettings savepointRestoreSettings);

    /**
     * Checks if the checkpointing was enabled.
     *
     * @return true if checkpointing enabled
     */
    default boolean isCheckpointingEnabled() {
        JobCheckpointingSettings checkpointingSettings = getCheckpointingSettings();
        if (checkpointingSettings == null) {
            return false;
        }

        return checkpointingSettings
                .getCheckpointCoordinatorConfiguration()
                .isCheckpointingEnabled();
    }

    /**
     * Gets the serialized execution configuration.
     *
     * @return The serialized execution configuration object
     */
    SerializedValue<ExecutionConfig> getSerializedExecutionConfig();
}
