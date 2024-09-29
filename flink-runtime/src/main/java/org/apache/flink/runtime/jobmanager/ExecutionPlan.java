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
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;

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
 * the job ID and job name.
 */
public interface ExecutionPlan extends Serializable {

    long serialVersionUID = 1L;

    /**
     * Gets the unique identifier of the job.
     *
     * @return the job ID
     */
    JobID getJobID();

    List<Path> getUserJars();

    List<PermanentBlobKey> getUserJarBlobKeys();

    JobType getJobType();

    boolean isDynamic();

    JobCheckpointingSettings getCheckpointingSettings();

    List<URL> getClasspaths();

    /**
     * Gets the name of the job.
     *
     * @return the job name
     */
    String getName();

    long getInitialClientHeartbeatTimeout();

    boolean isPartialResourceConfigured();

    boolean isEmptyGraph();

    Configuration getJobConfiguration();

    Map<String, DistributedCache.DistributedCacheEntry> getUserArtifacts();

    void addUserJarBlobKey(PermanentBlobKey permanentBlobKey);

    void setUserArtifactBlobKey(String f0, PermanentBlobKey f1) throws IOException;

    void writeUserArtifactEntriesToConfiguration();

    SavepointRestoreSettings getSavepointRestoreSettings();

    void setSavepointRestoreSettings(SavepointRestoreSettings savepointRestoreSettings);

    int getMaximumParallelism();
}
