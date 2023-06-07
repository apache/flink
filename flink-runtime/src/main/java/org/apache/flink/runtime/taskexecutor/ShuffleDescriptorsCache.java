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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.SerializedShuffleDescriptorAndIndicesID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;

/** Cache of shuffle descriptors in TaskExecutor. */
public interface ShuffleDescriptorsCache {
    /**
     * Start cache manager.
     *
     * @param mainThreadExecutor of main thread executor.
     */
    void start(ComponentMainThreadExecutor mainThreadExecutor);

    /** Stop cache manager. */
    void stop();

    /**
     * Get shuffle descriptors entry in cache.
     *
     * @param serializedShuffleDescriptorsId of cached serialized shuffle descriptors
     * @return cache shuffle descriptors entry in cache if exists, otherwise null
     */
    ShuffleDescriptorCacheEntry get(
            SerializedShuffleDescriptorAndIndicesID serializedShuffleDescriptorsId);

    /**
     * Put cache shuffle descriptors to cache.
     *
     * @param jobId of job
     * @param serializedShuffleDescriptorsId of cached serialized shuffle descriptors
     * @param shuffleDescriptorAndIndices of cached serialized shuffle descriptors
     */
    void put(
            JobID jobId,
            SerializedShuffleDescriptorAndIndicesID serializedShuffleDescriptorsId,
            TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]
                    shuffleDescriptorAndIndices);

    /**
     * Clear all cache of Job.
     *
     * @param jobId of job
     */
    void clearCacheOfJob(JobID jobId);

    /** Cached shuffle descriptors information. */
    class ShuffleDescriptorCacheEntry {
        private final TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]
                shuffleDescriptorAndIndices;
        private final JobID jobId;
        private long idleSince;

        public ShuffleDescriptorCacheEntry(
                TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]
                        shuffleDescriptorAndIndices,
                JobID jobId,
                long idleSince) {
            this.shuffleDescriptorAndIndices = shuffleDescriptorAndIndices;
            this.jobId = jobId;
            this.idleSince = idleSince;
        }

        public void updateIdleSince(long idleSince) {
            this.idleSince = idleSince;
        }

        public TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]
                getShuffleDescriptorAndIndices() {
            return shuffleDescriptorAndIndices;
        }

        public JobID getJobId() {
            return jobId;
        }

        public long getIdleSince() {
            return idleSince;
        }
    }
}
