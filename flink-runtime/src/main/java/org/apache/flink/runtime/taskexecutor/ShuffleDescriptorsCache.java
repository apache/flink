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
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;

/** Cache of shuffle descriptors in TaskExecutor. */
public interface ShuffleDescriptorsCache {

    /** clear all cache. */
    void clear();

    /**
     * Get shuffle descriptor group in cache.
     *
     * @param blobKey identify the shuffle descriptor group
     * @return shuffle descriptor group in cache if exists, otherwise null
     */
    ShuffleDescriptorGroup get(PermanentBlobKey blobKey);

    /**
     * Put shuffle descriptor group to cache.
     *
     * @param jobId of job
     * @param blobKey identify the shuffle descriptor group
     * @param shuffleDescriptorGroup shuffle descriptor group to cache
     */
    void put(JobID jobId, PermanentBlobKey blobKey, ShuffleDescriptorGroup shuffleDescriptorGroup);

    /**
     * Clear all cache for the Job.
     *
     * @param jobId of job
     */
    void clearCacheForJob(JobID jobId);
}
