/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.cleanup.GloballyCleanableResource;
import org.apache.flink.runtime.dispatcher.cleanup.LocallyCleanableResource;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Allows to store and remove job graphs. */
public interface JobGraphWriter extends LocallyCleanableResource, GloballyCleanableResource {
    /**
     * Adds the {@link JobGraph} instance.
     *
     * <p>If a job graph with the same {@link JobID} exists, it is replaced.
     */
    void putJobGraph(JobGraph jobGraph) throws Exception;

    /**
     * Persist {@link JobResourceRequirements job resource requirements} for the given job.
     *
     * @param jobId job the given requirements belong to
     * @param jobResourceRequirements requirements to persist
     * @throws Exception in case we're not able to persist the requirements for some reason
     */
    void putJobResourceRequirements(JobID jobId, JobResourceRequirements jobResourceRequirements)
            throws Exception;

    @Override
    default CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        return FutureUtils.completedVoidFuture();
    }

    @Override
    default CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        return FutureUtils.completedVoidFuture();
    }
}
