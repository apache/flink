/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.CompletableFuture;

/** A pipeline executor that can reach jobs it did not submit. */
@Internal
public interface JobRetrievalSupportedPipelineExecutor extends PipelineExecutor {

    /**
     * A JobClient for an existing job on the cluster the configuration names. Fails if the cluster
     * can't be retrieved.
     */
    CompletableFuture<JobClient> retrieveJobClient(
            JobID jobId, Configuration configuration, ClassLoader userCodeClassloader);
}
