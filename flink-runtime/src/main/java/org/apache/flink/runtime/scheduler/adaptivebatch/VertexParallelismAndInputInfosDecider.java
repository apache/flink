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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;

/**
 * {@link VertexParallelismAndInputInfosDecider} is responsible for deciding the parallelism and
 * {@link JobVertexInputInfo}s of a job vertex, based on the information of the consumed blocking
 * results.
 */
public interface VertexParallelismAndInputInfosDecider {

    /**
     * Decide the parallelism and {@link JobVertexInputInfo}s for this job vertex.
     *
     * @param jobVertexId The job vertex id
     * @param consumedResults The information of consumed blocking results
     * @param vertexInitialParallelism The initial parallelism of the job vertex. If it's a positive
     *     number, it will be respected. If it's not set(equals to {@link
     *     ExecutionConfig#PARALLELISM_DEFAULT}), a parallelism will be automatically decided for
     *     the vertex.
     * @param vertexMaxParallelism The max parallelism of the job vertex.
     * @return the parallelism and vertex input infos.
     */
    ParallelismAndInputInfos decideParallelismAndInputInfosForVertex(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int vertexInitialParallelism,
            int vertexMaxParallelism);
}
