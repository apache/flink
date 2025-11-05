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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

import java.util.Collection;

/**
 * The resolver to define how to construct execution slot sharing groups from the execution vertices
 * of a job with the vertices parallelisms.
 */
public interface SlotSharingResolver {
    /**
     * Get the execution slot sharing groups for the job information based on the vertices
     * parallelisms.
     *
     * @param jobInformation the job information.
     * @param vertexParallelism the parallelisms for the vertices of the job information.
     * @return the all execution slot sharing groups for the deployment of the job.
     */
    Collection<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups(
            JobInformation jobInformation, VertexParallelism vertexParallelism);
}
