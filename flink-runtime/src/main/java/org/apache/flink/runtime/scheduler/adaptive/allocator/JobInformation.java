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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;

import javax.annotation.Nullable;

import java.util.Collection;

/** Information about the job. */
public interface JobInformation {
    /**
     * Returns all slot-sharing groups of the job.
     *
     * <p>Attention: The returned slot sharing groups should never be modified (they are indeed
     * mutable)!
     *
     * @return all slot-sharing groups of the job
     */
    Collection<SlotSharingGroup> getSlotSharingGroups();

    /**
     * Returns all co-location groups of the job.
     *
     * <p>Attention: The returned co-location groups should never be modified (its are indeed
     * mutable)!
     *
     * @return all co-location groups of the job
     */
    Collection<CoLocationGroup> getCoLocationGroups();

    VertexInformation getVertexInformation(JobVertexID jobVertexId);

    Iterable<VertexInformation> getVertices();

    default VertexParallelismStore getVertexParallelismStore() {
        throw new UnsupportedOperationException();
    }

    /** Information about a single vertex. */
    interface VertexInformation {
        JobVertexID getJobVertexID();

        String getVertexName();

        int getMinParallelism();

        int getParallelism();

        int getMaxParallelism();

        SlotSharingGroup getSlotSharingGroup();

        @Nullable
        CoLocationGroup getCoLocationGroup();
    }
}
