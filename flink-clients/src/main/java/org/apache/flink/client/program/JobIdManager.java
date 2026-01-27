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

package org.apache.flink.client.program;

import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * Interface for managing the job IDs.
 *
 * <p>This interface allows custom logic to update the job ID of a {@link StreamGraph} before
 * submitting the job.
 */
public interface JobIdManager {

    /**
     * Updates the job ID of the given {@link StreamGraph}.
     *
     * @param streamGraph The {@link StreamGraph} to update.
     */
    void updateJobId(StreamGraph streamGraph);

    JobIdManager NO_OP =
            new JobIdManager() {
                @Override
                public void updateJobId(StreamGraph streamGraph) {
                    // no-op
                }
            };
}
