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

package org.apache.flink.runtime.scheduler.adaptive.scalingpolicy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import static org.apache.flink.configuration.JobManagerOptions.MIN_PARALLELISM_INCREASE;

/**
 * Simple scaling policy. The user can configure a minimum cumulative parallelism increase to allow
 * a scale up.
 */
public class EnforceMinimalIncreaseRescalingController implements RescalingController {

    private final int minParallelismIncrease;

    public EnforceMinimalIncreaseRescalingController(Configuration configuration) {
        minParallelismIncrease = configuration.get(MIN_PARALLELISM_INCREASE);
    }

    @Override
    public boolean shouldRescale(
            VertexParallelism currentParallelism, VertexParallelism newParallelism) {
        for (JobVertexID vertex : currentParallelism.getVertices()) {
            int parallelismChange =
                    newParallelism.getParallelism(vertex)
                            - currentParallelism.getParallelism(vertex);
            if (parallelismChange < 0 || parallelismChange >= minParallelismIncrease) {
                return true;
            }
        }
        return false;
    }
}
