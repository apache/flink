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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

/** Simple policy for controlling the scale up/down behavior of the {@link AdaptiveScheduler}. */
@Internal
public interface RescalingController {

    /**
     * This method gets called whenever new resources or {@link JobResourceRequirements resource
     * requirements} are available and the scheduler needs to design whether to rescale or not.
     *
     * @param currentParallelism parallelism of the currently running job graph.
     * @param newParallelism Potential new parallelism with the additional resources.
     * @return true if the policy decided to rescale based on the provided information.
     */
    boolean shouldRescale(VertexParallelism currentParallelism, VertexParallelism newParallelism);
}
