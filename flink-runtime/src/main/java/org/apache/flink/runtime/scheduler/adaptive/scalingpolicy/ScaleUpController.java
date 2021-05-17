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
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler;

/** Simple policy for controlling the scale up behavior of the {@link AdaptiveScheduler}. */
@Internal
public interface ScaleUpController {

    /**
     * This method gets called whenever new resources are available to the scheduler to scale up.
     *
     * @param currentCumulativeParallelism Cumulative parallelism of the currently running job
     *     graph.
     * @param newCumulativeParallelism Potential new cumulative parallelism with the additional
     *     resources.
     * @return true if the policy decided to scale up based on the provided information.
     */
    boolean canScaleUp(int currentCumulativeParallelism, int newCumulativeParallelism);
}
