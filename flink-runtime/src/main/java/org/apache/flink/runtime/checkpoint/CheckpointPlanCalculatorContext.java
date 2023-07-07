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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.util.concurrent.ScheduledExecutor;

/**
 * Provides the context for {@link DefaultPlanCalculator} to compute the plan of
 * checkpoints.
 */
public interface CheckpointPlanCalculatorContext {

    /**
     * Acquires the main thread executor for this job.
     *
     * @return The main thread executor.
     */
    ScheduledExecutor getMainExecutor();

    /**
     * Detects whether there are already some tasks finished.
     *
     * @return Whether there are finished tasks.
     */
    boolean hasFinishedTasks();
}
