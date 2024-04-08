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

package org.apache.flink.datastream.api.context;

import org.apache.flink.annotation.Experimental;

/** {@link TaskInfo} contains all the meta information of the task. */
@Experimental
public interface TaskInfo {
    /**
     * Get the parallelism of current task.
     *
     * @return the parallelism of this process function.
     */
    int getParallelism();

    /**
     * Get the max parallelism of current task.
     *
     * @return The max parallelism.
     */
    int getMaxParallelism();

    /**
     * Get the name of current task.
     *
     * @return The name of current task.
     */
    String getTaskName();
}
