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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.context.TaskInfo;

/** Default implementation of {@link TaskInfo}. */
public class DefaultTaskInfo implements TaskInfo {
    private final int parallelism;

    private final int maxParallelism;

    private final String taskName;

    public DefaultTaskInfo(int parallelism, int maxParallelism, String taskName) {
        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
        this.taskName = taskName;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public int getMaxParallelism() {
        return maxParallelism;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }
}
