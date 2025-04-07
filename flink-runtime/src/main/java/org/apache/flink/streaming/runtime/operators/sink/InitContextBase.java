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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.OptionalLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base implementation for the {@link InitContext}. */
class InitContextBase implements InitContext {

    private final OptionalLong restoredCheckpointId;

    private final StreamingRuntimeContext runtimeContext;

    InitContextBase(StreamingRuntimeContext runtimeContext, OptionalLong restoredCheckpointId) {
        this.runtimeContext = checkNotNull(runtimeContext);
        this.restoredCheckpointId = restoredCheckpointId;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return restoredCheckpointId;
    }

    @Override
    public JobInfo getJobInfo() {
        return runtimeContext.getJobInfo();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return runtimeContext.getTaskInfo();
    }

    RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}
