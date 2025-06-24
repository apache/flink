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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class is used to record the {@link ExecutionVertex} that has been reset. */
public class ExecutionVertexResetEvent implements JobEvent {

    private final List<ExecutionVertexID> executionVertexIds;

    public ExecutionVertexResetEvent(List<ExecutionVertexID> executionVertexIds) {
        this.executionVertexIds = checkNotNull(executionVertexIds);
    }

    public List<ExecutionVertexID> getExecutionVertexIds() {
        return executionVertexIds;
    }

    @Override
    public String toString() {
        return "ExecutionVertexResetEvent("
                + "ExecutionVertexIds={"
                + StringUtils.join(executionVertexIds, ",")
                + "})";
    }
}
