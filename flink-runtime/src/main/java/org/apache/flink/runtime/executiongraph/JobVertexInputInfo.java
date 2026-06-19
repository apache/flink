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

package org.apache.flink.runtime.executiongraph;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class describe how a job vertex consume an input(intermediate result). */
public class JobVertexInputInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<ExecutionVertexInputInfo> executionVertexInputInfos;

    public JobVertexInputInfo(final List<ExecutionVertexInputInfo> executionVertexInputInfos) {
        this.executionVertexInputInfos = checkNotNull(executionVertexInputInfos);
    }

    /** The input information of subtasks of this job vertex. */
    public List<ExecutionVertexInputInfo> getExecutionVertexInputInfos() {
        return executionVertexInputInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobVertexInputInfo that = (JobVertexInputInfo) o;
        return Objects.equals(executionVertexInputInfos, that.executionVertexInputInfos);
    }
}
