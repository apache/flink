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

package org.apache.flink.streaming.api.graph.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import javax.annotation.Nullable;

/** Helper class carries the data required to updates a stream edge. */
@Internal
public class StreamEdgeUpdateRequestInfo {
    private final String edgeId;
    private final Integer sourceId;
    private final Integer targetId;

    private StreamPartitioner<?> outputPartitioner;

    // The type number for the input of co-tasks.
    // For two or more inputs, typeNumber must be >= 1, and 0 means the request will not change the
    // typeNumber.
    private int typeNumber;

    // Null means no modifications will be applied to it
    @Nullable private Boolean intraInputKeyCorrelated;

    public StreamEdgeUpdateRequestInfo(String edgeId, Integer sourceId, Integer targetId) {
        this.edgeId = edgeId;
        this.sourceId = sourceId;
        this.targetId = targetId;
    }

    public StreamEdgeUpdateRequestInfo withOutputPartitioner(
            StreamPartitioner<?> outputPartitioner) {
        this.outputPartitioner = outputPartitioner;
        return this;
    }

    public StreamEdgeUpdateRequestInfo withTypeNumber(int typeNumber) {
        this.typeNumber = typeNumber;
        return this;
    }

    public StreamEdgeUpdateRequestInfo withIntraInputKeyCorrelated(
            boolean intraInputKeyCorrelated) {
        this.intraInputKeyCorrelated = intraInputKeyCorrelated;
        return this;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public Integer getSourceId() {
        return sourceId;
    }

    public Integer getTargetId() {
        return targetId;
    }

    public StreamPartitioner<?> getOutputPartitioner() {
        return outputPartitioner;
    }

    public int getTypeNumber() {
        return typeNumber;
    }

    @Nullable
    public Boolean getIntraInputKeyCorrelated() {
        return intraInputKeyCorrelated;
    }
}
