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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;

import java.util.ArrayList;
import java.util.List;

/** Helper class to help maintain the information of an operator. */
@Internal
public class OperatorInfo {

    private final OperatorID operatorId;

    private StreamConfig vertexConfig;

    // This is used to cache the non-chainable outputs, to set the non-chainable outputs config
    // after all job vertices are created.
    private final List<StreamEdge> nonChainableOutputs;

    public OperatorInfo(OperatorID operatorId) {
        this.operatorId = operatorId;
        this.nonChainableOutputs = new ArrayList<>();
    }

    public List<StreamEdge> getNonChainableOutputs() {
        return nonChainableOutputs;
    }

    public void addNonChainableOutputs(List<StreamEdge> nonChainableOutEdges) {
        this.nonChainableOutputs.addAll(nonChainableOutEdges);
    }

    public StreamConfig getVertexConfig() {
        return vertexConfig;
    }

    public void setVertexConfig(StreamConfig vertexConfig) {
        this.vertexConfig = vertexConfig;
    }

    public OperatorID getOperatorId() {
        return operatorId;
    }
}
