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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link LogicalResult}. It is an adapter of {@link IntermediateDataSet}.
 */
public class DefaultLogicalResult
        implements LogicalResult<DefaultLogicalVertex, DefaultLogicalResult> {

    private final IntermediateDataSet intermediateDataSet;

    private final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever;

    DefaultLogicalResult(
            final IntermediateDataSet intermediateDataSet,
            final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever) {

        this.intermediateDataSet = checkNotNull(intermediateDataSet);
        this.vertexRetriever = checkNotNull(vertexRetriever);
    }

    @Override
    public IntermediateDataSetID getId() {
        return intermediateDataSet.getId();
    }

    @Override
    public ResultPartitionType getResultType() {
        return intermediateDataSet.getResultType();
    }

    @Override
    public DefaultLogicalVertex getProducer() {
        return vertexRetriever.apply(intermediateDataSet.getProducer().getID());
    }

    @Override
    public Iterable<DefaultLogicalVertex> getConsumers() {
        return intermediateDataSet.getConsumers().stream()
                .map(JobEdge::getTarget)
                .map(JobVertex::getID)
                .map(vertexRetriever)
                .collect(Collectors.toList());
    }
}
