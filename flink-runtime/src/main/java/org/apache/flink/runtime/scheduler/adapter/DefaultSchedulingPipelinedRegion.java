/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/** Default implementation of {@link SchedulingPipelinedRegion}. */
public class DefaultSchedulingPipelinedRegion implements SchedulingPipelinedRegion {

    private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVertices;

    private List<ConsumedPartitionGroup> consumedResultIds;

    private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById;

    public DefaultSchedulingPipelinedRegion(
            Set<DefaultExecutionVertex> defaultExecutionVertices,
            Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById) {

        Preconditions.checkNotNull(defaultExecutionVertices);

        this.executionVertices = new HashMap<>();
        for (DefaultExecutionVertex executionVertex : defaultExecutionVertices) {
            this.executionVertices.put(executionVertex.getId(), executionVertex);
        }

        this.resultPartitionsById = resultPartitionsById;
    }

    @VisibleForTesting
    public DefaultSchedulingPipelinedRegion(Set<DefaultExecutionVertex> defaultExecutionVertices) {
        this(defaultExecutionVertices, null);
    }

    @Override
    public Iterable<DefaultExecutionVertex> getVertices() {
        return Collections.unmodifiableCollection(executionVertices.values());
    }

    @Override
    public DefaultExecutionVertex getVertex(final ExecutionVertexID vertexId) {
        final DefaultExecutionVertex executionVertex = executionVertices.get(vertexId);
        if (executionVertex == null) {
            throw new IllegalArgumentException(
                    String.format("Execution vertex %s not found in pipelined region", vertexId));
        }
        return executionVertex;
    }

    @Override
    public Iterable<DefaultResultPartition> getConsumedResults() {
        if (consumedResultIds == null) {
            initializeConsumedResults();
        }
        final List<ConsumedPartitionGroup> consumers = getGroupedConsumedResults();

        return () ->
                new Iterator<DefaultResultPartition>() {
                    private int groupIdx = 0;
                    private int idx = 0;

                    @Override
                    public boolean hasNext() {
                        if (groupIdx < consumers.size()
                                && idx >= consumers.get(groupIdx).getResultPartitions().size()) {
                            ++groupIdx;
                            idx = 0;
                        }
                        return groupIdx < consumers.size()
                                && idx < consumers.get(groupIdx).getResultPartitions().size();
                    }

                    @Override
                    public DefaultResultPartition next() {
                        if (hasNext()) {
                            return (DefaultResultPartition)
                                    getResultPartition(
                                            consumers
                                                    .get(groupIdx)
                                                    .getResultPartitions()
                                                    .get(idx++));
                        } else {
                            throw new NoSuchElementException();
                        }
                    }
                };
    }

    private void initializeConsumedResults() {
        final Set<ConsumedPartitionGroup> consumedResultIds = new HashSet<>();
        for (DefaultExecutionVertex executionVertex : executionVertices.values()) {
            for (ConsumedPartitionGroup consumedResultIdGroup :
                    executionVertex.getGroupedConsumedResults()) {
                SchedulingResultPartition resultPartition =
                        executionVertex.getResultPartition(
                                consumedResultIdGroup.getResultPartitions().get(0));
                if (!executionVertices.containsKey(resultPartition.getProducer().getId())) {
                    consumedResultIds.add(consumedResultIdGroup);
                }
            }
        }

        this.consumedResultIds = new ArrayList<>();
        this.consumedResultIds.addAll(consumedResultIds);
    }

    @Override
    public List<ConsumedPartitionGroup> getGroupedConsumedResults() {
        if (consumedResultIds == null) {
            initializeConsumedResults();
        }
        return Collections.unmodifiableList(consumedResultIds);
    }

    @Override
    public SchedulingResultPartition getResultPartition(IntermediateResultPartitionID id) {
        return resultPartitionsById.get(id);
    }
}
