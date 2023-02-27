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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Group of consumer {@link ExecutionVertexID}s. One such a group corresponds to one {@link
 * ConsumedPartitionGroup}.
 */
public class ConsumerVertexGroup implements Iterable<ExecutionVertexID> {
    private final List<ExecutionVertexID> vertices;

    private final ResultPartitionType resultPartitionType;

    @Nullable private ConsumedPartitionGroup consumedPartitionGroup;

    private ConsumerVertexGroup(
            List<ExecutionVertexID> vertices, ResultPartitionType resultPartitionType) {
        this.vertices = vertices;
        this.resultPartitionType = resultPartitionType;
    }

    public static ConsumerVertexGroup fromMultipleVertices(
            List<ExecutionVertexID> vertices, ResultPartitionType resultPartitionType) {
        return new ConsumerVertexGroup(vertices, resultPartitionType);
    }

    public static ConsumerVertexGroup fromSingleVertex(
            ExecutionVertexID vertex, ResultPartitionType resultPartitionType) {
        return new ConsumerVertexGroup(Collections.singletonList(vertex), resultPartitionType);
    }

    public ResultPartitionType getResultPartitionType() {
        return resultPartitionType;
    }

    @Override
    public Iterator<ExecutionVertexID> iterator() {
        return vertices.iterator();
    }

    public int size() {
        return vertices.size();
    }

    public boolean isEmpty() {
        return vertices.isEmpty();
    }

    public ExecutionVertexID getFirst() {
        return iterator().next();
    }

    public ConsumedPartitionGroup getConsumedPartitionGroup() {
        return checkNotNull(consumedPartitionGroup, "ConsumedPartitionGroup is not properly set.");
    }

    public void setConsumedPartitionGroup(ConsumedPartitionGroup consumedPartitionGroup) {
        checkState(this.consumedPartitionGroup == null);
        this.consumedPartitionGroup = checkNotNull(consumedPartitionGroup);
    }
}
