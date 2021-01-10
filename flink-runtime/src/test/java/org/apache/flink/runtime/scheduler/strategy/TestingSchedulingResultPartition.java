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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple implementation of {@link SchedulingResultPartition} for testing. */
public class TestingSchedulingResultPartition implements SchedulingResultPartition {

    private final IntermediateDataSetID intermediateDataSetID;

    private final IntermediateResultPartitionID intermediateResultPartitionID;

    private final ResultPartitionType partitionType;

    private TestingSchedulingExecutionVertex producer;

    private Collection<TestingSchedulingExecutionVertex> consumers;

    private ResultPartitionState state;

    TestingSchedulingResultPartition(
            IntermediateDataSetID dataSetID, ResultPartitionType type, ResultPartitionState state) {
        this.intermediateDataSetID = dataSetID;
        this.partitionType = type;
        this.state = state;
        this.intermediateResultPartitionID = new IntermediateResultPartitionID();
        this.consumers = new ArrayList<>();
    }

    @Override
    public IntermediateResultPartitionID getId() {
        return intermediateResultPartitionID;
    }

    @Override
    public IntermediateDataSetID getResultId() {
        return intermediateDataSetID;
    }

    @Override
    public ResultPartitionType getResultType() {
        return partitionType;
    }

    @Override
    public ResultPartitionState getState() {
        return state;
    }

    @Override
    public TestingSchedulingExecutionVertex getProducer() {
        return producer;
    }

    @Override
    public Iterable<TestingSchedulingExecutionVertex> getConsumers() {
        return consumers;
    }

    void addConsumer(TestingSchedulingExecutionVertex consumer) {
        this.consumers.add(consumer);
    }

    void setProducer(TestingSchedulingExecutionVertex producer) {
        this.producer = checkNotNull(producer);
    }

    void setState(ResultPartitionState state) {
        this.state = state;
    }

    /** Builder for {@link TestingSchedulingResultPartition}. */
    public static final class Builder {
        private IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();
        private ResultPartitionType resultPartitionType = ResultPartitionType.BLOCKING;
        private ResultPartitionState resultPartitionState = ResultPartitionState.CONSUMABLE;

        Builder withIntermediateDataSetID(IntermediateDataSetID intermediateDataSetId) {
            this.intermediateDataSetId = intermediateDataSetId;
            return this;
        }

        Builder withResultPartitionState(ResultPartitionState state) {
            this.resultPartitionState = state;
            return this;
        }

        Builder withResultPartitionType(ResultPartitionType type) {
            this.resultPartitionType = type;
            return this;
        }

        TestingSchedulingResultPartition build() {
            return new TestingSchedulingResultPartition(
                    intermediateDataSetId, resultPartitionType, resultPartitionState);
        }
    }
}
