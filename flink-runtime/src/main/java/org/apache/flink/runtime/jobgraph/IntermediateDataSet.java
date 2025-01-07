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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An intermediate data set is the data set produced by an operator - either a source or any
 * intermediate operation.
 *
 * <p>Intermediate data sets may be read by other operators, materialized, or discarded.
 */
public class IntermediateDataSet implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final IntermediateDataSetID id; // the identifier

    private final JobVertex producer; // the operation that produced this data set

    // All consumers must have the same partitioner and parallelism
    private final List<JobEdge> consumers = new ArrayList<>();

    // The type of partition to use at runtime
    private final ResultPartitionType resultType;

    private DistributionPattern distributionPattern;

    private boolean isBroadcast;

    private boolean isForward;

    /** The number of job edges that need to be created. */
    private int numJobEdgesToCreate;

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSet(
            IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
        this.id = checkNotNull(id);
        this.producer = checkNotNull(producer);
        this.resultType = checkNotNull(resultType);
    }

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSetID getId() {
        return id;
    }

    public JobVertex getProducer() {
        return producer;
    }

    public List<JobEdge> getConsumers() {
        return this.consumers;
    }

    public boolean areAllConsumerVerticesCreated() {
        return numJobEdgesToCreate == consumers.size();
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public boolean isForward() {
        return isForward;
    }

    public DistributionPattern getDistributionPattern() {
        return distributionPattern;
    }

    public ResultPartitionType getResultType() {
        return resultType;
    }

    // --------------------------------------------------------------------------------------------

    public void addConsumer(JobEdge edge) {
        // sanity check
        checkState(id.equals(edge.getSourceId()), "Incompatible dataset id.");

        if (consumers.isEmpty() && distributionPattern == null) {
            distributionPattern = edge.getDistributionPattern();
            isBroadcast = edge.isBroadcast();
            isForward = edge.isForward();
        } else {
            checkState(
                    distributionPattern == edge.getDistributionPattern(),
                    "Incompatible distribution pattern.");
            checkState(isBroadcast == edge.isBroadcast(), "Incompatible broadcast type.");
            checkState(isForward == edge.isForward(), "Incompatible forward type.");
        }
        consumers.add(edge);
    }

    public void configure(
            DistributionPattern distributionPattern, boolean isBroadcast, boolean isForward) {
        checkState(consumers.isEmpty(), "The output job edges have already been added.");
        if (this.distributionPattern == null) {
            this.distributionPattern = distributionPattern;
            this.isBroadcast = isBroadcast;
            this.isForward = isForward;
        } else {
            checkState(
                    this.distributionPattern == distributionPattern,
                    "Incompatible distribution pattern.");
            checkState(this.isBroadcast == isBroadcast, "Incompatible broadcast type.");
            checkState(this.isForward == isForward, "Incompatible forward type.");
        }
    }

    public void updateOutputPattern(
            DistributionPattern distributionPattern, boolean isBroadcast, boolean isForward) {
        checkState(consumers.isEmpty(), "The output job edges have already been added.");
        checkState(
                numJobEdgesToCreate == 1,
                "Modification is not allowed when the subscribing output is reused.");

        this.distributionPattern = distributionPattern;
        this.isBroadcast = isBroadcast;
        this.isForward = isForward;
    }

    public void increaseNumJobEdgesToCreate() {
        this.numJobEdgesToCreate++;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Intermediate Data Set (" + id + ")";
    }
}
