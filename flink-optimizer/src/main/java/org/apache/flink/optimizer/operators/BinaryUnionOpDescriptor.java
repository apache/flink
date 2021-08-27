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

package org.apache.flink.optimizer.operators;

import org.apache.flink.optimizer.dag.BinaryUnionNode;
import org.apache.flink.optimizer.dag.TwoInputNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.BinaryUnionPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;

import java.util.Collections;
import java.util.List;

/** */
public class BinaryUnionOpDescriptor extends OperatorDescriptorDual {

    public BinaryUnionOpDescriptor() {
        super();
    }

    @Override
    public DriverStrategy getStrategy() {
        return DriverStrategy.UNION;
    }

    @Override
    protected List<GlobalPropertiesPair> createPossibleGlobalProperties() {
        return Collections.emptyList();
    }

    @Override
    protected List<LocalPropertiesPair> createPossibleLocalProperties() {
        return Collections.emptyList();
    }

    @Override
    public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
        return new BinaryUnionPlanNode((BinaryUnionNode) node, in1, in2);
    }

    @Override
    public GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2) {
        GlobalProperties newProps = new GlobalProperties();

        if (in1.getPartitioning() == PartitioningProperty.HASH_PARTITIONED
                && in2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED
                && in1.getPartitioningFields().equals(in2.getPartitioningFields())) {
            newProps.setHashPartitioned(in1.getPartitioningFields());
        } else if (in1.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED
                && in2.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED
                && in1.getPartitioningOrdering().equals(in2.getPartitioningOrdering())
                && (in1.getDataDistribution() == null && in2.getDataDistribution() == null
                        || in1.getDataDistribution() != null
                                && in1.getDataDistribution().equals(in2.getDataDistribution()))) {
            if (in1.getDataDistribution() == null) {
                newProps.setRangePartitioned(in1.getPartitioningOrdering());
            } else {
                newProps.setRangePartitioned(
                        in1.getPartitioningOrdering(), in1.getDataDistribution());
            }
        } else if (in1.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING
                && in2.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING
                && in1.getPartitioningFields().equals(in2.getPartitioningFields())
                && in1.getCustomPartitioner().equals(in2.getCustomPartitioner())) {
            newProps.setCustomPartitioned(in1.getPartitioningFields(), in1.getCustomPartitioner());
        } else if (in1.getPartitioning() == PartitioningProperty.FORCED_REBALANCED
                && in2.getPartitioning() == PartitioningProperty.FORCED_REBALANCED) {
            newProps.setForcedRebalanced();
        } else if (in1.getPartitioning() == PartitioningProperty.FULL_REPLICATION
                && in2.getPartitioning() == PartitioningProperty.FULL_REPLICATION) {
            newProps.setFullyReplicated();
        }

        return newProps;
    }

    @Override
    public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
        // all local properties are destroyed
        return new LocalProperties();
    }

    @Override
    public boolean areCoFulfilled(
            RequestedLocalProperties requested1,
            RequestedLocalProperties requested2,
            LocalProperties produced1,
            LocalProperties produced2) {
        return true;
    }

    @Override
    public boolean areCompatible(
            RequestedGlobalProperties requested1,
            RequestedGlobalProperties requested2,
            GlobalProperties produced1,
            GlobalProperties produced2) {
        return true;
    }
}
