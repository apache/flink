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

import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.dag.GroupReduceNode;
import org.apache.flink.optimizer.dag.SingleInputNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;

import java.util.Collections;
import java.util.List;

public final class PartialGroupProperties extends OperatorDescriptorSingle {

    public PartialGroupProperties(FieldSet keys) {
        super(keys);
    }

    @Override
    public DriverStrategy getStrategy() {
        return DriverStrategy.SORTED_GROUP_COMBINE;
    }

    @Override
    public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
        // create in input node for combine with the same parallelism as input node
        GroupReduceNode combinerNode =
                new GroupReduceNode((GroupReduceOperatorBase<?, ?, ?>) node.getOperator());
        combinerNode.setParallelism(in.getSource().getParallelism());

        SingleInputPlanNode combiner =
                new SingleInputPlanNode(
                        combinerNode,
                        "Combine (" + node.getOperator().getName() + ")",
                        in,
                        DriverStrategy.SORTED_GROUP_COMBINE);
        // sorting key info
        combiner.setDriverKeyInfo(in.getLocalStrategyKeys(), in.getLocalStrategySortOrder(), 0);
        // set grouping comparator key info
        combiner.setDriverKeyInfo(this.keyList, 1);

        return combiner;
    }

    @Override
    protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
        return Collections.singletonList(new RequestedGlobalProperties());
    }

    @Override
    protected List<RequestedLocalProperties> createPossibleLocalProperties() {
        RequestedLocalProperties props = new RequestedLocalProperties();
        props.setGroupedFields(this.keys);
        return Collections.singletonList(props);
    }

    @Override
    public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
        if (gProps.getUniqueFieldCombination() != null
                && gProps.getUniqueFieldCombination().size() > 0
                && gProps.getPartitioning() == PartitioningProperty.RANDOM_PARTITIONED) {
            gProps.setAnyPartitioning(
                    gProps.getUniqueFieldCombination().iterator().next().toFieldList());
        }
        gProps.clearUniqueFieldCombinations();
        return gProps;
    }

    @Override
    public LocalProperties computeLocalProperties(LocalProperties lProps) {
        return lProps.clearUniqueFieldSets();
    }
}
