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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldSet;
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

public final class GroupReduceProperties extends OperatorDescriptorSingle {

    private final Ordering
            ordering; // ordering that we need to use if an additional ordering is requested

    private final Partitioner<?> customPartitioner;

    public GroupReduceProperties(FieldSet keys) {
        this(keys, null, null);
    }

    public GroupReduceProperties(FieldSet keys, Ordering additionalOrderKeys) {
        this(keys, additionalOrderKeys, null);
    }

    public GroupReduceProperties(FieldSet keys, Partitioner<?> customPartitioner) {
        this(keys, null, customPartitioner);
    }

    public GroupReduceProperties(
            FieldSet groupKeys, Ordering additionalOrderKeys, Partitioner<?> customPartitioner) {
        super(groupKeys);

        // if we have an additional ordering, construct the ordering to have primarily the grouping
        // fields
        if (additionalOrderKeys != null) {
            this.ordering = new Ordering();
            for (Integer key : this.keyList) {
                this.ordering.appendOrdering(key, null, Order.ANY);
            }

            // and next the additional order fields
            for (int i = 0; i < additionalOrderKeys.getNumberOfFields(); i++) {
                Integer field = additionalOrderKeys.getFieldNumber(i);
                Order order = additionalOrderKeys.getOrder(i);
                this.ordering.appendOrdering(field, additionalOrderKeys.getType(i), order);
            }
        } else {
            this.ordering = null;
        }

        this.customPartitioner = customPartitioner;
    }

    @Override
    public DriverStrategy getStrategy() {
        return DriverStrategy.SORTED_GROUP_REDUCE;
    }

    @Override
    public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
        return new SingleInputPlanNode(
                node,
                "GroupReduce (" + node.getOperator().getName() + ")",
                in,
                DriverStrategy.SORTED_GROUP_REDUCE,
                this.keyList);
    }

    @Override
    protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
        RequestedGlobalProperties props = new RequestedGlobalProperties();

        if (customPartitioner == null) {
            props.setAnyPartitioning(this.keys);
        } else {
            props.setCustomPartitioned(this.keys, this.customPartitioner);
        }
        return Collections.singletonList(props);
    }

    @Override
    protected List<RequestedLocalProperties> createPossibleLocalProperties() {
        RequestedLocalProperties props = new RequestedLocalProperties();
        if (this.ordering == null) {
            props.setGroupedFields(this.keys);
        } else {
            props.setOrdering(this.ordering);
        }
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
