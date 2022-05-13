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

package org.apache.flink.optimizer.plan;

import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.TwoInputNode;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

/** */
public class DualInputPlanNode extends PlanNode {

    protected final Channel input1;
    protected final Channel input2;

    protected final FieldList keys1;
    protected final FieldList keys2;

    protected final boolean[] sortOrders;

    private TypeComparatorFactory<?> comparator1;
    private TypeComparatorFactory<?> comparator2;
    private TypePairComparatorFactory<?, ?> pairComparator;

    public Object postPassHelper1;
    public Object postPassHelper2;

    // --------------------------------------------------------------------------------------------

    public DualInputPlanNode(
            OptimizerNode template,
            String nodeName,
            Channel input1,
            Channel input2,
            DriverStrategy diverStrategy) {
        this(template, nodeName, input1, input2, diverStrategy, null, null, null);
    }

    public DualInputPlanNode(
            OptimizerNode template,
            String nodeName,
            Channel input1,
            Channel input2,
            DriverStrategy diverStrategy,
            FieldList driverKeyFields1,
            FieldList driverKeyFields2) {
        this(
                template,
                nodeName,
                input1,
                input2,
                diverStrategy,
                driverKeyFields1,
                driverKeyFields2,
                SingleInputPlanNode.getTrueArray(driverKeyFields1.size()));
    }

    public DualInputPlanNode(
            OptimizerNode template,
            String nodeName,
            Channel input1,
            Channel input2,
            DriverStrategy diverStrategy,
            FieldList driverKeyFields1,
            FieldList driverKeyFields2,
            boolean[] driverSortOrders) {
        super(template, nodeName, diverStrategy);
        this.input1 = input1;
        this.input2 = input2;
        this.keys1 = driverKeyFields1;
        this.keys2 = driverKeyFields2;
        this.sortOrders = driverSortOrders;

        if (this.input1.getShipStrategy() == ShipStrategyType.BROADCAST) {
            this.input1.setReplicationFactor(getParallelism());
        }
        if (this.input2.getShipStrategy() == ShipStrategyType.BROADCAST) {
            this.input2.setReplicationFactor(getParallelism());
        }

        mergeBranchPlanMaps(input1.getSource(), input2.getSource());
    }

    // --------------------------------------------------------------------------------------------

    public TwoInputNode getTwoInputNode() {
        if (this.template instanceof TwoInputNode) {
            return (TwoInputNode) this.template;
        } else {
            throw new RuntimeException();
        }
    }

    public FieldList getKeysForInput1() {
        return this.keys1;
    }

    public FieldList getKeysForInput2() {
        return this.keys2;
    }

    public boolean[] getSortOrders() {
        return this.sortOrders;
    }

    public TypeComparatorFactory<?> getComparator1() {
        return this.comparator1;
    }

    public TypeComparatorFactory<?> getComparator2() {
        return this.comparator2;
    }

    public void setComparator1(TypeComparatorFactory<?> comparator) {
        this.comparator1 = comparator;
    }

    public void setComparator2(TypeComparatorFactory<?> comparator) {
        this.comparator2 = comparator;
    }

    public TypePairComparatorFactory<?, ?> getPairComparator() {
        return this.pairComparator;
    }

    public void setPairComparator(TypePairComparatorFactory<?, ?> comparator) {
        this.pairComparator = comparator;
    }

    /**
     * Gets the first input channel to this node.
     *
     * @return The first input channel to this node.
     */
    public Channel getInput1() {
        return this.input1;
    }

    /**
     * Gets the second input channel to this node.
     *
     * @return The second input channel to this node.
     */
    public Channel getInput2() {
        return this.input2;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void accept(Visitor<PlanNode> visitor) {
        if (visitor.preVisit(this)) {
            this.input1.getSource().accept(visitor);
            this.input2.getSource().accept(visitor);

            for (Channel broadcastInput : getBroadcastInputs()) {
                broadcastInput.getSource().accept(visitor);
            }

            visitor.postVisit(this);
        }
    }

    @Override
    public Iterable<PlanNode> getPredecessors() {
        if (getBroadcastInputs() == null || getBroadcastInputs().isEmpty()) {
            return Arrays.asList(this.input1.getSource(), this.input2.getSource());
        } else {
            List<PlanNode> preds = new ArrayList<PlanNode>();

            preds.add(input1.getSource());
            preds.add(input2.getSource());

            for (Channel c : getBroadcastInputs()) {
                preds.add(c.getSource());
            }

            return preds;
        }
    }

    @Override
    public Iterable<Channel> getInputs() {
        return Arrays.asList(this.input1, this.input2);
    }

    @Override
    public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
        if (source == this) {
            return FOUND_SOURCE;
        }

        // check first input
        SourceAndDamReport res1 = this.input1.getSource().hasDamOnPathDownTo(source);
        if (res1 == FOUND_SOURCE_AND_DAM) {
            return FOUND_SOURCE_AND_DAM;
        } else if (res1 == FOUND_SOURCE) {
            if (this.input1.getLocalStrategy().dams()
                    || this.input1.getTempMode().breaksPipeline()
                    || getDriverStrategy().firstDam() == DamBehavior.FULL_DAM) {
                return FOUND_SOURCE_AND_DAM;
            } else {
                return FOUND_SOURCE;
            }
        } else {
            SourceAndDamReport res2 = this.input2.getSource().hasDamOnPathDownTo(source);
            if (res2 == FOUND_SOURCE_AND_DAM) {
                return FOUND_SOURCE_AND_DAM;
            } else if (res2 == FOUND_SOURCE) {
                if (this.input2.getLocalStrategy().dams()
                        || this.input2.getTempMode().breaksPipeline()
                        || getDriverStrategy().secondDam() == DamBehavior.FULL_DAM) {
                    return FOUND_SOURCE_AND_DAM;
                } else {
                    return FOUND_SOURCE;
                }
            } else {
                // NOT_FOUND
                // check the broadcast inputs

                for (NamedChannel nc : getBroadcastInputs()) {
                    SourceAndDamReport bcRes = nc.getSource().hasDamOnPathDownTo(source);
                    if (bcRes != NOT_FOUND) {
                        // broadcast inputs are always dams
                        return FOUND_SOURCE_AND_DAM;
                    }
                }
                return NOT_FOUND;
            }
        }
    }
}
