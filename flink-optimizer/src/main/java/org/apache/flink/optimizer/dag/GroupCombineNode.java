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

package org.apache.flink.optimizer.dag;

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.GroupCombineOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.AllGroupCombineProperties;
import org.apache.flink.optimizer.operators.GroupCombineProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;

import java.util.Collections;
import java.util.List;

/** The optimizer representation of a <i>GroupCombineNode</i> operation. */
public class GroupCombineNode extends SingleInputNode {

    private final List<OperatorDescriptorSingle> possibleProperties;

    /**
     * Creates a new optimizer node for the given operator.
     *
     * @param operator The reduce operation.
     */
    public GroupCombineNode(GroupCombineOperatorBase<?, ?, ?> operator) {
        super(operator);

        if (this.keys == null) {
            // case of a key-less reducer. force a parallelism of 1
            setParallelism(1);
        }

        this.possibleProperties = initPossibleProperties();
    }

    private List<OperatorDescriptorSingle> initPossibleProperties() {

        // check if we can work with a grouping (simple reducer), or if we need ordering because of
        // a group order
        Ordering groupOrder = getOperator().getGroupOrder();
        if (groupOrder != null && groupOrder.getNumberOfFields() == 0) {
            groupOrder = null;
        }

        OperatorDescriptorSingle props =
                (this.keys == null
                        ? new AllGroupCombineProperties()
                        : new GroupCombineProperties(this.keys, groupOrder));

        return Collections.singletonList(props);
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the operator represented by this optimizer node.
     *
     * @return The operator represented by this optimizer node.
     */
    @Override
    public GroupCombineOperatorBase<?, ?, ?> getOperator() {
        return (GroupCombineOperatorBase<?, ?, ?>) super.getOperator();
    }

    @Override
    public String getOperatorName() {
        return "GroupCombine";
    }

    @Override
    protected List<OperatorDescriptorSingle> getPossibleProperties() {
        return this.possibleProperties;
    }

    @Override
    protected SemanticProperties getSemanticPropertiesForLocalPropertyFiltering() {

        // Local properties for GroupCombine may only be preserved on key fields.
        SingleInputSemanticProperties origProps =
                ((SingleInputOperator<?, ?, ?>) getOperator()).getSemanticProperties();
        SingleInputSemanticProperties filteredProps = new SingleInputSemanticProperties();
        FieldSet readSet = origProps.getReadFields(0);
        if (readSet != null) {
            filteredProps.addReadFields(readSet);
        }

        // only add forward field information for key fields
        if (this.keys != null) {
            for (int f : this.keys) {
                FieldSet targets = origProps.getForwardingTargetFields(0, f);
                for (int t : targets) {
                    filteredProps.addForwardedField(f, t);
                }
            }
        }
        return filteredProps;
    }

    // --------------------------------------------------------------------------------------------
    //  Estimates
    // --------------------------------------------------------------------------------------------

    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        // no real estimates possible for a reducer.
    }
}
