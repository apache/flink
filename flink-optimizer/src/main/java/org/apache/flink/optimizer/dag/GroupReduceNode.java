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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.operators.AllGroupReduceProperties;
import org.apache.flink.optimizer.operators.AllGroupWithPartialPreGroupProperties;
import org.apache.flink.optimizer.operators.GroupReduceProperties;
import org.apache.flink.optimizer.operators.GroupReduceWithCombineProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;

import java.util.Collections;
import java.util.List;

/** The optimizer representation of a <i>GroupReduce</i> operation. */
public class GroupReduceNode extends SingleInputNode {

    private final List<OperatorDescriptorSingle> possibleProperties;

    private final String operatorName;

    private GroupReduceNode combinerUtilityNode;

    /**
     * Creates a new optimizer node for the given operator.
     *
     * @param operator The reduce operation.
     */
    public GroupReduceNode(GroupReduceOperatorBase<?, ?, ?> operator) {
        super(operator);
        this.operatorName = "GroupReduce";

        if (this.keys == null) {
            // case of a key-less reducer. force a parallelism of 1
            setParallelism(1);
        }

        this.possibleProperties = initPossibleProperties(operator.getCustomPartitioner());
    }

    private GroupReduceNode(GroupReduceNode reducerToCopyForCombiner) {
        super(reducerToCopyForCombiner);
        this.operatorName = "GroupCombine";

        this.possibleProperties = Collections.emptyList();
    }

    private List<OperatorDescriptorSingle> initPossibleProperties(
            Partitioner<?> customPartitioner) {
        // see if an internal hint dictates the strategy to use
        final Configuration conf = getOperator().getParameters();
        final String localStrategy = conf.getString(Optimizer.HINT_LOCAL_STRATEGY, null);

        final boolean useCombiner;
        if (localStrategy != null) {
            if (Optimizer.HINT_LOCAL_STRATEGY_SORT.equals(localStrategy)) {
                useCombiner = false;
            } else if (Optimizer.HINT_LOCAL_STRATEGY_COMBINING_SORT.equals(localStrategy)) {
                if (!isCombineable()) {
                    Optimizer.LOG.warn(
                            "Strategy hint for GroupReduce '"
                                    + getOperator().getName()
                                    + "' requires combinable reduce, but user function is not marked combinable.");
                }
                useCombiner = true;
            } else {
                throw new CompilerException(
                        "Invalid local strategy hint for match contract: " + localStrategy);
            }
        } else {
            useCombiner = isCombineable();
        }

        // check if we can work with a grouping (simple reducer), or if we need ordering because of
        // a group order
        Ordering groupOrder = null;
        if (getOperator() != null) {
            groupOrder = getOperator().getGroupOrder();
            if (groupOrder != null && groupOrder.getNumberOfFields() == 0) {
                groupOrder = null;
            }
        }

        OperatorDescriptorSingle props =
                useCombiner
                        ? (this.keys == null
                                ? new AllGroupWithPartialPreGroupProperties()
                                : new GroupReduceWithCombineProperties(
                                        this.keys, groupOrder, customPartitioner))
                        : (this.keys == null
                                ? new AllGroupReduceProperties()
                                : new GroupReduceProperties(
                                        this.keys, groupOrder, customPartitioner));

        return Collections.singletonList(props);
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the operator represented by this optimizer node.
     *
     * @return The operator represented by this optimizer node.
     */
    @Override
    public GroupReduceOperatorBase<?, ?, ?> getOperator() {
        return (GroupReduceOperatorBase<?, ?, ?>) super.getOperator();
    }

    /**
     * Checks, whether a combiner function has been given for the function encapsulated by this
     * reduce contract.
     *
     * @return True, if a combiner has been given, false otherwise.
     */
    public boolean isCombineable() {
        return getOperator().isCombinable();
    }

    @Override
    public String getOperatorName() {
        return this.operatorName;
    }

    @Override
    protected List<OperatorDescriptorSingle> getPossibleProperties() {
        return this.possibleProperties;
    }

    @Override
    protected SemanticProperties getSemanticPropertiesForLocalPropertyFiltering() {
        // Local properties for GroupReduce may only be preserved on key fields.
        SingleInputSemanticProperties origProps = getOperator().getSemanticProperties();
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

    public GroupReduceNode getCombinerUtilityNode() {
        if (this.combinerUtilityNode == null) {
            this.combinerUtilityNode = new GroupReduceNode(this);

            // we conservatively assume the combiner returns the same data size as it consumes
            this.combinerUtilityNode.estimatedOutputSize =
                    getPredecessorNode().getEstimatedOutputSize();
            this.combinerUtilityNode.estimatedNumRecords =
                    getPredecessorNode().getEstimatedNumRecords();
        }
        return this.combinerUtilityNode;
    }
}
