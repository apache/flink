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
import org.apache.flink.api.common.operators.base.InnerJoinOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.operators.AbstractJoinDescriptor;
import org.apache.flink.optimizer.operators.HashJoinBuildFirstProperties;
import org.apache.flink.optimizer.operators.HashJoinBuildSecondProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.operators.SortMergeInnerJoinDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The Optimizer representation of a join operator. */
public class JoinNode extends TwoInputNode {

    private List<OperatorDescriptorDual> dataProperties;

    /**
     * Creates a new JoinNode for the given join operator.
     *
     * @param joinOperatorBase The join operator object.
     */
    public JoinNode(InnerJoinOperatorBase<?, ?, ?, ?> joinOperatorBase) {
        super(joinOperatorBase);

        this.dataProperties =
                getDataProperties(
                        joinOperatorBase,
                        joinOperatorBase.getJoinHint(),
                        joinOperatorBase.getCustomPartitioner());
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the contract object for this match node.
     *
     * @return The contract.
     */
    @Override
    public InnerJoinOperatorBase<?, ?, ?, ?> getOperator() {
        return (InnerJoinOperatorBase<?, ?, ?, ?>) super.getOperator();
    }

    @Override
    public String getOperatorName() {
        return "Join";
    }

    @Override
    protected List<OperatorDescriptorDual> getPossibleProperties() {
        return this.dataProperties;
    }

    public void makeJoinWithSolutionSet(int solutionsetInputIndex) {
        OperatorDescriptorDual op;
        if (solutionsetInputIndex == 0) {
            op = new HashJoinBuildFirstProperties(this.keys1, this.keys2);
        } else if (solutionsetInputIndex == 1) {
            op = new HashJoinBuildSecondProperties(this.keys1, this.keys2);
        } else {
            throw new IllegalArgumentException();
        }

        this.dataProperties = Collections.singletonList(op);
    }

    /**
     * The default estimates build on the principle of inclusion: The smaller input key domain is
     * included in the larger input key domain. We also assume that every key from the larger input
     * has one join partner in the smaller input. The result cardinality is hence the larger one.
     */
    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
        long card2 = getSecondPredecessorNode().getEstimatedNumRecords();
        this.estimatedNumRecords = (card1 < 0 || card2 < 0) ? -1 : Math.max(card1, card2);

        if (this.estimatedNumRecords >= 0) {
            float width1 = getFirstPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
            float width2 = getSecondPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
            float width = (width1 <= 0 || width2 <= 0) ? -1 : width1 + width2;

            if (width > 0) {
                this.estimatedOutputSize = (long) (width * this.estimatedNumRecords);
            }
        }
    }

    private List<OperatorDescriptorDual> getDataProperties(
            InnerJoinOperatorBase<?, ?, ?, ?> joinOperatorBase,
            JoinHint joinHint,
            Partitioner<?> customPartitioner) {
        // see if an internal hint dictates the strategy to use
        Configuration conf = joinOperatorBase.getParameters();
        String localStrategy = conf.getString(Optimizer.HINT_LOCAL_STRATEGY, null);

        if (localStrategy != null) {
            final AbstractJoinDescriptor fixedDriverStrat;
            if (Optimizer.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE.equals(localStrategy)
                    || Optimizer.HINT_LOCAL_STRATEGY_SORT_FIRST_MERGE.equals(localStrategy)
                    || Optimizer.HINT_LOCAL_STRATEGY_SORT_SECOND_MERGE.equals(localStrategy)
                    || Optimizer.HINT_LOCAL_STRATEGY_MERGE.equals(localStrategy)) {
                fixedDriverStrat = new SortMergeInnerJoinDescriptor(this.keys1, this.keys2);
            } else if (Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST.equals(localStrategy)) {
                fixedDriverStrat = new HashJoinBuildFirstProperties(this.keys1, this.keys2);
            } else if (Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND.equals(localStrategy)) {
                fixedDriverStrat = new HashJoinBuildSecondProperties(this.keys1, this.keys2);
            } else {
                throw new CompilerException(
                        "Invalid local strategy hint for match contract: " + localStrategy);
            }

            if (customPartitioner != null) {
                fixedDriverStrat.setCustomPartitioner(customPartitioner);
            }

            ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
            list.add(fixedDriverStrat);
            return list;
        } else {
            ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();

            joinHint = joinHint == null ? JoinHint.OPTIMIZER_CHOOSES : joinHint;

            switch (joinHint) {
                case BROADCAST_HASH_FIRST:
                    list.add(
                            new HashJoinBuildFirstProperties(
                                    this.keys1, this.keys2, true, false, false));
                    break;
                case BROADCAST_HASH_SECOND:
                    list.add(
                            new HashJoinBuildSecondProperties(
                                    this.keys1, this.keys2, false, true, false));
                    break;
                case REPARTITION_HASH_FIRST:
                    list.add(
                            new HashJoinBuildFirstProperties(
                                    this.keys1, this.keys2, false, false, true));
                    break;
                case REPARTITION_HASH_SECOND:
                    list.add(
                            new HashJoinBuildSecondProperties(
                                    this.keys1, this.keys2, false, false, true));
                    break;
                case REPARTITION_SORT_MERGE:
                    list.add(
                            new SortMergeInnerJoinDescriptor(
                                    this.keys1, this.keys2, false, false, true));
                    break;
                case OPTIMIZER_CHOOSES:
                    list.add(new SortMergeInnerJoinDescriptor(this.keys1, this.keys2));
                    list.add(new HashJoinBuildFirstProperties(this.keys1, this.keys2));
                    list.add(new HashJoinBuildSecondProperties(this.keys1, this.keys2));
                    break;
                default:
                    throw new CompilerException("Unrecognized join hint: " + joinHint);
            }

            if (customPartitioner != null) {
                for (OperatorDescriptorDual descr : list) {
                    ((AbstractJoinDescriptor) descr).setCustomPartitioner(customPartitioner);
                }
            }

            return list;
        }
    }
}
