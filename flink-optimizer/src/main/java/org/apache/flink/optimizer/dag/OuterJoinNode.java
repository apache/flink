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
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase;
import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase.OuterJoinType;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.AbstractJoinDescriptor;
import org.apache.flink.optimizer.operators.HashFullOuterJoinBuildFirstDescriptor;
import org.apache.flink.optimizer.operators.HashFullOuterJoinBuildSecondDescriptor;
import org.apache.flink.optimizer.operators.HashLeftOuterJoinBuildFirstDescriptor;
import org.apache.flink.optimizer.operators.HashLeftOuterJoinBuildSecondDescriptor;
import org.apache.flink.optimizer.operators.HashRightOuterJoinBuildFirstDescriptor;
import org.apache.flink.optimizer.operators.HashRightOuterJoinBuildSecondDescriptor;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.operators.SortMergeFullOuterJoinDescriptor;
import org.apache.flink.optimizer.operators.SortMergeLeftOuterJoinDescriptor;
import org.apache.flink.optimizer.operators.SortMergeRightOuterJoinDescriptor;

import java.util.ArrayList;
import java.util.List;

public class OuterJoinNode extends TwoInputNode {

    private List<OperatorDescriptorDual> dataProperties;

    /**
     * Creates a new two input node for the optimizer plan, representing the given operator.
     *
     * @param operator The operator that the optimizer DAG node should represent.
     */
    public OuterJoinNode(OuterJoinOperatorBase<?, ?, ?, ?> operator) {
        super(operator);

        this.dataProperties = getDataProperties();
    }

    private List<OperatorDescriptorDual> getDataProperties() {
        OuterJoinOperatorBase<?, ?, ?, ?> operator = getOperator();

        OuterJoinType type = operator.getOuterJoinType();

        JoinHint joinHint = operator.getJoinHint();
        joinHint = joinHint == null ? JoinHint.OPTIMIZER_CHOOSES : joinHint;

        List<OperatorDescriptorDual> list;
        switch (type) {
            case LEFT:
                list = createLeftOuterJoinDescriptors(joinHint);
                break;
            case RIGHT:
                list = createRightOuterJoinDescriptors(joinHint);
                break;
            case FULL:
                list = createFullOuterJoinDescriptors(joinHint);
                break;
            default:
                throw new CompilerException("Unknown outer join type: " + type);
        }

        Partitioner<?> customPartitioner = operator.getCustomPartitioner();
        if (customPartitioner != null) {
            for (OperatorDescriptorDual desc : list) {
                ((AbstractJoinDescriptor) desc).setCustomPartitioner(customPartitioner);
            }
        }
        return list;
    }

    private List<OperatorDescriptorDual> createLeftOuterJoinDescriptors(JoinHint hint) {

        List<OperatorDescriptorDual> list = new ArrayList<>();
        switch (hint) {
            case OPTIMIZER_CHOOSES:
                list.add(new SortMergeLeftOuterJoinDescriptor(this.keys1, this.keys2, true));
                list.add(
                        new HashLeftOuterJoinBuildSecondDescriptor(
                                this.keys1, this.keys2, true, true));
                break;
            case REPARTITION_SORT_MERGE:
                list.add(new SortMergeLeftOuterJoinDescriptor(this.keys1, this.keys2, false));
                break;
            case REPARTITION_HASH_SECOND:
                list.add(
                        new HashLeftOuterJoinBuildSecondDescriptor(
                                this.keys1, this.keys2, false, true));
                break;
            case BROADCAST_HASH_SECOND:
                list.add(
                        new HashLeftOuterJoinBuildSecondDescriptor(
                                this.keys1, this.keys2, true, false));
                break;
            case REPARTITION_HASH_FIRST:
                list.add(
                        new HashLeftOuterJoinBuildFirstDescriptor(
                                this.keys1, this.keys2, false, true));
                break;
            case BROADCAST_HASH_FIRST:
            default:
                throw new CompilerException("Invalid join hint: " + hint + " for left outer join");
        }
        return list;
    }

    private List<OperatorDescriptorDual> createRightOuterJoinDescriptors(JoinHint hint) {

        List<OperatorDescriptorDual> list = new ArrayList<>();
        switch (hint) {
            case OPTIMIZER_CHOOSES:
                list.add(new SortMergeRightOuterJoinDescriptor(this.keys1, this.keys2, true));
                list.add(
                        new HashRightOuterJoinBuildFirstDescriptor(
                                this.keys1, this.keys2, true, true));
                break;
            case REPARTITION_SORT_MERGE:
                list.add(new SortMergeRightOuterJoinDescriptor(this.keys1, this.keys2, false));
                break;
            case REPARTITION_HASH_FIRST:
                list.add(
                        new HashRightOuterJoinBuildFirstDescriptor(
                                this.keys1, this.keys2, false, true));
                break;
            case BROADCAST_HASH_FIRST:
                list.add(
                        new HashRightOuterJoinBuildFirstDescriptor(
                                this.keys1, this.keys2, true, false));
                break;
            case REPARTITION_HASH_SECOND:
                list.add(
                        new HashRightOuterJoinBuildSecondDescriptor(
                                this.keys1, this.keys2, false, true));
                break;
            case BROADCAST_HASH_SECOND:
            default:
                throw new CompilerException("Invalid join hint: " + hint + " for right outer join");
        }
        return list;
    }

    private List<OperatorDescriptorDual> createFullOuterJoinDescriptors(JoinHint hint) {

        List<OperatorDescriptorDual> list = new ArrayList<>();
        switch (hint) {
            case OPTIMIZER_CHOOSES:
                list.add(new SortMergeFullOuterJoinDescriptor(this.keys1, this.keys2));
                break;
            case REPARTITION_SORT_MERGE:
                list.add(new SortMergeFullOuterJoinDescriptor(this.keys1, this.keys2));
                break;
            case REPARTITION_HASH_FIRST:
                list.add(new HashFullOuterJoinBuildFirstDescriptor(this.keys1, this.keys2));
                break;
            case REPARTITION_HASH_SECOND:
                list.add(new HashFullOuterJoinBuildSecondDescriptor(this.keys1, this.keys2));
                break;
            case BROADCAST_HASH_FIRST:
            case BROADCAST_HASH_SECOND:
            default:
                throw new CompilerException("Invalid join hint: " + hint + " for full outer join");
        }
        return list;
    }

    @Override
    public OuterJoinOperatorBase<?, ?, ?, ?> getOperator() {
        return (OuterJoinOperatorBase<?, ?, ?, ?>) super.getOperator();
    }

    @Override
    protected List<OperatorDescriptorDual> getPossibleProperties() {
        return dataProperties;
    }

    @Override
    public String getOperatorName() {
        return "Outer Join";
    }

    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
        long card2 = getSecondPredecessorNode().getEstimatedNumRecords();

        if (card1 < 0 || card2 < 0) {
            this.estimatedNumRecords = -1;
        } else {
            this.estimatedNumRecords = Math.max(card1, card2);
        }

        if (this.estimatedNumRecords >= 0) {
            float width1 = getFirstPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
            float width2 = getSecondPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
            float width = (width1 <= 0 || width2 <= 0) ? -1 : width1 + width2;

            if (width > 0) {
                this.estimatedOutputSize = (long) (width * this.estimatedNumRecords);
            }
        }
    }
}
