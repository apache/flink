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

import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.AllReduceProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;
import org.apache.flink.optimizer.operators.ReduceProperties;
import org.apache.flink.runtime.operators.DriverStrategy;

import java.util.Collections;
import java.util.List;

/** The Optimizer representation of a <i>Reduce</i> operator. */
public class ReduceNode extends SingleInputNode {

    private final List<OperatorDescriptorSingle> possibleProperties;

    private ReduceNode preReduceUtilityNode;

    public ReduceNode(ReduceOperatorBase<?, ?> operator) {
        super(operator);

        if (this.keys == null) {
            // case of a key-less reducer. force a parallelism of 1
            setParallelism(1);
        }

        OperatorDescriptorSingle props;

        if (this.keys == null) {
            props = new AllReduceProperties();
        } else {
            DriverStrategy combinerStrategy;
            switch (operator.getCombineHint()) {
                case OPTIMIZER_CHOOSES:
                    combinerStrategy = DriverStrategy.SORTED_PARTIAL_REDUCE;
                    break;
                case SORT:
                    combinerStrategy = DriverStrategy.SORTED_PARTIAL_REDUCE;
                    break;
                case HASH:
                    combinerStrategy = DriverStrategy.HASHED_PARTIAL_REDUCE;
                    break;
                case NONE:
                    combinerStrategy = DriverStrategy.NONE;
                    break;
                default:
                    throw new RuntimeException("Unknown CombineHint");
            }
            props =
                    new ReduceProperties(
                            this.keys, operator.getCustomPartitioner(), combinerStrategy);
        }

        this.possibleProperties = Collections.singletonList(props);
    }

    public ReduceNode(ReduceNode reducerToCopyForCombiner) {
        super(reducerToCopyForCombiner);

        this.possibleProperties = Collections.emptyList();
    }

    // ------------------------------------------------------------------------

    @Override
    public ReduceOperatorBase<?, ?> getOperator() {
        return (ReduceOperatorBase<?, ?>) super.getOperator();
    }

    @Override
    public String getOperatorName() {
        return "Reduce";
    }

    @Override
    protected List<OperatorDescriptorSingle> getPossibleProperties() {
        return this.possibleProperties;
    }

    // --------------------------------------------------------------------------------------------
    //  Estimates
    // --------------------------------------------------------------------------------------------

    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        // no real estimates possible for a reducer.
    }

    public ReduceNode getCombinerUtilityNode() {
        if (this.preReduceUtilityNode == null) {
            this.preReduceUtilityNode = new ReduceNode(this);

            // we conservatively assume the combiner returns the same data size as it consumes
            this.preReduceUtilityNode.estimatedOutputSize =
                    getPredecessorNode().getEstimatedOutputSize();
            this.preReduceUtilityNode.estimatedNumRecords =
                    getPredecessorNode().getEstimatedNumRecords();
        }
        return this.preReduceUtilityNode;
    }
}
