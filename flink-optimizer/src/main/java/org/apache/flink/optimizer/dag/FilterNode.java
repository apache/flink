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

import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.FilterDescriptor;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;

import java.util.Collections;
import java.util.List;

/** The optimizer's internal representation of a <i>Filter</i> operator node. */
public class FilterNode extends SingleInputNode {

    private final List<OperatorDescriptorSingle> possibleProperties;

    public FilterNode(FilterOperatorBase<?, ?> operator) {
        super(operator);
        this.possibleProperties =
                Collections.<OperatorDescriptorSingle>singletonList(new FilterDescriptor());
    }

    @Override
    public FilterOperatorBase<?, ?> getOperator() {
        return (FilterOperatorBase<?, ?>) super.getOperator();
    }

    @Override
    public String getOperatorName() {
        return "Filter";
    }

    @Override
    public SemanticProperties getSemanticProperties() {
        return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
    }

    @Override
    protected List<OperatorDescriptorSingle> getPossibleProperties() {
        return this.possibleProperties;
    }

    /**
     * Computes the estimates for the Filter operator. Since it applies a filter on the data we
     * assume a cardinality decrease. To give the system a hint at data decrease, we use a default
     * magic number to indicate a 0.5 decrease.
     */
    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        this.estimatedNumRecords = (long) (getPredecessorNode().getEstimatedNumRecords() * 0.5);
        this.estimatedOutputSize = (long) (getPredecessorNode().getEstimatedOutputSize() * 0.5);
    }
}
