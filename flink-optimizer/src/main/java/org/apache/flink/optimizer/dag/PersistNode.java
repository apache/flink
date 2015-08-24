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
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;
import org.apache.flink.optimizer.operators.PersistDescriptor;

import java.util.Collections;
import java.util.List;

/**
 * The optimizer's internal representation of a <i>Persist</i> node.
 */
public class PersistNode extends SingleInputNode {

	private final List<OperatorDescriptorSingle> possibleProperties;

	/**
	 * Creates a new PersistNode for the given operator.
	 *
	 * @param operator The persist operator.
	 */
	public PersistNode(SingleInputOperator<?, ?, ?> operator) {
		super(operator);

		this.possibleProperties = Collections.<OperatorDescriptorSingle>singletonList(new PersistDescriptor());
	}

	@Override
	public String getName() {
		return "Persist";
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return this.possibleProperties;
	}

	@Override
	public SemanticProperties getSemanticProperties() {
		return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
	}

	/**
	 * Computes the estimates for the Persist operator.
	 */
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
	}
}
