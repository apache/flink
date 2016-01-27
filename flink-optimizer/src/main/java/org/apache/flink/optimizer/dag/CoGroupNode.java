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

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.CoGroupDescriptor;
import org.apache.flink.optimizer.operators.CoGroupWithSolutionSetFirstDescriptor;
import org.apache.flink.optimizer.operators.CoGroupWithSolutionSetSecondDescriptor;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.api.common.operators.util.FieldSet;

/**
 * The Optimizer representation of a <i>CoGroup</i> operator.
 */
public class CoGroupNode extends TwoInputNode {
	
	private List<OperatorDescriptorDual> dataProperties;
	
	public CoGroupNode(CoGroupOperatorBase<?, ?, ?, ?> operator) {
		super(operator);
		this.dataProperties = initializeDataProperties(operator.getCustomPartitioner());
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the operator for this CoGroup node.
	 * 
	 * @return The CoGroup operator.
	 */
	@Override
	public CoGroupOperatorBase<?, ?, ?, ?> getOperator() {
		return (CoGroupOperatorBase<?, ?, ?, ?>) super.getOperator();
	}

	@Override
	public String getOperatorName() {
		return "CoGroup";
	}

	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return this.dataProperties;
	}
	
	public void makeCoGroupWithSolutionSet(int solutionsetInputIndex) {
		OperatorDescriptorDual op;
		if (solutionsetInputIndex == 0) {
			op = new CoGroupWithSolutionSetFirstDescriptor(keys1, keys2);
		} else if (solutionsetInputIndex == 1) {
			op = new CoGroupWithSolutionSetSecondDescriptor(keys1, keys2);
		} else {
			throw new IllegalArgumentException();
		}
		this.dataProperties = Collections.<OperatorDescriptorDual>singletonList(op);
	}

	@Override
	protected SemanticProperties getSemanticPropertiesForLocalPropertyFiltering() {

		// Local properties for CoGroup may only be preserved on key fields.
		DualInputSemanticProperties origProps = ((DualInputOperator<?, ?, ?, ?>) getOperator()).getSemanticProperties();

		DualInputSemanticProperties filteredProps = new DualInputSemanticProperties();
		FieldSet readSet1 = origProps.getReadFields(0);
		FieldSet readSet2 = origProps.getReadFields(1);
		if(readSet1 != null) {
			filteredProps.addReadFields(0, readSet1);
		}
		if(readSet2 != null) {
			filteredProps.addReadFields(1, readSet2);
		}

		// preserve only key fields (first input)
		for(int f : this.keys1) {
			FieldSet targets = origProps.getForwardingTargetFields(0, f);
			for(int t : targets) {
				filteredProps.addForwardedField(0, f, t);
			}
		}

		// preserve only key fields (second input)
		for(int f : this.keys2) {
			FieldSet targets = origProps.getForwardingTargetFields(1, f);
			for(int t : targets) {
				filteredProps.addForwardedField(1, f, t);
			}
		}

		return filteredProps;
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// for CoGroup, we currently make no reasonable default estimates
	}
	
	private List<OperatorDescriptorDual> initializeDataProperties(Partitioner<?> customPartitioner) {
		Ordering groupOrder1 = null;
		Ordering groupOrder2 = null;
		
		CoGroupOperatorBase<?, ?, ?, ?> cgc = getOperator();
		groupOrder1 = cgc.getGroupOrderForInputOne();
		groupOrder2 = cgc.getGroupOrderForInputTwo();
			
		if (groupOrder1 != null && groupOrder1.getNumberOfFields() == 0) {
			groupOrder1 = null;
		}
		if (groupOrder2 != null && groupOrder2.getNumberOfFields() == 0) {
			groupOrder2 = null;
		}
		
		CoGroupDescriptor descr = new CoGroupDescriptor(this.keys1, this.keys2, groupOrder1, groupOrder2);
		if (customPartitioner != null) {
			descr.setCustomPartitioner(customPartitioner);
		}
		
		return Collections.<OperatorDescriptorDual>singletonList(descr);
	}
}
