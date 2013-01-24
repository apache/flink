/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.plan;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.operators.BinaryUnionOpDescriptor;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.pact.compiler.util.PactType;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.generic.stub.AbstractStub;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;

/**
 * The Optimizer representation of a <i>Union</i>. A Union is automatically
 * inserted before any node which has more than one incoming connection per
 * input.
 */
public class BinaryUnionNode extends TwoInputNode {
	

	public BinaryUnionNode(OptimizerNode pred1, OptimizerNode pred2) {
		super(new UnionPlaceholderContract());
		
		this.input1 = new PactConnection(pred1, this);
		this.input2 = new PactConnection(pred2, this);
		
		pred1.addOutgoingConnection(this.input1);
		pred2.addOutgoingConnection(this.input2);
	}
	
	public BinaryUnionNode(OptimizerNode pred1, OptimizerNode pred2, ShipStrategyType preSet) {
		this(pred1, pred2);
		
		if (preSet != null) {
			this.input1.setShipStrategy(preSet);
			this.input2.setShipStrategy(preSet);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Union";
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getPactType()
	 */
	@Override
	public PactType getPactType() {
		return PactType.Union;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.TwoInputNode#getPossibleProperties()
	 */
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return Collections.<OperatorDescriptorDual>singletonList(new BinaryUnionOpDescriptor());
	}
	
	public void computeUnionOfInterestingPropertiesFromSuccessors() {
		super.computeUnionOfInterestingPropertiesFromSuccessors();
		// clear all local properties, as they are destroyed anyways
		getInterestingProperties().getLocalProperties().clear();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readStubAnnotations()
	 */
	@Override
	protected void readStubAnnotations() {
		//DO NOTHING
		//do not read annotations for union nodes, as this node is artificially generated
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readConstantAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		//DO NOTHING
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldConstant(int, int)
	 */
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		return true;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		
		
		this.estimatedNumRecords = 0;
		this.estimatedOutputSize = 0;
		
		// init estimated cardinalities with the fields of the first input
		// remove the field which are unknown for other inputs later on
		for (FieldSet fieldSet : getIncomingConnections().get(0).getSourcePact().getEstimatedCardinalities().keySet()) {
			this.estimatedCardinality.put(fieldSet, -1L);
		}
		
		
		for (PactConnection inConn : getIncomingConnections()) {
			
			OptimizerNode inputPact = inConn.getSourcePact();
			
			// sum up estimatedNumRecords for inputs
			long estimatedNumRecordForInput = inputPact.estimatedNumRecords;
			
			if (estimatedNumRecordForInput != -1 && this.estimatedNumRecords != -1) {
				this.estimatedNumRecords += estimatedNumRecordForInput;
			}
			else {
				this.estimatedNumRecords = -1;
			}
			
			// sum up estimatedOutputSize for inputs
			long estimatedOutputSizeForInput = inputPact.estimatedOutputSize;
			
			if (estimatedOutputSizeForInput != -1 && this.estimatedOutputSize != -1) {
				this.estimatedOutputSize += estimatedOutputSizeForInput;
			}
			else {
				this.estimatedOutputSize = -1;
			}
			
			
			//sum up cardinalities or remove them if they are unknown
			Set<FieldSet> toRemove = new HashSet<FieldSet>();
			
			for (Entry<FieldSet, Long> cardinality : this.estimatedCardinality.entrySet()) {
				long inputCard = inputPact.getEstimatedCardinality(cardinality.getKey());
				if (inputCard == -1) {
					toRemove.add(cardinality.getKey());
				}
				else {
					//to be conservative for joins we use the max for new column cardinality
					inputCard = Math.max(inputCard, cardinality.getValue());
					cardinality.setValue(inputCard);
				}
			}
			
			this.estimatedCardinality.keySet().removeAll(toRemove);
			
		}
		
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeNumberOfStubCalls()
	 */
	@Override
	protected long computeNumberOfStubCalls() {
		return this.estimatedNumRecords;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeNumberOfStubCalls()
	 */
	@Override
	protected double computeAverageRecordWidth() {
		if (this.estimatedNumRecords == -1 || this.estimatedOutputSize == -1) return -1;
		
		final double width = this.estimatedOutputSize / (double) this.estimatedNumRecords;

		// a record must have at least one byte...
		if(width < 1) {
			return 1;
		} else {
			return width;
		}
	}
	
	// ------------------------------------------------------------------------
	//  Mock classes that represents a contract without behavior.
	// ------------------------------------------------------------------------
	
	private static final class MockStub extends AbstractStub {}
	
	private static final class UnionPlaceholderContract extends DualInputContract<MockStub>
	{
		private UnionPlaceholderContract() {
			super(MockStub.class, "UnionPlaceholderContract");
		}
	}
}
