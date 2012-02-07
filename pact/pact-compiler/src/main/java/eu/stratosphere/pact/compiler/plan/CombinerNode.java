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
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class CombinerNode extends OptimizerNode {
	private PactConnection input;

	public CombinerNode(ReduceContract reducer, OptimizerNode predecessor, float reducingFactor) {
		super(reducer);

		this.input = new PactConnection(predecessor, this, ShipStrategy.FORWARD);
		this.setLocalStrategy(LocalStrategy.COMBININGSORT);

		this.globalProps = predecessor.globalProps;
		this.localProps = predecessor.localProps;

		this.setDegreeOfParallelism(predecessor.getDegreeOfParallelism());
		this.setInstancesPerMachine(predecessor.getInstancesPerMachine());

		// set the estimates
		this.estimatedCardinality.putAll(predecessor.estimatedCardinality);
		
		long estKeyCard = getEstimatedCardinality(new FieldSet(getPactContract().getKeyColumnNumbers(0)));

		if (predecessor.estimatedNumRecords >= 1 && estKeyCard >= 1
			&& predecessor.estimatedOutputSize >= -1) {
			this.estimatedNumRecords = (long) (predecessor.estimatedNumRecords * reducingFactor);
			this.estimatedOutputSize = (long) (predecessor.estimatedOutputSize * reducingFactor);
		} else {
			this.estimatedNumRecords = predecessor.estimatedNumRecords;
			this.estimatedOutputSize = predecessor.estimatedOutputSize;
		}
		
		// copy the child's branch-plan map
		if (this.branchPlan == null) {
			this.branchPlan = predecessor.branchPlan;
		} else if (predecessor.branchPlan != null) {
			this.branchPlan.putAll(predecessor.branchPlan);
		}
	}

	@Override
	public String getName() {
		return "Combine";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case COMBININGSORT: return 1;
			default:	        return 0;
		}
	}
	
	@Override
	public ReduceContract getPactContract() {
		return (ReduceContract) super.getPactContract();
	}

	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<List<PactConnection>> getIncomingConnections() {
		return Collections.singletonList(Collections.singletonList(this.input));
	}

	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void computeUnclosedBranchStack() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<? extends OptimizerNode> getAlternativePlans(CostEstimator estimator) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input.getSourcePact().accept(visitor);
			visitor.postVisit(this);
		}
	}

	public boolean isFieldKept(int input, int fieldNumber) {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readCopyProjectionAnnotations()
	 */
	@Override
	protected void readCopyProjectionAnnotations() {
		// DO NOTHING		
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readReadsAnnotation() {
		// DO NOTHING
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#deriveOutputSchema()
	 */
	@Override
	public void deriveOutputSchema() {
		// DataSink has no output
		// DO NOTHING
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int)
	 */
	@Override
	public int[] getWriteSet(int input) {
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getReadSet(int)
	 */
	@Override
	public int[] getReadSet(int input) {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputSchema(java.util.List)
	 */
	@Override
	public int[] computeOutputSchema(List<int[]> inputSchemas) {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int, java.util.List)
	 */
	@Override
	public int[] getWriteSet(int input, List<int[]> inputSchemas) {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isValidInputSchema(int, int[])
	 */
	@Override
	public boolean isValidInputSchema(int input, int[] inputSchema) {
		return false;
	}
}
