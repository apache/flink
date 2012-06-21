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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a single input.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class SingleInputNode extends OptimizerNode {

	// ------------- Node Connection
	
	protected PactConnection inConn = null; // the input of the node
	
	// ------------- Optimizer Cache
	
	private List<OptimizerNode> cachedPlans; // a cache for the computed alternative plans

	// ------------- Stub Annotations
	
	protected FieldSet constantSet; // set of fields that are left unchanged by the stub
	protected FieldSet notConstantSet; // set of fields that are changed by the stub
	
	protected FieldList keyList; // The set of key fields (order is relevant!)

	// ------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public SingleInputNode(SingleInputContract<?> pactContract) {
		super(pactContract);
		this.keyList = new FieldList(pactContract.getKeyColumnNumbers(0));
	}

	/**
	 * Copy constructor to create a copy of a node with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param predNode
	 *        The new predecessor.
	 * @param inConn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected SingleInputNode(SingleInputNode template, OptimizerNode predNode, PactConnection inConn,
			GlobalProperties globalProps, LocalProperties localProps) {
		super(template, globalProps, localProps);

		// copy annotations
		this.constantSet = template.constantSet;
		
		// copy key set
		this.keyList = template.keyList;
		
		// copy input connection
		this.inConn = new PactConnection(inConn, predNode, this);
		
		// copy the child's branch-plan map
		if(predNode.branchPlan != null && predNode.branchPlan.size() > 0) {
			this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>(predNode.branchPlan);
		} else {
			this.branchPlan = null;
		}

	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @return The input connection.
	 */
	public PactConnection getInConn() {
		return this.inConn;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @param conn
	 *        The input connection to set.
	 */
	public void setInConn(PactConnection inConn) {
		this.inConn = inConn;
	}
	
	/**
	 * Gets the predecessor of this node.
	 * 
	 * @return The predecessor of this node. 
	 */
	public OptimizerNode getPredNode() {
		if(this.inConn != null) {
			return this.inConn.getSourcePact();
		} else {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.singletonList(this.inConn);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) throws CompilerException {
		// get the predecessor node
		List<Contract> children = ((SingleInputContract<?>) getPactContract()).getInputs();
		
		for(Contract child : children) {
			OptimizerNode predNode = contractToNode.get(child);
	
			// create a connection
			PactConnection conn = new PactConnection(predNode, this);
			this.setInConn(conn);
			predNode.addOutConn(conn);
	
			// see if an internal hint dictates the strategy to use
			Configuration conf = getPactContract().getParameters();
			String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
			if (shipStrategy != null) {
				if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
					conn.setShipStrategy(ShipStrategy.FORWARD);
				} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
					conn.setShipStrategy(ShipStrategy.PARTITION_HASH);
				} else {
					throw new CompilerException("Invalid hint for the shipping strategy of a single input connection: "
						+ shipStrategy);
				}
			}
		}
	}

	// ----------------- Recursive Optimization
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans()
	 */
	@Override
	final public List<OptimizerNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// calculate alternative subplans for predecessor
		List<? extends OptimizerNode> subPlans = this.getPredNode().getAlternativePlans(estimator);  

		List<OptimizerNode> outputPlans = new ArrayList<OptimizerNode>();

		computeValidPlanAlternatives(subPlans, estimator,  outputPlans);
		
		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (this.getOutConns() != null && this.getOutConns().size() > 1) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}
	
	/**
	 * Takes a list with all subplans and produces alternative plans for the current node
	 *  
	 * @param altSubPlans	Alternative subplans
	 * @param estimator		Cost estimator to be used
	 * @param outputPlans	The generated output plans
	 */
	protected abstract void computeValidPlanAlternatives(List<? extends OptimizerNode> altSubPlans, 
			CostEstimator estimator, List<OptimizerNode> outputPlans);
	
	// -------------------- Branch Handling
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		List<UnclosedBranchDescriptor> result = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge of lists is really necessary
		result = mergeLists(result, this.getPredNode().getBranchesForParent(this)); 
			
		this.openBranches = result;
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor
	 * )
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		boolean descend = visitor.preVisit(this);

		if (descend) {
			
			if(this.getPredNode() != null) {
				this.getPredNode().accept(visitor);
			}
			
			visitor.postVisit(this);
		}
	}
	
	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.OptimizerNode}.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setCosts(eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		
		// check, if this node has no branch beneath it, no double-counted cost then
		if (this.lastJoinedBranchNode == null) {
			return;
		} else {
			// TODO: revisit branch handling
			throw new CompilerException("SingleInputNode should not have a branch node");
		}

	}
	
	// ------------------------- Estimate Computation
	
	/**
	 * Computes the width of output records.
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		
		CompilerHints hints = getPactContract().getCompilerHints();
		
		// use hint if available
		if(hints != null && hints.getAvgBytesPerRecord() != -1) {
			return hints.getAvgBytesPerRecord();
		}

		// compute width from output size and cardinality
		final long numRecords = computeNumberOfStubCalls();
		
		long outputSize = 0;
		if(this.getPredNode() != null) {
			outputSize = this.getPredNode().estimatedOutputSize;
		}
		
		// compute width only if we have information
		if(numRecords == -1 || outputSize == -1)
			return -1;
		
		final double width = outputSize / (double)numRecords;

		// a record must have at least one byte...
		if(width < 1)
			return 1;
		else 
			return width;
	}
	
	// -------------------- Operator Properties
	
	public boolean isFieldKept(int input, int fieldNumber) {
		
		if (input != 0) {
			throw new IndexOutOfBoundsException();
		}
		
		if (this.constantSet == null) {
			if (this.notConstantSet == null) {
				return false;
			}
			return this.notConstantSet.contains(fieldNumber) == false;
		}
		
		return this.constantSet.contains(fieldNumber);
	}
		
	public FieldList getKeySet() {
		return this.keyList;
	}
	
	// --------------------------- Stub Annotation Handling
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		
		SingleInputContract<?> c = (SingleInputContract<?>)super.getPactContract();
		
		// get constantSet annotation from stub
		ConstantFields constantSet = c.getUserCodeClass().getAnnotation(ConstantFields.class);
		
		// extract constantSet from annotation
		if(constantSet == null) {
			this.constantSet = null;
		} else {
			this.constantSet = new FieldSet(constantSet.fields());
		}
		
		ConstantFieldsExcept notConstantSet = c.getUserCodeClass().getAnnotation(ConstantFieldsExcept.class);
		
		// extract notConstantSet from annotation
		if(notConstantSet == null) {
			this.notConstantSet = null;
		} else {
			this.notConstantSet = new FieldSet(notConstantSet.fields());
		}
		
		if (this.notConstantSet != null && this.constantSet != null) {
			throw new CompilerException("Either ConstantFields or ConstantFieldsExcept can be specified, not both.");
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getReadSet(int)
	 */
	@Override
	public FieldSet getConstantSet(int input) {
		
		if(input < -1 || input > 0)
			throw new IndexOutOfBoundsException();
		
		return this.constantSet;
	}

	
}
