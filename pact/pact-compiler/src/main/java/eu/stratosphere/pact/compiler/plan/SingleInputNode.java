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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorSingle;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * A node in the optimizer's program representation for a PACT with a single input.
 * 
 * This class contains all the generic logic for branch handling, interesting properties,
 * and candidate plan enumeration. The subclasses for specific operators simply add logic
 * for cost estimates and specify possible strategies for their realization.
 */
public abstract class SingleInputNode extends OptimizerNode
{
	protected final FieldSet keys; 			// The set of key fields
	
	private final List<OperatorDescriptorSingle> possibleProperties;
	
	protected PactConnection inConn; 		// the input of the node
	
	private FieldSet constantSet; 			// set of fields that are left unchanged by the stub
	private FieldSet notConstantSet;		// set of fields that are changed by the stub
	
	private List<PlanNode> cachedPlans;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract The PACT that the node represents.
	 */
	protected SingleInputNode(SingleInputContract<?> pactContract) {
		super(pactContract);
		
		int[] k = pactContract.getKeyColumns(0);
		this.keys = k == null || k.length == 0 ? null : new FieldSet(k);
		
		this.possibleProperties = getPossibleProperties();
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getPactContract()
	 */
	@Override
	public SingleInputContract<?> getPactContract() {
		return (SingleInputContract<?>) super.getPactContract();
	}
	
	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @return The input connection.
	 */
	public PactConnection getIncomingConnection() {
		return this.inConn;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @param conn The input connection to set.
	 */
	public void setIncomingConnection(PactConnection inConn) {
		this.inConn = inConn;
	}
	
	/**
	 * Gets the predecessor of this node.
	 * 
	 * @return The predecessor of this node. 
	 */
	public OptimizerNode getPredecessorNode() {
		if (this.inConn != null) {
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

	/**
	 * @param fieldNumber
	 * @return
	 */
	public boolean isFieldConstant(int fieldNumber) {
		return isFieldConstant(0, fieldNumber);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldConstant(int, int)
	 */
	public boolean isFieldConstant(int input, int fieldNumber) {
		if (input != 0) {
			throw new IndexOutOfBoundsException();
		}
		if (this.constantSet == null) {
			return this.notConstantSet == null ? false : !this.notConstantSet.contains(fieldNumber);
		} else {
			return this.constantSet.contains(fieldNumber);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) throws CompilerException {
		// see if an internal hint dictates the strategy to use
		final Configuration conf = getPactContract().getParameters();
		final String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
		final ShipStrategyType preSet;
		
		if (shipStrategy != null) {
			if (shipStrategy.equalsIgnoreCase(PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH)) {
				preSet = ShipStrategyType.PARTITION_HASH;
			} else if (shipStrategy.equalsIgnoreCase(PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_RANGE)) {
				preSet = ShipStrategyType.PARTITION_RANGE;
			} else if (shipStrategy.equalsIgnoreCase(PactCompiler.HINT_SHIP_STRATEGY_FORWARD)) {
				preSet = ShipStrategyType.FORWARD;
			} else if (shipStrategy.equalsIgnoreCase(PactCompiler.HINT_SHIP_STRATEGY_REPARTITION)) {
				preSet = ShipStrategyType.PARTITION_RANDOM;
			} else {
				throw new CompilerException("Unrecognized ship strategy hint: " + shipStrategy);
			}
		} else {
			preSet = null;
		}
		
		// get the predecessor node
		List<Contract> children = ((SingleInputContract<?>) getPactContract()).getInputs();
		
		OptimizerNode pred;
		PactConnection conn;
		if (children.size() == 1) {
			pred = contractToNode.get(children.get(0));
			conn = new PactConnection(pred, this);
			if (preSet != null) {
				conn.setShipStrategy(preSet);
			}
		} else {
			pred = createdUnionCascade(children, contractToNode, preSet);
			conn = new PactConnection(pred, this);
			conn.setShipStrategy(ShipStrategyType.FORWARD);
		}
		// create the connection and add it
		setIncomingConnection(conn);
		pred.addOutgoingConnection(conn);
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Properties and Optimization
	// --------------------------------------------------------------------------------------------
	
	
	
	protected abstract List<OperatorDescriptorSingle> getPossibleProperties();
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		for (OperatorDescriptorSingle dps : this.possibleProperties) {
			if (dps.getStrategy().firstDam().isMaterializing()) {
				return true;
			}
			for (RequestedLocalProperties rlp : dps.getPossibleLocalProperties()) {
				if (!rlp.isTrivial()) {
					return true;
				}
			}
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// get what we inherit and what is preserved by our user code 
		final InterestingProperties props = getInterestingProperties().filterByCodeAnnotations(this, 0);
		
		// add all properties relevant to this node
		for (OperatorDescriptorSingle dps : this.possibleProperties) {
			for (RequestedGlobalProperties gp : dps.getPossibleGlobalProperties()) {
				props.addGlobalProperties(gp);
			}
			for (RequestedLocalProperties lp : dps.getPossibleLocalProperties()) {
				props.addLocalProperties(lp);
			}
		}
		this.inConn.setInterestingProperties(props);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// calculate alternative sub-plans for predecessor
		final List<? extends PlanNode> subPlans = getPredecessorNode().getAlternativePlans(estimator);
		final Set<RequestedGlobalProperties> intGlobal = this.inConn.getInterestingProperties().getGlobalProperties();
		
		final RequestedGlobalProperties[] allValidGlobals;
		{
			Set<RequestedGlobalProperties> pairs = new HashSet<RequestedGlobalProperties>();
			for (OperatorDescriptorSingle ods : this.possibleProperties) {
				pairs.addAll(ods.getPossibleGlobalProperties());
			}
			allValidGlobals = (RequestedGlobalProperties[]) pairs.toArray(new RequestedGlobalProperties[pairs.size()]);
		}
		final ArrayList<PlanNode> outputPlans = new ArrayList<PlanNode>();
		
		final int dop = getDegreeOfParallelism();
		final int subPerInstance = getSubtasksPerInstance();
		final int inDop = getPredecessorNode().getDegreeOfParallelism();
		final int inSubPerInstance = getPredecessorNode().getSubtasksPerInstance();
		final int numInstances = dop / subPerInstance + (dop % subPerInstance == 0 ? 0 : 1);
		final int inNumInstances = inDop / inSubPerInstance + (inDop % inSubPerInstance == 0 ? 0 : 1);
		
		final boolean globalDopChange = numInstances != inNumInstances;
		final boolean localDopChange = numInstances == inNumInstances & subPerInstance != inSubPerInstance;
		
		// create all candidates
		for (PlanNode child : subPlans) {
			if (this.inConn.getShipStrategy() == null) {
				// pick the strategy ourselves
				for (RequestedGlobalProperties igps: intGlobal) {
					final Channel c = new Channel(child);
					igps.parameterizeChannel(c, globalDopChange, localDopChange);
					
					// if the DOP changed, make sure that we cancel out properties, unless the
					// ship strategy preserves/establishes them even under changing DOPs
					if (globalDopChange && !c.getShipStrategy().isNetworkStrategy()) {
						c.getGlobalProperties().reset();
					}
					if (localDopChange && !(c.getShipStrategy().isNetworkStrategy() || 
								c.getShipStrategy().compensatesForLocalDOPChanges())) {
						c.getGlobalProperties().reset();
					}
					
					// check whether we meet any of the accepted properties
					// we may remove this check, when we do a check to not inherit
					// requested global properties that are incompatible with all possible
					// requested properties
					for (RequestedGlobalProperties rgps: allValidGlobals) {
						if (rgps.isMetBy(c.getGlobalProperties())) {
							addLocalCandidates(c, outputPlans);
							break;
						}
					}
				}
			} else {
				// hint fixed the strategy
				final Channel c = new Channel(child);
				if (this.keys != null) {
					c.setShipStrategy(this.inConn.getShipStrategy(), this.keys.toFieldList());
				} else {
					c.setShipStrategy(this.inConn.getShipStrategy());
				}
				
				if (globalDopChange) {
					c.adjustGlobalPropertiesForFullParallelismChange();
				} else if (localDopChange) {
					c.adjustGlobalPropertiesForLocalParallelismChange();
				}
				
				// check whether we meet any of the accepted properties
				for (RequestedGlobalProperties rgps: allValidGlobals) {
					if (rgps.isMetBy(c.getGlobalProperties())) {
						addLocalCandidates(c, outputPlans);
						break;
					}
				}
			}
		}
		
		// cost and prune the plans
		for (PlanNode node : outputPlans) {
			estimator.costOperator(node);
		}
		prunePlanAlternatives(outputPlans);
		outputPlans.trimToSize();

		this.cachedPlans = outputPlans;
		return outputPlans;
	}
	
	private void addLocalCandidates(Channel template, List<PlanNode> target)
	{
		final LocalProperties lp = template.getLocalPropertiesAfterShippingOnly();
		for (RequestedLocalProperties ilp : this.inConn.getInterestingProperties().getLocalProperties()) {
			final Channel in = template.clone();
			if (ilp.isMetBy(lp)) {
				in.setLocalStrategy(LocalStrategy.NONE);
			} else {
				ilp.parameterizeChannel(in);
			}
			
			// instantiate a candidate, if the instantiated local properties meet one possible local property set
			for (OperatorDescriptorSingle dps: this.possibleProperties) {
				for (RequestedLocalProperties ilps : dps.getPossibleLocalProperties()) {
					if (ilps.isMetBy(in.getLocalProperties())) {
						final SingleInputPlanNode node = dps.instantiate(in, this);
						final GlobalProperties gProps = node.getGlobalProperties();
						final LocalProperties lProps = node.getLocalProperties();
						dps.processPropertiesByStrategy(gProps, lProps);
						gProps.filterByNodesConstantSet(this, 0);
						lProps.filterByNodesConstantSet(this, 0);
						node.updatePropertiesWithUniqueSets(getUniqueFields());
						target.add(node);
						break;
					}
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Branch Handling
	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		addClosedBranches(getPredecessorNode().closedBranchingNodes);
		this.openBranches = getPredecessorNode().getBranchesForParent(this.inConn);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                   Estimates Computation
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Computes the width of output records.
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		
		CompilerHints hints = getPactContract().getCompilerHints();
		
		// use hint if available
		if (hints != null && hints.getAvgBytesPerRecord() != -1) {
			return hints.getAvgBytesPerRecord();
		}

		// compute width from output size and cardinality
		final long numRecords = computeNumberOfStubCalls();
		
		long outputSize = 0;
		if (getPredecessorNode() != null) {
			outputSize = getPredecessorNode().estimatedOutputSize;
		}
		
		// compute width only if we have information
		if (numRecords == -1 || outputSize == -1)
			return -1;
		
		final double width = outputSize / (double)numRecords;

		// a record must have at least one byte...
		if (width < 1)
			return 1;
		else 
			return width;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                   Stub Annotation Handling
	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		final SingleInputContract<?> c = (SingleInputContract<?>) super.getPactContract();
		
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

	// --------------------------------------------------------------------------------------------
	//                                     Miscellaneous
	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor
	 * )
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			if (getPredecessorNode() != null) {
				getPredecessorNode().accept(visitor);
			} else {
				throw new CompilerException();
			}
			visitor.postVisit(this);
		}
	}
}
