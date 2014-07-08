/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.dag;

import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.operators.OperatorDescriptorSingle;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.NamedChannel;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport;
import eu.stratosphere.compiler.util.NoOpUnaryUdfOp;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.util.Visitor;

/**
 * A node in the optimizer's program representation for a PACT with a single input.
 * 
 * This class contains all the generic logic for branch handling, interesting properties,
 * and candidate plan enumeration. The subclasses for specific operators simply add logic
 * for cost estimates and specify possible strategies for their realization.
 */
public abstract class SingleInputNode extends OptimizerNode {
	
	protected final FieldSet keys; 			// The set of key fields
	
	protected PactConnection inConn; 		// the input of the node
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract The PACT that the node represents.
	 */
	protected SingleInputNode(SingleInputOperator<?, ?, ?> pactContract) {
		super(pactContract);
		
		int[] k = pactContract.getKeyColumns(0);
		this.keys = k == null || k.length == 0 ? null : new FieldSet(k);
	}
	
	protected SingleInputNode(FieldSet keys) {
		super(NoOpUnaryUdfOp.INSTANCE);
		this.keys = keys;
	}
	
	protected SingleInputNode() {
		super(NoOpUnaryUdfOp.INSTANCE);
		this.keys = null;
	}
	
	protected SingleInputNode(SingleInputNode toCopy) {
		super(toCopy);
		
		this.keys = toCopy.keys;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public SingleInputOperator<?, ?, ?> getPactContract() {
		return (SingleInputOperator<?, ?, ?>) super.getPactContract();
	}
	
	/**
	 * Gets the input of this operator.
	 * 
	 * @return The input.
	 */
	public PactConnection getIncomingConnection() {
		return this.inConn;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @param inConn The input connection to set.
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
			return this.inConn.getSource();
		} else {
			return null;
		}
	}


	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.singletonList(this.inConn);
	}
	

	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		if (input != 0) {
			throw new IndexOutOfBoundsException();
		}
		
		SingleInputOperator<?, ?, ?> c = getPactContract();
		SingleInputSemanticProperties semanticProperties = c.getSemanticProperties();
		
		if (semanticProperties != null) {
			FieldSet fs;
			if ((fs = semanticProperties.getForwardedField(fieldNumber)) != null) {
				return fs.contains(fieldNumber);
			}
		}
		
		return false;
	}
	

	@Override
	public void setInput(Map<Operator<?>, OptimizerNode> contractToNode) throws CompilerException {
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
		Operator<?> children = ((SingleInputOperator<?, ?, ?>) getPactContract()).getInput();
		
		OptimizerNode pred;
		PactConnection conn;
		if (children == null) {
			throw new CompilerException("Error: Node for '" + getPactContract().getName() + "' has no input.");
		} else {
			pred = contractToNode.get(children);
			conn = new PactConnection(pred, this);
			if (preSet != null) {
				conn.setShipStrategy(preSet);
			}
		}
		
		// create the connection and add it
		setIncomingConnection(conn);
		pred.addOutgoingConnection(conn);
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Properties and Optimization
	// --------------------------------------------------------------------------------------------
	
	protected abstract List<OperatorDescriptorSingle> getPossibleProperties();
	

	@Override
	public boolean isMemoryConsumer() {
		for (OperatorDescriptorSingle dps : getPossibleProperties()) {
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

	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// get what we inherit and what is preserved by our user code 
		final InterestingProperties props = getInterestingProperties().filterByCodeAnnotations(this, 0);
		
		// add all properties relevant to this node
		for (OperatorDescriptorSingle dps : getPossibleProperties()) {
			for (RequestedGlobalProperties gp : dps.getPossibleGlobalProperties()) {
				props.addGlobalProperties(gp);
			}
			for (RequestedLocalProperties lp : dps.getPossibleLocalProperties()) {
				props.addLocalProperties(lp);
			}
		}
		this.inConn.setInterestingProperties(props);
		
		for (PactConnection conn : getBroadcastConnections()) {
			conn.setInterestingProperties(new InterestingProperties());
		}
	}
	

	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// calculate alternative sub-plans for predecessor
		final List<? extends PlanNode> subPlans = getPredecessorNode().getAlternativePlans(estimator);
		final Set<RequestedGlobalProperties> intGlobal = this.inConn.getInterestingProperties().getGlobalProperties();
		
		// calculate alternative sub-plans for broadcast inputs
		final List<Set<? extends NamedChannel>> broadcastPlanChannels = new ArrayList<Set<? extends NamedChannel>>();
		List<PactConnection> broadcastConnections = getBroadcastConnections();
		List<String> broadcastConnectionNames = getBroadcastConnectionNames();
		for (int i = 0; i < broadcastConnections.size(); i++ ) {
			PactConnection broadcastConnection = broadcastConnections.get(i);
			String broadcastConnectionName = broadcastConnectionNames.get(i);
			List<PlanNode> broadcastPlanCandidates = broadcastConnection.getSource().getAlternativePlans(estimator);
			// wrap the plan candidates in named channels 
			HashSet<NamedChannel> broadcastChannels = new HashSet<NamedChannel>(broadcastPlanCandidates.size());
			for (PlanNode plan: broadcastPlanCandidates) {
				final NamedChannel c = new NamedChannel(broadcastConnectionName, plan);
				c.setShipStrategy(ShipStrategyType.BROADCAST);
				broadcastChannels.add(c);
			}
			broadcastPlanChannels.add(broadcastChannels);
		}

		final RequestedGlobalProperties[] allValidGlobals;
		{
			Set<RequestedGlobalProperties> pairs = new HashSet<RequestedGlobalProperties>();
			for (OperatorDescriptorSingle ods : getPossibleProperties()) {
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
					final Channel c = new Channel(child, this.inConn.getMaterializationMode());
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
							c.setRequiredGlobalProps(rgps);
							addLocalCandidates(c, broadcastPlanChannels, igps, outputPlans, estimator);
							break;
						}
					}
				}
			} else {
				// hint fixed the strategy
				final Channel c = new Channel(child, this.inConn.getMaterializationMode());
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
						addLocalCandidates(c, broadcastPlanChannels, rgps, outputPlans, estimator);
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
	
	protected void addLocalCandidates(Channel template, List<Set<? extends NamedChannel>> broadcastPlanChannels, RequestedGlobalProperties rgps,
			List<PlanNode> target, CostEstimator estimator)
	{
		for (RequestedLocalProperties ilp : this.inConn.getInterestingProperties().getLocalProperties()) {
			final Channel in = template.clone();
			ilp.parameterizeChannel(in);
			
			// instantiate a candidate, if the instantiated local properties meet one possible local property set
			outer:
			for (OperatorDescriptorSingle dps: getPossibleProperties()) {
				for (RequestedLocalProperties ilps : dps.getPossibleLocalProperties()) {
					if (ilps.isMetBy(in.getLocalProperties())) {
						in.setRequiredLocalProps(ilps);
						instantiateCandidate(dps, in, broadcastPlanChannels, target, estimator, rgps, ilp);
						break outer;
					}
				}
			}
		}
	}

	protected void instantiateCandidate(OperatorDescriptorSingle dps, Channel in, List<Set<? extends NamedChannel>> broadcastPlanChannels,
			List<PlanNode> target, CostEstimator estimator, RequestedGlobalProperties globPropsReq, RequestedLocalProperties locPropsReq)
	{
		final PlanNode inputSource = in.getSource();
		
		for (List<NamedChannel> broadcastChannelsCombination: Sets.cartesianProduct(broadcastPlanChannels)) {
			
			boolean validCombination = true;
			boolean requiresPipelinebreaker = false;
			
			// check whether the broadcast inputs use the same plan candidate at the branching point
			for (int i = 0; i < broadcastChannelsCombination.size(); i++) {
				NamedChannel nc = broadcastChannelsCombination.get(i);
				PlanNode bcSource = nc.getSource();
				
				// check branch compatibility against input
				if (!areBranchCompatible(bcSource, inputSource)) {
					validCombination = false;
					break;
				}
				
				// check branch compatibility against all other broadcast variables
				for (int k = 0; k < i; k++) {
					PlanNode otherBcSource = broadcastChannelsCombination.get(k).getSource();
					
					if (!areBranchCompatible(bcSource, otherBcSource)) {
						validCombination = false;
						break;
					}
				}
				
				// check if there is a common predecessor and whether there is a dam on the way to all common predecessors
				if (this.hereJoinedBranches != null) {
					for (OptimizerNode brancher : this.hereJoinedBranches) {
						PlanNode candAtBrancher = in.getSource().getCandidateAtBranchPoint(brancher);
						
						if (candAtBrancher == null) {
							// closed branch between two broadcast variables
							continue;
						}
						
						SourceAndDamReport res = in.getSource().hasDamOnPathDownTo(candAtBrancher);
						if (res == NOT_FOUND) {
							throw new CompilerException("Bug: Tracing dams for deadlock detection is broken.");
						} else if (res == FOUND_SOURCE) {
							requiresPipelinebreaker = true;
							break;
						} else if (res == FOUND_SOURCE_AND_DAM) {
							// good
						} else {
							throw new CompilerException();
						}
					}
				}
			}
			
			if (!validCombination) {
				continue;
			}
			
			if (requiresPipelinebreaker) {
				in.setTempMode(in.getTempMode().makePipelineBreaker());
			}
			
			final SingleInputPlanNode node = dps.instantiate(in, this);
			node.setBroadcastInputs(broadcastChannelsCombination);
			
			// compute how the strategy affects the properties
			GlobalProperties gProps = in.getGlobalProperties().clone();
			LocalProperties lProps = in.getLocalProperties().clone();
			gProps = dps.computeGlobalProperties(gProps);
			lProps = dps.computeLocalProperties(lProps);
			
			// filter by the user code field copies
			gProps = gProps.filterByNodesConstantSet(this, 0);
			lProps = lProps.filterByNodesConstantSet(this, 0);
			
			// apply
			node.initProperties(gProps, lProps);
			node.updatePropertiesWithUniqueSets(getUniqueFields());
			target.add(node);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Branch Handling
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		addClosedBranches(getPredecessorNode().closedBranchingNodes);
		List<UnclosedBranchDescriptor> fromInput = getPredecessorNode().getBranchesForParent(this.inConn);
		
		// handle the data flow branching for the broadcast inputs
		List<UnclosedBranchDescriptor> result = computeUnclosedBranchStackForBroadcastInputs(fromInput);
		
		this.openBranches = (result == null || result.isEmpty()) ? Collections.<UnclosedBranchDescriptor>emptyList() : result;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Miscellaneous
	// --------------------------------------------------------------------------------------------

	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			if (getPredecessorNode() != null) {
				getPredecessorNode().accept(visitor);
			} else {
				throw new CompilerException();
			}
			for (PactConnection connection : getBroadcastConnections()) {
				connection.getSource().accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
