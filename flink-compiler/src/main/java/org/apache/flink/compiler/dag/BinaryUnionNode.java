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


package org.apache.flink.compiler.dag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.operators.Union;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.costs.CostEstimator;
import org.apache.flink.compiler.dataproperties.GlobalProperties;
import org.apache.flink.compiler.dataproperties.InterestingProperties;
import org.apache.flink.compiler.dataproperties.RequestedGlobalProperties;
import org.apache.flink.compiler.dataproperties.RequestedLocalProperties;
import org.apache.flink.compiler.operators.BinaryUnionOpDescriptor;
import org.apache.flink.compiler.operators.OperatorDescriptorDual;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.plan.NamedChannel;
import org.apache.flink.compiler.plan.PlanNode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

/**
 * The Optimizer representation of a binary <i>Union</i>.
 */
public class BinaryUnionNode extends TwoInputNode {
	
	private Set<RequestedGlobalProperties> channelProps;

	public BinaryUnionNode(Union<?> union){
		super(union);
	}

	@Override
	public String getName() {
		return "Union";
	}

	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return new ArrayList<OperatorDescriptorDual>();
	}
	
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
		long card2 = getSecondPredecessorNode().getEstimatedNumRecords();
		this.estimatedNumRecords = (card1 < 0 || card2 < 0) ? -1 : card1 + card2;
		
		long size1 = getFirstPredecessorNode().getEstimatedOutputSize();
		long size2 = getSecondPredecessorNode().getEstimatedOutputSize();
		this.estimatedOutputSize = (size1 < 0 || size2 < 0) ? -1 : size1 + size2;
	}
	
	@Override
	public void computeUnionOfInterestingPropertiesFromSuccessors() {
		super.computeUnionOfInterestingPropertiesFromSuccessors();
		// clear all local properties, as they are destroyed anyways
		getInterestingProperties().getLocalProperties().clear();
	}
	
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) { 
		final InterestingProperties props = getInterestingProperties();
		
		// if no other properties exist, add the pruned trivials back
		if (props.getGlobalProperties().isEmpty()) {
			props.addGlobalProperties(new RequestedGlobalProperties());
		}
		props.addLocalProperties(new RequestedLocalProperties());
		this.input1.setInterestingProperties(props.clone());
		this.input2.setInterestingProperties(props.clone());
		
		this.channelProps = props.getGlobalProperties();
	}
	
	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// step down to all producer nodes and calculate alternative plans
		final List<? extends PlanNode> subPlans1 = getFirstPredecessorNode().getAlternativePlans(estimator);
		final List<? extends PlanNode> subPlans2 = getSecondPredecessorNode().getAlternativePlans(estimator);
		
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
		
		final ArrayList<PlanNode> outputPlans = new ArrayList<PlanNode>();
		
		final BinaryUnionOpDescriptor operator = new BinaryUnionOpDescriptor();
		final RequestedLocalProperties noLocalProps = new RequestedLocalProperties();
		
		final int dop = getDegreeOfParallelism();
		final int inDop1 = getFirstPredecessorNode().getDegreeOfParallelism();
		final int inDop2 = getSecondPredecessorNode().getDegreeOfParallelism();

		final boolean dopChange1 = dop != inDop1;
		final boolean dopChange2 = dop != inDop2;

		// enumerate all pairwise combination of the children's plans together with
		// all possible operator strategy combination
		
		// create all candidates
		for (PlanNode child1 : subPlans1) {
			for (PlanNode child2 : subPlans2) {
				
				// check that the children go together. that is the case if they build upon the same
				// candidate at the joined branch plan. 
				if (!areBranchCompatible(child1, child2)) {
					continue;
				}
				
				for (RequestedGlobalProperties igps: this.channelProps) {
					// create a candidate channel for the first input. mark it cached, if the connection says so
					Channel c1 = new Channel(child1, this.input1.getMaterializationMode());
					if (this.input1.getShipStrategy() == null) {
						// free to choose the ship strategy
						igps.parameterizeChannel(c1, dopChange1);
						
						// if the DOP changed, make sure that we cancel out properties, unless the
						// ship strategy preserves/establishes them even under changing DOPs
						if (dopChange1 && !c1.getShipStrategy().isNetworkStrategy()) {
							c1.getGlobalProperties().reset();
						}
					} else {
						// ship strategy fixed by compiler hint
						if (this.keys1 != null) {
							c1.setShipStrategy(this.input1.getShipStrategy(), this.keys1.toFieldList());
						} else {
							c1.setShipStrategy(this.input1.getShipStrategy());
						}
						
						if (dopChange1) {
							c1.adjustGlobalPropertiesForFullParallelismChange();
						}
					}
					
					// create a candidate channel for the first input. mark it cached, if the connection says so
					Channel c2 = new Channel(child2, this.input2.getMaterializationMode());
					if (this.input2.getShipStrategy() == null) {
						// free to choose the ship strategy
						igps.parameterizeChannel(c2, dopChange2);
						
						// if the DOP changed, make sure that we cancel out properties, unless the
						// ship strategy preserves/establishes them even under changing DOPs
						if (dopChange2 && !c2.getShipStrategy().isNetworkStrategy()) {
							c2.getGlobalProperties().reset();
						}
					} else {
						// ship strategy fixed by compiler hint
						if (this.keys2 != null) {
							c2.setShipStrategy(this.input2.getShipStrategy(), this.keys2.toFieldList());
						} else {
							c2.setShipStrategy(this.input2.getShipStrategy());
						}
						
						if (dopChange2) {
							c2.adjustGlobalPropertiesForFullParallelismChange();
						}
					}
					
					// get the global properties and clear unique fields (not preserved anyways during the union)
					GlobalProperties p1 = c1.getGlobalProperties();
					GlobalProperties p2 = c2.getGlobalProperties();
					p1.clearUniqueFieldCombinations();
					p2.clearUniqueFieldCombinations();
					
					// adjust the partitionings, if they exist but are not equal. this may happen when both channels have a
					// partitioning that fulfills the requirements, but both are incompatible. For example may a property requirement
					// be ANY_PARTITIONING on fields (0) and one channel is range partitioned on that field, the other is hash
					// partitioned on that field. 
					if (!igps.isTrivial() && !(p1.equals(p2))) {
						if (c1.getShipStrategy() == ShipStrategyType.FORWARD && c2.getShipStrategy() != ShipStrategyType.FORWARD) {
							// adjust c2 to c1
							c2 = c2.clone();
							p1.parameterizeChannel(c2,dopChange2);
						} else if (c2.getShipStrategy() == ShipStrategyType.FORWARD && c1.getShipStrategy() != ShipStrategyType.FORWARD) {
							// adjust c1 to c2
							c1 = c1.clone();
							p2.parameterizeChannel(c1,dopChange1);
						} else if (c1.getShipStrategy() == ShipStrategyType.FORWARD && c2.getShipStrategy() == ShipStrategyType.FORWARD) {
							boolean adjustC1 = c1.getEstimatedOutputSize() <= 0 || c2.getEstimatedOutputSize() <= 0 ||
									c1.getEstimatedOutputSize() <= c2.getEstimatedOutputSize();
							if (adjustC1) {
								c2 = c2.clone();
								p1.parameterizeChannel(c2, dopChange2);
							} else {
								c1 = c1.clone();
								p2.parameterizeChannel(c1, dopChange1);
							}
						} else {
							// this should never happen, as it implies both realize a different strategy, which is
							// excluded by the check that the required strategies must match
							throw new CompilerException("Bug in Plan Enumeration for Union Node.");
						}
					}
					
					instantiate(operator, c1, c2, broadcastPlanChannels, outputPlans, estimator, igps, igps, noLocalProps, noLocalProps);
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
	
	@Override
	protected void readStubAnnotations() {}

	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		return true;
	}
	
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		OptimizerNode in1 = getFirstPredecessorNode();
		OptimizerNode in2 = getSecondPredecessorNode();
		
		this.estimatedNumRecords = in1.estimatedNumRecords > 0 && in2.estimatedNumRecords > 0 ?
				in1.estimatedNumRecords + in2.estimatedNumRecords : -1;
		this.estimatedOutputSize = in1.estimatedOutputSize > 0 && in2.estimatedOutputSize > 0 ?
			in1.estimatedOutputSize + in2.estimatedOutputSize : -1;
	}
}
