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
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Map</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MapNode extends SingleInputNode
{
	private List<MapNode> cachedPlans; // a cache for the computed alternative plans

	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param pactContract
	 *        The map contract object.
	 */
	public MapNode(MapContract pactContract) {
		super(pactContract);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Copy constructor to create a copy a MapNode with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred
	 *        The new predecessor.
	 * @param conn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected MapNode(MapNode template, OptimizerNode pred, PactConnection conn, GlobalProperties globalProps,
			LocalProperties localProps) {
		super(template, pred, conn, globalProps, localProps);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Gets the contract object for this map node.
	 * 
	 * @return The contract.
	 */
	public MapContract getPactContract() {
		return (MapContract) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Map";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		super.setInputs(contractToNode);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
//		// the map itself has no interesting properties.
//		// check, if there is an output contract that tells us that certain properties are preserved.
//		// if so, propagate to the child.
//
//		OutputContract oc = getOutputContract();
//		if (oc == OutputContract.None) {
//			input.setNoInterestingProperties();
//		} else if (oc == OutputContract.SameKey || oc == OutputContract.SuperKey) {
//			List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
//			List<InterestingProperties> props = InterestingProperties.filterByOutputContract(thisNodesIntProps,
//				getOutputContract());
//			if (!props.isEmpty()) {
//				input.addAllInterestingProperties(props);
//			} else {
//				input.setNoInterestingProperties();
//			}
//		}
		this.input.setNoInterestingProperties();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans()
	 */
	@Override
	public List<MapNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (cachedPlans != null) {
			return cachedPlans;
		}

		// when generating the different alternative plans for a map,
		// we need to take all alternative plans for input and
		// filter their properties by the output contract.
		// the remaining list is pruned.

		// the map itself also adds no cost for local strategies!

		List<? extends OptimizerNode> inPlans = input.getSourcePact().getAlternativePlans(estimator);
		List<MapNode> outputPlans = new ArrayList<MapNode>();

		for (OptimizerNode pred : inPlans) {

			ShipStrategy ss = input.getShipStrategy() == ShipStrategy.NONE ? ShipStrategy.FORWARD : input
				.getShipStrategy();
			GlobalProperties gp = PactConnection.getGlobalPropertiesAfterConnection(pred, this, ss);
			LocalProperties lp = PactConnection.getLocalPropertiesAfterConnection(pred, this, ss);

			// we take each input and add a mapper to it
			// the properties of the inputs are copied
			MapNode nMap = new MapNode(this, pred, input, gp, lp);
			nMap.input.setShipStrategy(ss);

			// now, the properties (copied from the inputs) are filtered by the
			// output contracts
//			nMap.getGlobalProperties().filterByOutputContract(getOutputContract());
//			nMap.getLocalProperties().filterByOutputContract(getOutputContract());
			nMap.getGlobalProperties().reset();
			nMap.getLocalProperties().reset();

			// copy the cumulative costs and set the costs of the map itself to zero
			estimator.costOperator(nMap);

			outputPlans.add(nMap);
		}

		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (this.getOutgoingConnections() != null && this.getOutgoingConnections().size() > 1) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}
	
//	/**
//	 * Computes the number of keys that are processed by the PACT.
//	 * 
//	 * @return the number of keys processed by the PACT.
//	 */
//	private long computeNumberOfProcessedKeys() {
//		OptimizerNode pred = input == null ? null : input.getSourcePact();
//
//		if(pred != null) {
//			// Each key is processed by Map
//			return pred.getEstimatedKeyCardinality();
//		} else {
//			return -1;
//		}
//	}
	
//	/**
//	 * Computes the number of stub calls for one processed key. 
//	 * 
//	 * @return the number of stub calls for one processed key.
//	 */
//	private double computeStubCallsPerProcessedKey() {
//		OptimizerNode pred = input == null ? null : input.getSourcePact();
//		
//		if(pred != null) {
//			if(pred.getEstimatedNumRecords() != -1 && pred.getEstimatedKeyCardinality() != -1) {
//				return (pred.getEstimatedNumRecords() / pred.getEstimatedKeyCardinality());
//			} else {
//				return -1;
//			}
//		} else {
//			return -1;
//		}
//	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		
		if(pred != null) {
			// Map is called once per record
			return pred.getEstimatedNumRecords();
		} else {
			return -1;
		}
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		CompilerHints hints = getPactContract().getCompilerHints();
		
		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		
		} else if (pred != null) {
			// use record width of previous node
			
			if(pred.estimatedOutputSize != -1 && pred.estimatedNumRecords != -1) {
				return (pred.getEstimatedOutputSize() / pred.getEstimatedNumRecords()) >= 1 ? 
						(long) (pred.getEstimatedOutputSize() / pred.getEstimatedNumRecords()) : 1;
			} else {
				return -1.0;
			}
			
		} else {
			return -1.0;
		}
	}
	
}
