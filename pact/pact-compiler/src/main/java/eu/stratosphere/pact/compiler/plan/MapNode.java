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

import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.util.FieldSet;
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
public class MapNode extends SingleInputNode {

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
	protected MapNode(MapNode template, List<OptimizerNode> pred, List<PactConnection> conn, GlobalProperties globalProps,
			LocalProperties localProps) {
		super(template, pred, conn, globalProps, localProps);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Gets the contract object for this map node.
	 * 
	 * @return The contract.
	 */
	@Override
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
		// the map itself has no interesting properties.
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.
		
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		List<InterestingProperties> props = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
			this, 0);
		
		for(PactConnection c : this.input) {
			if (!props.isEmpty()) {
				c.addAllInterestingProperties(props);
			} else {
				c.setNoInterestingProperties();
			} 
		}
	}

	@Override
	protected void computeValidPlanAlternatives(List<List<OptimizerNode>> alternativeSubPlanCominations, CostEstimator estimator, List<OptimizerNode> outputPlans) {
		
		for(List<OptimizerNode> predList : alternativeSubPlanCominations) {
			// we have to check if all input ShipStrategies are the same or at least compatible
			ShipStrategy ss = ShipStrategy.NONE;
		
			for(PactConnection c : this.input) {
				ShipStrategy newSS = c.getShipStrategy();
				
				if(newSS == ShipStrategy.BROADCAST || newSS == ShipStrategy.SFR)
					// invalid strategy: we do not produce an alternative node
					continue;
		
				// as long as no ShipStrategy is set we can pick the strategy from the current connection
				if(ss == ShipStrategy.NONE) {
					ss = newSS;
					continue;
				}
				
				// as long as the ShipStrategy is the same everything is fine
				if(ss == newSS)
					continue;
				
				// incompatible strategies: we do not produce an alternative node
				continue;
			}
		
			// if no hit for a strategy was provided, we use the default
			if(ss == ShipStrategy.NONE)
				ss = ShipStrategy.FORWARD;
			
			GlobalProperties gp;
			LocalProperties lp;
			if(predList.size() == 1) {
				gp = PactConnection.getGlobalPropertiesAfterConnection(predList.get(0), this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(predList.get(0), this, ss);
			} else {
				// TODO right now we drop all properties in the union case; need to figure out what properties can be kept
				gp = new GlobalProperties();
				lp = new LocalProperties();
			}
			
			
			MapNode nMap = new MapNode(this, predList, this.input, gp, lp);
			for(PactConnection cc : nMap.getInputConnections()) {
				cc.setShipStrategy(ss);
			}
		
			// now, the properties (copied from the inputs) are filtered by the
			// output contracts
			nMap.getGlobalProperties().filterByNodesConstantSet(this, 0);
			nMap.getLocalProperties().filterByNodesConstantSet(this, 0);

			// copy the cumulative costs and set the costs of the map itself to zero
			estimator.costOperator(nMap);
		
			outputPlans.add(nMap);			
		}		
	}

	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	private long computeNumberOfProcessedKeys() {
		long keySum = 0;
		FieldSet fieldSet = new FieldSet(getPactContract().getKeyColumnNumbers(0));
		
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();
		
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its record count, we a pessimistic and return "unknown" as well
				if(pred.getEstimatedCardinality(fieldSet) == -1)
					return -1;
				
				// Each key is processed by Map
				// all inputs are union -> we sum up the keyCounts
				keySum += pred.getEstimatedCardinality(fieldSet);
			} 
		}
		
		return keySum;
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
	protected double computeStubCallsPerProcessedKey() {
		// we are pessimistic -> if one value is unknown we return "unknown" as well
		
		long numRecords = computeNumberOfStubCalls();
		if(numRecords == -1)
			return -1;
		
		long keyCardinality = computeNumberOfProcessedKeys();
		if(keyCardinality == -1)
			return -1;

		return numRecords / (double)keyCardinality;
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		long sumStubCalls = 0;
		
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();

			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its stub call count, we a pessimistic and return "unknown" as well
				if(pred.estimatedNumRecords == -1)
					return -1;
				
				// Map is called once per record
				// all inputs are union -> we sum up the stubCallCount
				sumStubCalls += pred.estimatedNumRecords; 
			}
			
		}
		
		return sumStubCalls;
	}
	
}
