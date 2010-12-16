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
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.OutputContract;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Map</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MapNode extends SingleInputNode {
	private List<MapNode> cachedPlans; // a cache for the computed alternative plans

	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param pactContract
	 *        The map contract object.
	 */
	public MapNode(MapContract<?, ?, ?, ?> pactContract) {
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
	 * @param globalProps
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
	public MapContract<?, ?, ?, ?> getPactContract() {
		return (MapContract<?, ?, ?, ?>) super.getPactContract();
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
	public boolean isMemoryConsumer() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		super.setInputs(contractToNode);
	}

	/**
	 * This method computes the estimated outputs for the user function represented by this node.
	 * 
	 * @param statistics
	 *        The statistics wrapper to be used to obtain additional knowledge. Currently ignored.
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		CompilerHints hints = getPactContract().getCompilerHints();

		if (pred != null) {

			// the key cardinality is computed as follows:
			// 1) If a hint specifies it directly, that value is taken
			// 2) If a same-key contract is given, we assume the key cardinality remains the same.
			// It may still be lower (but not higher), but for robustness reasons, we choose the maximum value,
			// because overestimates are more robust than underestimates.

			if (hints.getKeyCardinality() > 0) {
				this.estimatedKeyCardinality = hints.getKeyCardinality();
			} else {
				// if we have a same-key contract, we take the predecessors key cardinality. otherwise, it is unknown.
				if (getOutputContract() == OutputContract.SameKey) {
					this.estimatedKeyCardinality = pred.estimatedKeyCardinality;
				} else {
					this.estimatedKeyCardinality = -1;
				}
			}

			// estimate the number of rows
			this.estimatedNumRecords = -1;
			if (hints.getSelectivity() > 0.0f && pred.estimatedNumRecords != -1) {
				this.estimatedNumRecords = (long) (pred.estimatedNumRecords * hints.getSelectivity()) + 1;

				if (hints.getAvgNumValuesPerKey() >= 1.0f) {
					long v = (long) (this.estimatedNumRecords / hints.getAvgNumValuesPerKey()) + 1;
					if (this.estimatedKeyCardinality == -1) {
						this.estimatedKeyCardinality = v;
					} else if (hints.getKeyCardinality() < 1) {
						this.estimatedKeyCardinality = Math.min(this.estimatedKeyCardinality, v);
					}
				}
			} else if (this.estimatedKeyCardinality != -1 && hints.getAvgNumValuesPerKey() >= 1.0f) {
				this.estimatedNumRecords = (long) (this.estimatedKeyCardinality * hints.getAvgNumValuesPerKey()) + 1;
			} else {
				// we assume that the data size of the mapper is non-increasing
				this.estimatedNumRecords = pred.estimatedNumRecords;
			}

			// estimate the output size
			this.estimatedOutputSize = -1;
			if (this.estimatedNumRecords != -1) {
				if (hints.getAvgBytesPerRecord() > 0.0f) {
					this.estimatedOutputSize = (long) (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) + 1;
				} else {
					// calculate the width of records in the previous function
					long inSize = pred.estimatedOutputSize;
					long inVals = pred.estimatedNumRecords;
					float prevBytesPerRecord = (inSize > 0 && inVals > 0) ? inSize / ((float) inVals) : -1.0f;

					// assume that the records are still of the same width
					if (prevBytesPerRecord > 0.0f) {
						this.estimatedOutputSize = (long) (this.estimatedNumRecords * prevBytesPerRecord) + 1;
					}
				}
			} else {
				// assume that the data remains of the same size
				this.estimatedOutputSize = pred.estimatedOutputSize;
			}

			// check that the key-card is maximally as large as the number of rows
			if (this.estimatedKeyCardinality > this.estimatedNumRecords) {
				this.estimatedKeyCardinality = this.estimatedNumRecords;
			}
		} else {
			// we don't know anything
			this.estimatedKeyCardinality = hints.getKeyCardinality();
			this.estimatedNumRecords = -1;
			this.estimatedOutputSize = -1;
		}
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

		OutputContract oc = getOutputContract();
		if (oc == OutputContract.None) {
			input.setNoInterestingProperties();
		} else if (oc == OutputContract.SameKey || oc == OutputContract.SuperKey) {
			List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
			List<InterestingProperties> props = InterestingProperties.filterByOutputContract(thisNodesIntProps,
				getOutputContract());
			if (!props.isEmpty()) {
				input.addAllInterestingProperties(props);
			} else {
				input.setNoInterestingProperties();
			}
		}
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
			GlobalProperties gp = PactConnection.getGlobalPropertiesAfterConnection(pred, ss);
			LocalProperties lp = PactConnection.getLocalPropertiesAfterConnection(pred, ss);

			// we take each input and add a mapper to it
			// the properties of the inputs are copied
			MapNode nMap = new MapNode(this, pred, input, gp, lp);
			nMap.input.setShipStrategy(ss);

			// now, the properties (copied from the inputs) are filtered by the
			// output contracts
			nMap.getGlobalProperties().getPreservedAfterContract(getOutputContract());
			nMap.getLocalProperties().getPreservedAfterContract(getOutputContract());

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
}
