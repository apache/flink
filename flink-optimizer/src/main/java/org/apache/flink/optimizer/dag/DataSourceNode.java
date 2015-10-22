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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.ReplicatingInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.GenericDataSourceBase.SplitDataProperties;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties.EmptySemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.costs.Costs;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Visitor;

/**
 * The optimizer's internal representation of a data source.
 */
public class DataSourceNode extends OptimizerNode {
	
	private final boolean sequentialInput;

	private final boolean replicatedInput;

	private GlobalProperties gprops;

	private LocalProperties lprops;

	/**
	 * Creates a new DataSourceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data source contract object.
	 */
	public DataSourceNode(GenericDataSourceBase<?, ?> pactContract) {
		super(pactContract);
		
		if (pactContract.getUserCodeWrapper().getUserCodeClass() == null) {
			throw new IllegalArgumentException("Input format has not been set.");
		}
		
		if (NonParallelInput.class.isAssignableFrom(pactContract.getUserCodeWrapper().getUserCodeClass())) {
			setParallelism(1);
			this.sequentialInput = true;
		} else {
			this.sequentialInput = false;
		}

		this.replicatedInput = ReplicatingInputFormat.class.isAssignableFrom(
														pactContract.getUserCodeWrapper().getUserCodeClass());

		this.gprops = new GlobalProperties();
		this.lprops = new LocalProperties();

		SplitDataProperties<?> splitProps = pactContract.getSplitDataProperties();

		if(replicatedInput) {
			this.gprops.setFullyReplicated();
			this.lprops = new LocalProperties();
		} else if (splitProps != null) {
			// configure data properties of data source using split properties
			setDataPropertiesFromSplitProperties(splitProps);
		}

	}

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericDataSourceBase<?, ?> getOperator() {
		return (GenericDataSourceBase<?, ?>) super.getOperator();
	}

	@Override
	public String getOperatorName() {
		return "Data Source";
	}

	@Override
	public void setParallelism(int parallelism) {
		// if unsplittable, parallelism remains at 1
		if (!this.sequentialInput) {
			super.setParallelism(parallelism);
		}
	}

	@Override
	public List<DagConnection> getIncomingConnections() {
		return Collections.<DagConnection>emptyList();
	}

	@Override
	public void setInput(Map<Operator<?>, OptimizerNode> contractToNode, ExecutionMode defaultDataExchangeMode) {}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// see, if we have a statistics object that can tell us a bit about the file
		if (statistics != null) {
			// instantiate the input format, as this is needed by the statistics 
			InputFormat<?, ?> format;
			String inFormatDescription = "<unknown>";
			
			try {
				format = getOperator().getFormatWrapper().getUserCodeObject();
				Configuration config = getOperator().getParameters();
				format.configure(config);
			}
			catch (Throwable t) {
				if (Optimizer.LOG.isWarnEnabled()) {
					Optimizer.LOG.warn("Could not instantiate InputFormat to obtain statistics."
						+ " Limited statistics will be available.", t);
				}
				return;
			}
			try {
				inFormatDescription = format.toString();
			}
			catch (Throwable t) {
				// we can ignore this error, as it only prevents us to use a cosmetic string
			}
			
			// first of all, get the statistics from the cache
			final String statisticsKey = getOperator().getStatisticsKey();
			final BaseStatistics cachedStatistics = statistics.getBaseStatistics(statisticsKey);
			
			BaseStatistics bs = null;
			try {
				bs = format.getStatistics(cachedStatistics);
			}
			catch (Throwable t) {
				if (Optimizer.LOG.isWarnEnabled()) {
					Optimizer.LOG.warn("Error obtaining statistics from input format: " + t.getMessage(), t);
				}
			}
			
			if (bs != null) {
				final long len = bs.getTotalInputSize();
				if (len == BaseStatistics.SIZE_UNKNOWN) {
					if (Optimizer.LOG.isInfoEnabled()) {
						Optimizer.LOG.info("Compiler could not determine the size of input '" + inFormatDescription + "'. Using default estimates.");
					}
				}
				else if (len >= 0) {
					this.estimatedOutputSize = len;
				}
				
				final long card = bs.getNumberOfRecords();
				if (card != BaseStatistics.NUM_RECORDS_UNKNOWN) {
					this.estimatedNumRecords = card;
				}
			}
		}
	}

	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// no children, so nothing to compute
	}

	@Override
	public void computeUnclosedBranchStack() {
		// because there are no inputs, there are no unclosed branches.
		this.openBranches = Collections.emptyList();
	}

	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		SourcePlanNode candidate = new SourcePlanNode(this, "DataSource ("+this.getOperator().getName()+")",
				this.gprops, this.lprops);

		if(!replicatedInput) {
			candidate.updatePropertiesWithUniqueSets(getUniqueFields());

			final Costs costs = new Costs();
			if (FileInputFormat.class.isAssignableFrom(getOperator().getFormatWrapper().getUserCodeClass()) &&
					this.estimatedOutputSize >= 0) {
				estimator.addFileInputCost(this.estimatedOutputSize, costs);
			}
			candidate.setCosts(costs);
		} else {
			// replicated input
			final Costs costs = new Costs();
			InputFormat<?,?> inputFormat =
					((ReplicatingInputFormat<?,?>) getOperator().getFormatWrapper().getUserCodeObject()).getReplicatedInputFormat();
			if (FileInputFormat.class.isAssignableFrom(inputFormat.getClass()) &&
					this.estimatedOutputSize >= 0) {
				estimator.addFileInputCost(this.estimatedOutputSize * this.getParallelism(), costs);
			}
			candidate.setCosts(costs);
		}

		// since there is only a single plan for the data-source, return a list with that element only
		List<PlanNode> plans = new ArrayList<PlanNode>(1);
		plans.add(candidate);

		this.cachedPlans = plans;
		return plans;
	}

	@Override
	public SemanticProperties getSemanticProperties() {
		return new EmptySemanticProperties();
	}
	
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}

	private void setDataPropertiesFromSplitProperties(SplitDataProperties splitProps) {

		// set global properties
		int[] partitionKeys = splitProps.getSplitPartitionKeys();
		Partitioner<?> partitioner = splitProps.getSplitPartitioner();

		if(partitionKeys != null && partitioner != null) {
			this.gprops.setCustomPartitioned(new FieldList(partitionKeys), partitioner);
		}
		else if(partitionKeys != null) {
			this.gprops.setAnyPartitioning(new FieldList(partitionKeys));
		}
		// set local properties
		int[] groupingKeys = splitProps.getSplitGroupKeys();
		Ordering ordering = splitProps.getSplitOrder();

		// more than one split per source tasks possible.
		// adapt split grouping and sorting
		if(ordering != null) {

			// sorting falls back to grouping because a source can read multiple,
			// randomly assigned splits
			groupingKeys = ordering.getFieldPositions();
		}

		if(groupingKeys != null && partitionKeys != null) {
			// check if grouping is also valid across splits, i.e., whether grouping keys are
			// valid superset of partition keys
			boolean allFieldsIncluded = true;
			for(int i : partitionKeys) {
				boolean fieldIncluded = false;
				for(int j : groupingKeys) {
					if(i == j) {
						fieldIncluded = true;
						break;
					}
				}
				if(!fieldIncluded) {
					allFieldsIncluded = false;
					break;
				}
			}
			if (allFieldsIncluded) {
				this.lprops = LocalProperties.forGrouping(new FieldList(groupingKeys));
			} else {
				this.lprops = new LocalProperties();
			}

		} else {
			this.lprops = new LocalProperties();
		}
	}
}
