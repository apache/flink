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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.CostEstimator;
import org.apache.flink.compiler.costs.Costs;
import org.apache.flink.compiler.plan.PlanNode;
import org.apache.flink.compiler.plan.SourcePlanNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Visitor;

/**
 * The optimizer's internal representation of a data source.
 */
public class DataSourceNode extends OptimizerNode {
	
	private final boolean sequentialInput;

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
			setDegreeOfParallelism(1);
			this.sequentialInput = true;
		} else {
			this.sequentialInput = false;
		}
	}

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericDataSourceBase<?, ?> getPactContract() {
		return (GenericDataSourceBase<?, ?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Data Source";
	}

	@Override
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		// if unsplittable, DOP remains at 1
		if (!this.sequentialInput) {
			super.setDegreeOfParallelism(degreeOfParallelism);
		}
	}

	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.<PactConnection>emptyList();
	}

	@Override
	public void setInput(Map<Operator<?>, OptimizerNode> contractToNode) {}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// see, if we have a statistics object that can tell us a bit about the file
		if (statistics != null) {
			// instantiate the input format, as this is needed by the statistics 
			InputFormat<?, ?> format = null;
			String inFormatDescription = "<unknown>";
			
			try {
				format = getPactContract().getFormatWrapper().getUserCodeObject();
				Configuration config = getPactContract().getParameters();
				format.configure(config);
			}
			catch (Throwable t) {
				if (PactCompiler.LOG.isWarnEnabled()) {
					PactCompiler.LOG.warn("Could not instantiate InputFormat to obtain statistics."
						+ " Limited statistics will be available.", t);
				}
				return;
			}
			try {
				inFormatDescription = format.toString();
			}
			catch (Throwable t) {}
			
			// first of all, get the statistics from the cache
			final String statisticsKey = getPactContract().getStatisticsKey();
			final BaseStatistics cachedStatistics = statistics.getBaseStatistics(statisticsKey);
			
			BaseStatistics bs = null;
			try {
				bs = format.getStatistics(cachedStatistics);
			}
			catch (Throwable t) {
				if (PactCompiler.LOG.isWarnEnabled()) {
					PactCompiler.LOG.warn("Error obtaining statistics from input format: " + t.getMessage(), t);
				}
			}
			
			if (bs != null) {
				final long len = bs.getTotalInputSize();
				if (len == BaseStatistics.SIZE_UNKNOWN) {
					if (PactCompiler.LOG.isInfoEnabled()) {
						PactCompiler.LOG.info("Compiler could not determine the size of input '" + inFormatDescription + "'. Using default estimates.");
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
		
		SourcePlanNode candidate = new SourcePlanNode(this, "DataSource ("+this.getPactContract().getName()+")");
		candidate.updatePropertiesWithUniqueSets(getUniqueFields());
		
		final Costs costs = new Costs();
		if (FileInputFormat.class.isAssignableFrom(getPactContract().getFormatWrapper().getUserCodeClass()) &&
				this.estimatedOutputSize >= 0)
		{
			estimator.addFileInputCost(this.estimatedOutputSize, costs);
		}
		candidate.setCosts(costs);

		// since there is only a single plan for the data-source, return a list with that element only
		List<PlanNode> plans = new ArrayList<PlanNode>(1);
		plans.add(candidate);

		this.cachedPlans = plans;
		return plans;
	}

	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
	
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}
}
