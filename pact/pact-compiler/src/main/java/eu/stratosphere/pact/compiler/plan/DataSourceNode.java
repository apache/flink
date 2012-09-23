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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * The optimizer's internal representation of a data source.
 * 
 * @author Stephan Ewen
 */
public class DataSourceNode extends OptimizerNode
{
	private List<PlanNode> candidate;		// the candidate (there can only be one) for this node
	
	private long inputSize;			//the size of the input in bytes

	/**
	 * Creates a new DataSourceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data source contract object.
	 */
	public DataSourceNode(GenericDataSource<?> pactContract) {
		super(pactContract);
		setDriverStrategy(DriverStrategy.NONE);
	}

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericDataSource<?> getPactContract() {
		return (GenericDataSource<?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Data Source";
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.<PactConnection>emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		throw new UnsupportedOperationException("A DataSourceNode does not have any input.");
	}

	/**
	 * Causes this node to compute its output estimates (such as number of rows, size in bytes)
	 * based on the file size and the compiler hints. The compiler hints are instantiated with
	 * conservative default values which are used if no other values are provided.
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics)
	{
		CompilerHints hints = getPactContract().getCompilerHints();
		
		// initialize basic estimates to unknown
		this.estimatedOutputSize = -1;
		this.inputSize = -1;
		this.estimatedNumRecords = -1;

		// see, if we have a statistics object that can tell us a bit about the file
		if (statistics != null)
		{
			// instantiate the input format, as this is needed by the statistics 
			InputFormat<?, ?> format = null;
			String inFormatDescription = "<unknown>";
			
			try {
				Class<? extends InputFormat<?, ?>> formatClass = getPactContract().getFormatClass();
				format = formatClass.newInstance();
				format.configure(getPactContract().getParameters());
			}
			catch (Throwable t) {
				if (PactCompiler.LOG.isWarnEnabled())
					PactCompiler.LOG.warn("Could not instantiate input format to obtain statistics."
						+ " Limited statistics will be available.", t);
				return;
			}
			try {
				inFormatDescription = format.toString();
			}
			catch (Throwable t) {}
			
			// first of all, get the statistics from the cache
			final String statisticsKey = getPactContract().getParameters().getString(InputFormat.STATISTICS_CACHE_KEY, null);
			final BaseStatistics cachedStatistics = statistics.getBaseStatistics(statisticsKey);
			
			BaseStatistics bs = null;
			try {
				bs = format.getStatistics(cachedStatistics);
			}
			catch (Throwable t) {
				if (PactCompiler.LOG.isWarnEnabled())
					PactCompiler.LOG.warn("Error obtaining statistics from input format: " + t.getMessage(), t);
			}
			
			if (bs != null) {
				final long len = bs.getTotalInputSize();
				if (len == BaseStatistics.UNKNOWN) {
					if (PactCompiler.LOG.isWarnEnabled())
						PactCompiler.LOG.warn("Pact compiler could not determine the size of input '" + inFormatDescription + "'.");
				}
				else if (len >= 0) {
					this.inputSize = len;
				}

				
				final float avgBytes = bs.getAverageRecordWidth();
				if (avgBytes > 0.0f && hints.getAvgBytesPerRecord() < 1.0f) {
					hints.setAvgBytesPerRecord(avgBytes);
				}
				
				final long card = bs.getNumberOfRecords();
				if (card != BaseStatistics.UNKNOWN) {
					this.estimatedNumRecords = card;
				}
			}
		}

		// the estimated number of rows is depending on the average row width
		if (this.estimatedNumRecords == -1 && hints.getAvgBytesPerRecord() != -1.0f && this.inputSize > 0) {
			this.estimatedNumRecords = (long) (this.inputSize / hints.getAvgBytesPerRecord()) + 1;
		}

		// the key cardinality is either explicitly specified, derived from an avgNumValuesPerKey hint, 
		// or we assume for robustness reasons that every record has a unique key. 
		// Key cardinality overestimation results in more robust plans
		
		this.estimatedCardinality.putAll(hints.getDistinctCounts());
		
		if(this.estimatedNumRecords != -1) {
			for (Entry<FieldSet, Float> avgNumValues : hints.getAvgNumRecordsPerDistinctFields().entrySet()) {
				if (estimatedCardinality.get(avgNumValues.getKey()) == null) {
					long estimatedCard = (this.estimatedNumRecords / avgNumValues.getValue() >= 1) ? 
							(long) (this.estimatedNumRecords / avgNumValues.getValue()) : 1;
					estimatedCardinality.put(avgNumValues.getKey(), estimatedCard);
				}
			}
		}
		else {
			// if we have the key cardinality and an average number of values per key, we can estimate the number
			// of rows
			this.estimatedNumRecords = 0;
			int count = 0;
			
			for (Entry<FieldSet, Long> cardinality : hints.getDistinctCounts().entrySet()) {
				float avgNumValues = hints.getAvgNumRecordsPerDistinctFields(cardinality.getKey());
				if (avgNumValues != -1) {
					this.estimatedNumRecords += cardinality.getValue() * avgNumValues;
					count++;
				}
			}
			
			if (count > 0) {
				this.estimatedNumRecords = (this.estimatedNumRecords /count) >= 1 ?
						(this.estimatedNumRecords /count) : 1;
			}
			else {
				this.estimatedNumRecords = -1;
			}
		}
		
		
		// Estimate output size
		if (this.estimatedNumRecords != -1 && hints.getAvgBytesPerRecord() != -1.0f) {
			this.estimatedOutputSize = (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) >= 1 ? 
				(long) (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) : 1;
		}
		else {
			this.estimatedOutputSize = this.inputSize;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// no children, so nothing to compute
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		// because there are no inputs, there are no unclosed branches.
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeAlternativePlans()
	 */
	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		if (this.candidate != null) {
			return this.candidate;
		}
		
		SourcePlanNode candidate = new SourcePlanNode(this);
		
		final Costs costs = new Costs(0, 0);
		if (FileInputFormat.class.isAssignableFrom(getPactContract().getFormatClass())) {
			costs.addSecondaryStorageCost(this.inputSize);
		}
		candidate.setCosts(costs);

		// since there is only a single plan for the data-source, return a list with that element only
		List<PlanNode> plans = new ArrayList<PlanNode>(1);
		plans.add(candidate);

		if (isBranching()) {
			this.candidate = plans;
		}
		return plans;
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldConstant(int, int)
	 */
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}
}
