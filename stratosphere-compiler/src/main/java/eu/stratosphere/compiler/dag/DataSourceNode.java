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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.api.common.operators.CompilerHints;
import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.io.UnsplittableInput;
import eu.stratosphere.api.record.io.FileInputFormat;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.costs.Costs;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SourcePlanNode;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Visitor;

/**
 * The optimizer's internal representation of a data source.
 */
public class DataSourceNode extends OptimizerNode {
	
	private long inputSize;			//the size of the input in bytes
	
	private final boolean unsplittable;

	/**
	 * Creates a new DataSourceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data source contract object.
	 */
	public DataSourceNode(GenericDataSource<?> pactContract) {
		super(pactContract);
		
		if (pactContract.getUserCodeWrapper().getUserCodeClass() == null) {
			throw new IllegalArgumentException("Input format has not been set.");
		}
		
		if (UnsplittableInput.class.isAssignableFrom(pactContract.getUserCodeWrapper().getUserCodeClass())) {
			setDegreeOfParallelism(1);
			setSubtasksPerInstance(1);
			this.unsplittable = true;
		} else {
			this.unsplittable = false;
		}
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

	@Override
	public String getName() {
		return "Data Source";
	}

	@Override
	public boolean isMemoryConsumer() {
		return false;
	}
	

	@Override
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		// if unsplittable, DOP remains at 1
		if (!this.unsplittable) {
			super.setDegreeOfParallelism(degreeOfParallelism);
		}
	}
	

	@Override
	public void setSubtasksPerInstance(int instancesPerMachine) {
		// if unsplittable, DOP remains at 1
		if (!this.unsplittable) {
			super.setSubtasksPerInstance(instancesPerMachine);
		}
	}

	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.<PactConnection>emptyList();
	}

	@Override
	public void setInputs(Map<Operator, OptimizerNode> contractToNode) {
		// do nothing
	}

	/**
	 * Causes this node to compute its output estimates (such as number of rows, size in bytes)
	 * based on the file size and the compiler hints. The compiler hints are instantiated with
	 * conservative default values which are used if no other values are provided.
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		final CompilerHints hints = getPactContract().getCompilerHints();
		
		// initialize basic estimates to unknown
		this.estimatedOutputSize = -1;
		this.inputSize = -1;
		this.estimatedNumRecords = -1;

		// see, if we have a statistics object that can tell us a bit about the file
		if (statistics != null) {
			// instantiate the input format, as this is needed by the statistics 
			InputFormat<?, ?> format = null;
			String inFormatDescription = "<unknown>";
			
			try {
				format = getPactContract().getFormatWrapper().getUserCodeObject();
				Configuration config = getPactContract().getParameters();
				config.setClassLoader(getPactContract().getClass().getClassLoader());
				format.configure(config);
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
			final String statisticsKey = getPactContract().getStatisticsKey();
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
				if (len == BaseStatistics.SIZE_UNKNOWN) {
					if (PactCompiler.LOG.isWarnEnabled())
						PactCompiler.LOG.warn("Pact compiler could not determine the size of input '" + inFormatDescription + "'.");
				}
				else if (len >= 0) {
					this.inputSize = len;
				}
				
				final float avgBytes = bs.getAverageRecordWidth();
				if (avgBytes != BaseStatistics.AVG_RECORD_BYTES_UNKNOWN && hints.getAvgBytesPerRecord() <= 0.0f) {
					hints.setAvgBytesPerRecord(avgBytes);
				}
				
				final long card = bs.getNumberOfRecords();
				if (card != BaseStatistics.NUM_RECORDS_UNKNOWN) {
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
		} else {
			this.estimatedOutputSize = this.inputSize;
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
		
		SourcePlanNode candidate = new SourcePlanNode(this, "DataSource("+this.getPactContract().getName()+")");
		candidate.updatePropertiesWithUniqueSets(getUniqueFields());
		
		final Costs costs = new Costs();
		if (FileInputFormat.class.isAssignableFrom(getPactContract().getFormatWrapper().getUserCodeObject().getClass())) {
			estimator.addFileInputCost(this.inputSize, costs);
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
	protected void readConstantAnnotation() {}

	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}
}
