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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.OutputContract;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a data source.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DataSourceNode extends OptimizerNode {
	List<DataSourceNode> cachedPlans; // the cache in case there are multiple outputs;

	private long fileSize = -1; // the size of the input file. unknown by default.

	/**
	 * Creates a new DataSourceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data source contract object.
	 */
	public DataSourceNode(DataSourceContract<?, ?> pactContract) {
		super(pactContract);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Copy constructor to create a copy of the data-source object for the process of plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected DataSourceNode(DataSourceNode template, GlobalProperties gp, LocalProperties lp) {
		super(template, gp, lp);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#determineOutputContractFromStub()
	 */
	// @Override
	// protected OutputContract determineOutputContractFromStub()
	// {
	// return OutputContract.getOutputContract(getPactContract().getOutputContract());
	// }

	/**
	 * Gets the size of the input file.
	 * 
	 * @return The size of the input file, or -1, if the size is unknown.
	 */
	public long getFileSize() {
		return this.fileSize;
	}

	/**
	 * Gets the fully qualified path to the input file.
	 * 
	 * @return The path to the input file.
	 */
	public String getFilePath() {
		return getPactContract().getFilePath();
	}

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	public DataSourceContract<?, ?> getPactContract() {
		return (DataSourceContract<?, ?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Data Source";
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
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.<PactConnection> emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		// TODO: A exception should be thrown here?
		// no inputs, so do nothing.
	}

	/**
	 * Causes this node to compute its output estimates (such as number of rows, size in bytes)
	 * based on the file size and the compiler hints. The compiler hints are instantiated with
	 * conservative default values which are used if no other values are provided.
	 */
	public void computeOutputEstimates(DataStatistics statistics) {
		CompilerHints hints = getPactContract().getCompilerHints();

		// see, if we have a statistics object that can tell us a bit about the file
		if (statistics != null) {
			DataStatistics.BasicFileStatistics bfs = statistics.getFileStatistics(getFilePath());

			long len = bfs.getFileSize();
			if (len == DataStatistics.UNKNOWN) {
				PactCompiler.LOG.warn("Pact compiler could not determine the size of file '" + getFilePath() + "'.");
				this.fileSize = -1;
			} else if (len >= 0) {
				this.fileSize = len;
			}

			float avgBytes = bfs.getAvgBytesPerRecord();
			if (avgBytes > 0.0f && hints.getAvgBytesPerRecord() < 1.0f) {
				hints.setAvgBytesPerRecord(avgBytes);
			}
		} else {
			this.fileSize = -1;
		}

		// the output size is equal to the file size
		this.estimatedOutputSize = getFileSize();

		// initialize the others to unknown
		this.estimatedNumRecords = -1;
		this.estimatedKeyCardinality = -1;

		// the estimated number of rows is depending on the average row width
		if (hints.getAvgBytesPerRecord() >= 1.0f && this.estimatedOutputSize > 0) {
			this.estimatedNumRecords = (long) (this.estimatedOutputSize / hints.getAvgBytesPerRecord()) + 1;
		}

		// the key cardinality is either explicitly specified, or we assume for robustness reasons
		// that every record has a unique key. key cardinality overestimation results in more robust plans
		if (hints.getKeyCardinality() < 1) {
			this.estimatedKeyCardinality = this.estimatedNumRecords;
		} else {
			this.estimatedKeyCardinality = hints.getKeyCardinality();

			// if we have the key cardinality and an average number of values per key, we can estimate the number
			// of rows
			if (this.estimatedNumRecords == -1 && hints.getAvgNumValuesPerKey() >= 1.0f) {
				this.estimatedNumRecords = (long) (this.estimatedKeyCardinality * hints.getAvgNumValuesPerKey()) + 1;

				if (this.estimatedOutputSize == -1 && hints.getAvgBytesPerRecord() >= 1.0f) {
					this.estimatedOutputSize = (long) (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) + 1;
				}
			}
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
	public List<DataSourceNode> getAlternativePlans(CostEstimator estimator) {
		if (cachedPlans != null) {
			return cachedPlans;
		}

		GlobalProperties gp = new GlobalProperties();
		LocalProperties lp = new LocalProperties();

		// first, compute the properties of the output
		if (getOutputContract() == OutputContract.UniqueKey) {
			gp.setKeyUnique(true);
			gp.setPartitioning(PartitionProperty.ANY);

			lp.setKeyUnique(true);
			lp.setKeysGrouped(true);
		}

		DataSourceNode candidate = new DataSourceNode(this, gp, lp);

		// compute the costs
		candidate.setCosts(new Costs(0, this.fileSize));

		// since there is only a single plan for the data-source, return a list with that element only
		List<DataSourceNode> plans = Collections.singletonList(candidate);

		if (isBranching()) {
			this.cachedPlans = plans;
		}

		return plans;
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
