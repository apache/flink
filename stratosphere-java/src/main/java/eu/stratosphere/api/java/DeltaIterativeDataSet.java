/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.operators.TwoInputOperator;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 * The DeltaIterativeDataSet represents the start of a delta iteration. It is created from the DataSet that 
 * represents the initial solution set via the {@link DataSet#iterateDelta(DataSet, int, int)} method.
 * 
 * @param<ST> The data type of the solution set.
 * @param<WT> The data type of the workset (the feedback data set).
 *
 * @see DataSet#iterateDelta(DataSet, int, int)
 * @see DataSet#iterateDelta(DataSet, int, int[])
 */
public class DeltaIterativeDataSet<ST, WT> extends TwoInputOperator<ST, WT, ST, DeltaIterativeDataSet<ST, WT>> {
	
	private DeltaIterativeDataSet<ST, WT> solutionSetPlaceholder;
	
	private Keys<ST> keys;
	
	private int maxIterations;

	DeltaIterativeDataSet(ExecutionEnvironment context, TypeInformation<ST> type, DataSet<ST> solutionSet, DataSet<WT> workset, Keys<ST> keys, int maxIterations) {
		super(solutionSet, workset, type);
		
		solutionSetPlaceholder = new DeltaIterativeDataSet<ST, WT>(context, type, solutionSet, workset, true);
		this.keys = keys;
		this.maxIterations = maxIterations;
	}
	
	private DeltaIterativeDataSet(ExecutionEnvironment context, TypeInformation<ST> type, DataSet<ST> solutionSet, DataSet<WT> workset, boolean placeholder) {
		super(solutionSet, workset, type);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Closes the delta iteration. This method defines the end of the delta iteration's function.
	 * 
	 * @param solutionSetDelta The delta for the solution set. The delta will be merged into the solution set at the end of
	 *                         each iteration.
	 * @param newWorkset The new workset (feedback data set) that will be fed back to the next iteration.
	 * @return The DataSet that represents the result of the iteration, after the computation has terminated.
	 * 
	 * @see DataSet#iterateDelta(DataSet, int, int)
	 */
	public DataSet<ST> closeWith(DataSet<ST> solutionSetDelta, DataSet<WT> newWorkset) {
		return new DeltaIterativeResultDataSet<ST, WT>(getExecutionEnvironment(), getType(), newWorkset.getType(), this, solutionSetPlaceholder, solutionSetDelta, newWorkset, keys, maxIterations);
	}
	
	/**
	 * Gets the solution set of the delta iteration. The solution set represents the state that is kept across iterations.
	 * 
	 * @return The solution set of the delta iteration.
	 */
	public DataSet<ST> getSolutionSet() {
		return solutionSetPlaceholder;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	protected Operator translateToDataFlow(Operator input1, Operator input2) {
		// All the translation magic happens when the iteration end is encountered.
		throw new UnsupportedOperationException("This should never happen.");
	}
}
