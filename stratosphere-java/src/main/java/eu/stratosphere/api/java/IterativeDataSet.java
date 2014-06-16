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

import eu.stratosphere.api.common.accumulators.ConvergenceCriterion;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.operators.SingleInputOperator;
import eu.stratosphere.types.TypeInformation;

/**
 * The IterativeDataSet represents the start of an iteration. It is created from the DataSet that 
 * represents the initial solution set via the {@link DataSet#iterate(int)} method.
 * 
 * @param <T> The data type of set that is the input and feedback of the iteration.
 *
 * @see DataSet#iterate(int)
 */
public class IterativeDataSet<T> extends SingleInputOperator<T, T, IterativeDataSet<T>> {
	
	private int maxIterations;

	IterativeDataSet(ExecutionEnvironment context, TypeInformation<T> type, DataSet<T> input, int maxIterations) {
		super(input, type);
		this.maxIterations = maxIterations;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Closes the iteration. This method defines the end of the iterative program part.
	 * 
	 * @param iterationResult The data set that will be fed back to the next iteration.
	 * @return The DataSet that represents the result of the iteration, after the computation has terminated.
	 * 
	 * @see DataSet#iterate(int)
	 */
	public DataSet<T> closeWith(DataSet<T> iterationResult) {
		return new BulkIterationResultSet<T>(getExecutionEnvironment(), getType(), this, iterationResult);
	}
	
	/**
	 * Closes the iteration and specifies a termination criterion. This method defines the end of
	 * the iterative program part.
	 * <p>
	 * The termination criterion is a means of dynamically signaling the iteration to halt. It is expressed via a data
	 * set that will trigger to halt the loop as soon as the data set is empty. A typical way of using the termination
	 * criterion is to have a filter that filters out all elements that are considered non-converged. As soon as no more
	 * such elements exist, the iteration finishes.
	 * 
	 * @param iterationResult The data set that will be fed back to the next iteration.
	 * @return The DataSet that represents the result of the iteration, after the computation has terminated.
	 * 
	 * @see DataSet#iterate(int)
	 */
	public DataSet<T> closeWith(DataSet<T> iterationResult, DataSet<?> terminationCriterion) {
		return new BulkIterationResultSet<T>(getExecutionEnvironment(), getType(), this, iterationResult, terminationCriterion);
	}
	
	/**
	 * Closes the iteration and specifies a convergence criterion. This method defines the end of
	 * the iterative program part.
	 * <p>
	 * The convergence criterion is a means of dynamically signaling the iteration to halt. It is expressed via an object
	 * of type ConvergenceCriterion and the name of an Accumulator the convergence criterion is based on. After every superstep
	 * the isConverged() method of the given criterion is called with the current global value of the given Accumulator. 
	 * It is therefore necessary that the type of the ConvergenceCriterion is equal to the return type of the Accumulator.
	 * 
	 * @param iterationResult The data set that will be fed back to the next iteration.
	 * @param convergenceCriterion The object that specifies the criterion
	 * @param accumulatorName The Name of the Accumulator the criterion is based on
	 * @return The DataSet that represents the result of the iteration, after the computation has terminated.
	 * 
	 * @see DataSet#iterate(int)
	 */
	public DataSet<T> closeWith(DataSet<T> iterationResult, ConvergenceCriterion<?> convergenceCriterion, String accumulatorName) {
		return new BulkIterationResultSet<T>(getExecutionEnvironment(), getType(), this, iterationResult, convergenceCriterion, accumulatorName);
	}

	/**
	 * Gets the maximum number of iterations.
	 * 
	 * @return The maximum number of iterations.
	 */
	public int getMaxIterations() {
		return maxIterations;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	protected eu.stratosphere.api.common.operators.SingleInputOperator<T, T, ?> translateToDataFlow(Operator<T> input) {
		// All the translation magic happens when the iteration end is encountered.
		throw new UnsupportedOperationException("This should never happen.");
	}
}
