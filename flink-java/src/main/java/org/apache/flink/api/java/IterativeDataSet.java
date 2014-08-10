/**
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

package org.apache.flink.api.java;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorRegistry;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.operators.SingleInputOperator;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.types.Value;

/**
 * The IterativeDataSet represents the start of an iteration. It is created from the DataSet that 
 * represents the initial solution set via the {@link DataSet#iterate(int)} method.
 * 
 * @param <T> The data type of set that is the input and feedback of the iteration.
 *
 * @see DataSet#iterate(int)
 */
public class IterativeDataSet<T> extends SingleInputOperator<T, T, IterativeDataSet<T>> {

	private final AggregatorRegistry aggregators = new AggregatorRegistry();
	
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
	 * Gets the maximum number of iterations.
	 * 
	 * @return The maximum number of iterations.
	 */
	public int getMaxIterations() {
		return maxIterations;
	}
	
	/**
	 * Registers an {@link Aggregator} for the iteration. Aggregators can be used to maintain simple statistics during the
	 * iteration, such as number of elements processed. The aggregators compute global aggregates: After each iteration step,
	 * the values are globally aggregated to produce one aggregate that represents statistics across all parallel instances.
	 * The value of an aggregator can be accessed in the next iteration.
	 * <p>
	 * Aggregators can be accessed inside a function via the
	 * {@link org.apache.flink.api.common.functions.AbstractRichFunction#getIterationRuntimeContext()} method.
	 * 
	 * @param name The name under which the aggregator is registered.
	 * @param aggregator The aggregator class.
	 * 
	 * @return The IterativeDataSet itself, to allow chaining function calls.
	 */
	public IterativeDataSet<T> registerAggregator(String name, Aggregator<?> aggregator) {
		this.aggregators.registerAggregator(name, aggregator);
		return this;
	}
	
	/**
	 * Registers an {@link Aggregator} for the iteration together with a {@link ConvergenceCriterion}. For a general description
	 * of aggregators, see {@link #registerAggregator(String, Aggregator)} and {@link Aggregator}.
	 * At the end of each iteration, the convergence criterion takes the aggregator's global aggregate value and decided whether
	 * the iteration should terminate. A typical use case is to have an aggregator that sums up the total error of change
	 * in an iteration step and have to have a convergence criterion that signals termination as soon as the aggregate value
	 * is below a certain threshold.
	 * 
	 * @param name The name under which the aggregator is registered.
	 * @param aggregator The aggregator class.
	 * @param convergenceCheck The convergence criterion.
	 * 
	 * @return The IterativeDataSet itself, to allow chaining function calls.
	 */
	public <X extends Value> IterativeDataSet<T> registerAggregationConvergenceCriterion(
			String name, Aggregator<X> aggregator, ConvergenceCriterion<X> convergenceCheck)
	{
		this.aggregators.registerAggregationConvergenceCriterion(name, aggregator, convergenceCheck);
		return this;
	}
	
	/**
	 * Gets the registry for aggregators. On the registry, one can add {@link Aggregator}s and an aggregator-based 
	 * {@link ConvergenceCriterion}. This method offers an alternative way to registering the aggregators via
	 * {@link #registerAggregator(String, Aggregator)} and {@link #registerAggregationConvergenceCriterion(String, Aggregator, ConvergenceCriterion)
)}.
	 * 
	 * @return The registry for aggregators.
	 */
	public AggregatorRegistry getAggregators() {
		return aggregators;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	protected org.apache.flink.api.common.operators.SingleInputOperator<T, T, ?> translateToDataFlow(Operator<T> input) {
		// All the translation magic happens when the iteration end is encountered.
		throw new RuntimeException("Error while creating the data flow plan for an iteration: The iteration end was not specified correctly.");
	}
}
