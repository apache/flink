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

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.api.common.aggregators.AggregatorRegistry;
import eu.stratosphere.api.common.aggregators.ConvergenceCriterion;
import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.java.operators.SingleInputOperator;
import eu.stratosphere.api.java.operators.translation.UnaryNodeTranslation;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.types.Value;

/**
 * The IterativeDataSet represents the start of an iteration. It is created from the DataSet that 
 * represents the initial solution set via the {@link DataSet#iterate(int)} method.
 * 
 * @param<T> The data type of set that is the input and feedback of the iteration.
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
		return new IterativeResultDataSet<T>(getExecutionEnvironment(), getType(), this, iterationResult);
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
	 * Aggregators can be accessed inside a function via the {@link AbstractFunction#getIterationRuntimeContext()} method.
	 * 
	 * @param name The name under which the aggregator is registered.
	 * @param aggregator The aggregator class.
	 * 
	 * @return The IterativeDataSet itself, to allow chaining function calls.
	 */
	public <X> IterativeDataSet<T> registerAggregator(String name, Class<? extends Aggregator<?>> aggregator) {
		this.aggregators.registerAggregator(name, aggregator);
		return this;
	}
	
	/**
	 * Registers an {@link Aggregator} for the iteration together with a {@link ConvergenceCriterion}. For a general description
	 * of aggregators, see {@link #registerAggregator(String, Class)} and {@link Aggregator}.
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
			String name, Class<? extends Aggregator<X>> aggregator, Class<? extends ConvergenceCriterion<X>> convergenceCheck)
	{
		this.aggregators.registerAggregationConvergenceCriterion(name, aggregator, convergenceCheck);
		return this;
	}
	
	/**
	 * Gets the registry for aggregators. On the registry, one can add {@link Aggregator}s and an aggregator-based 
	 * {@link ConvergenceCriterion}. This method offers an alternative way to registering the aggregators via
	 * {@link #registerAggregator(String, Class)} and {@link #registerAggregationConvergenceCriterion(String, Class, Class)}.
	 * 
	 * @return The registry for aggregators.
	 */
	public AggregatorRegistry getAggregators() {
		return aggregators;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	protected UnaryNodeTranslation translateToDataFlow() {
		// All the translation magic happens when the iteration end is encountered.
		throw new UnsupportedOperationException("This should never happen.");
	}
}
