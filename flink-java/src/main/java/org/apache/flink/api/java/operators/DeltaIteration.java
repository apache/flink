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

package org.apache.flink.api.java.operators;

import java.util.Arrays;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorRegistry;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import com.google.common.base.Preconditions;

/**
 * The DeltaIteration represents the start of a delta iteration. It is created from the DataSet that
 * represents the initial solution set via the {@link DataSet#iterateDelta(DataSet, int, int...)} method.
 * 
 * @param <ST> The data type of the solution set.
 * @param <WT> The data type of the workset (the feedback data set).
 *
 * @see DataSet#iterateDelta(DataSet, int, int...)
 * @see DataSet#iterateDelta(DataSet, int, int[])
 */
@Public
public class DeltaIteration<ST, WT> {
	
	private final AggregatorRegistry aggregators = new AggregatorRegistry();
	
	private final DataSet<ST> initialSolutionSet;
	private final DataSet<WT> initialWorkset;
	
	private final SolutionSetPlaceHolder<ST> solutionSetPlaceholder;
	private final WorksetPlaceHolder<WT> worksetPlaceholder;

	private final Keys<ST> keys;
	
	private final int maxIterations;
	
	private String name;
	
	private int parallelism = -1;
	
	private boolean solutionSetUnManaged;
	
	
	public DeltaIteration(ExecutionEnvironment context, TypeInformation<ST> type, DataSet<ST> solutionSet, DataSet<WT> workset, Keys<ST> keys, int maxIterations) {
		initialSolutionSet = solutionSet;
		initialWorkset = workset;
		solutionSetPlaceholder = new SolutionSetPlaceHolder<ST>(context, solutionSet.getType(), this);
		worksetPlaceholder = new WorksetPlaceHolder<WT>(context, workset.getType());
		this.keys = keys;
		this.maxIterations = maxIterations;
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
	 * @see DataSet#iterateDelta(DataSet, int, int...)
	 */
	public DataSet<ST> closeWith(DataSet<ST> solutionSetDelta, DataSet<WT> newWorkset) {
		return new DeltaIterationResultSet<ST, WT>(initialSolutionSet.getExecutionEnvironment(),
				initialSolutionSet.getType(), initialWorkset.getType(), this, solutionSetDelta, newWorkset, keys, maxIterations);
	}

	/**
	 * Gets the initial solution set. This is the data set on which the delta iteration was started.
	 * <p>
	 * Consider the following example:
	 * <pre>
	 * {@code
	 * DataSet<MyType> solutionSetData = ...;
	 * DataSet<AnotherType> worksetData = ...;
	 * 
	 * DeltaIteration<MyType, AnotherType> iteration = solutionSetData.iteratorDelta(worksetData, 10, ...);
	 * }
	 * </pre>
	 * The <tt>solutionSetData</tt> would be the data set returned by {@code iteration.getInitialSolutionSet();}.
	 * 
	 * @return The data set that forms the initial solution set.
	 */
	public DataSet<ST> getInitialSolutionSet() {
		return initialSolutionSet;
	}

	/**
	 * Gets the initial workset. This is the data set passed to the method that starts the delta
	 * iteration.
	 * <p>
	 * Consider the following example:
	 * <pre>
	 * {@code
	 * DataSet<MyType> solutionSetData = ...;
	 * DataSet<AnotherType> worksetData = ...;
	 * 
	 * DeltaIteration<MyType, AnotherType> iteration = solutionSetData.iteratorDelta(worksetData, 10, ...);
	 * }
	 * </pre>
	 * The <tt>worksetData</tt> would be the data set returned by {@code iteration.getInitialWorkset();}.
	 * 
	 * @return The data set that forms the initial workset.
	 */
	public DataSet<WT> getInitialWorkset() {
		return initialWorkset;
	}

	/**
	 * Gets the solution set of the delta iteration. The solution set represents the state that is kept across iterations.
	 * 
	 * @return The solution set of the delta iteration.
	 */
	public SolutionSetPlaceHolder<ST> getSolutionSet() {
		return solutionSetPlaceholder;
	}

	/**
	 * Gets the working set of the delta iteration. The working set is constructed by the previous iteration.
	 *
	 * @return The working set of the delta iteration.
	 */
	public WorksetPlaceHolder<WT> getWorkset() {
		return worksetPlaceholder;
	}

	/**
	 * Sets the name for the iteration. The name is displayed in logs and messages.
	 * 
	 * @param name The name for the iteration.
	 * @return The iteration object, for function call chaining.
	 */
	public DeltaIteration<ST, WT> name(String name) {
		this.name = name;
		return this;
	}
	
	/**
	 * Gets the name from this iteration.
	 * 
	 * @return The name of the iteration.
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Sets the parallelism for the iteration.
	 *
	 * @param parallelism The parallelism.
	 * @return The iteration object, for function call chaining.
	 */
	public DeltaIteration<ST, WT> parallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == -1, "The parallelism must be positive, or -1 (use default).");
		this.parallelism = parallelism;
		return this;
	}
	
	/**
	 * Gets the iteration's parallelism.
	 * 
	 * @return The iterations parallelism, or -1, if not set.
	 */
	public int getParallelism() {
		return parallelism;
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
	 * @return The DeltaIteration itself, to allow chaining function calls.
	 */
	@Experimental
	public DeltaIteration<ST, WT> registerAggregator(String name, Aggregator<?> aggregator) {
		this.aggregators.registerAggregator(name, aggregator);
		return this;
	}
	
	/**
	 * Gets the registry for aggregators for the iteration.
	 * 
	 * @return The registry with all aggregators.
	 */
	@Experimental
	public AggregatorRegistry getAggregators() {
		return this.aggregators;
	}
	
	/**
	 * Sets whether to keep the solution set in managed memory (safe against heap exhaustion) or unmanaged memory
	 * (objects on heap).
	 * 
	 * @param solutionSetUnManaged True to keep the solution set in unmanaged memory, false to keep it in managed memory.
	 * 
	 * @see #isSolutionSetUnManaged()
	 */
	public void setSolutionSetUnManaged(boolean solutionSetUnManaged) {
		this.solutionSetUnManaged = solutionSetUnManaged;
	}
	
	/**
	 * gets whether the solution set is in managed or unmanaged memory.
	 * 
	 * @return True, if the solution set is in unmanaged memory (object heap), false if in managed memory.
	 * 
	 * @see #setSolutionSetUnManaged(boolean)
	 */
	public boolean isSolutionSetUnManaged() {
		return solutionSetUnManaged;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * A {@link DataSet} that acts as a placeholder for the solution set during the iteration.
	 * 
	 * @param <ST> The type of the elements in the solution set.
	 */
	@Public
	public static class SolutionSetPlaceHolder<ST> extends DataSet<ST>{
		
		private final DeltaIteration<ST, ?> deltaIteration;
		
		private SolutionSetPlaceHolder(ExecutionEnvironment context, TypeInformation<ST> type, DeltaIteration<ST, ?> deltaIteration) {
			super(context, type);
			this.deltaIteration = deltaIteration;
		}
		
		public void checkJoinKeyFields(int[] keyFields) {
			int[] ssKeys = deltaIteration.keys.computeLogicalKeyPositions();
			if (!Arrays.equals(ssKeys, keyFields)) {
				throw new InvalidProgramException("The solution can only be joined/co-grouped with the same keys as the elements are identified with (here: " + Arrays.toString(ssKeys) + ").");
			}
		}
	}

	/**
	 * A {@link DataSet} that acts as a placeholder for the workset during the iteration.
	 *
	 * @param <WT> The data type of the elements in the workset.
	 */
	@Public
	public static class WorksetPlaceHolder<WT> extends DataSet<WT>{
		private WorksetPlaceHolder(ExecutionEnvironment context, TypeInformation<WT> type) {
			super(context, type);
		}
	}
}
