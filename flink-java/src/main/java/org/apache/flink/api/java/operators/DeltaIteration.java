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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorRegistry;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Value;

import java.util.Arrays;

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

	private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

	private ResourceSpec minResources = ResourceSpec.DEFAULT;

	private ResourceSpec preferredResources = ResourceSpec.DEFAULT;

	private boolean solutionSetUnManaged;

	public DeltaIteration(ExecutionEnvironment context, TypeInformation<ST> type, DataSet<ST> solutionSet, DataSet<WT> workset, Keys<ST> keys, int maxIterations) {
		initialSolutionSet = solutionSet;
		initialWorkset = workset;
		solutionSetPlaceholder = new SolutionSetPlaceHolder<>(context, solutionSet.getType(), this);
		worksetPlaceholder = new WorksetPlaceHolder<>(context, workset.getType());
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
	 *
	 * <p>Consider the following example:
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
	 *
	 * <p>Consider the following example:
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
		OperatorValidationUtils.validateParallelism(parallelism);
		this.parallelism = parallelism;
		return this;
	}

	/**
	 * Gets the iteration's parallelism.
	 *
	 * @return The iteration's parallelism, or {@link ExecutionConfig#PARALLELISM_DEFAULT} if not set.
	 */
	public int getParallelism() {
		return parallelism;
	}

	//	---------------------------------------------------------------------------
	//	 Fine-grained resource profiles are an incomplete work-in-progress feature
	//	 The setters are hence private at this point.
	//	---------------------------------------------------------------------------

	/**
	 * Sets the minimum and preferred resources for the iteration. This overrides the default resources.
	 * The lower and upper resource limits will be considered in dynamic resource resize feature for future plan.
	 *
	 * @param minResources The minimum resources for the iteration.
	 * @param preferredResources The preferred resources for the iteration.
	 * @return The iteration with set minimum and preferred resources.
	 */
	private DeltaIteration<ST, WT> setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
		OperatorValidationUtils.validateMinAndPreferredResources(minResources, preferredResources);

		this.minResources = minResources;
		this.preferredResources = preferredResources;

		return this;
	}

	/**
	 * Sets the resources for the iteration, and the minimum and preferred resources are the same by default.
	 *	The lower and upper resource limits will be considered in dynamic resource resize feature for future plan.
	 *
	 * @param resources The resources for the iteration.
	 * @return The iteration with set minimum and preferred resources.
	 */
	private DeltaIteration<ST, WT> setResources(ResourceSpec resources) {
		OperatorValidationUtils.validateResources(resources);

		this.minResources = resources;
		this.preferredResources = resources;

		return this;
	}

	/**
	 * Gets the minimum resources from this iteration. If no minimum resources have been set,
	 * it returns the default empty resource.
	 *
	 * @return The minimum resources of the iteration.
	 */
	@PublicEvolving
	public ResourceSpec getMinResources() {
		return this.minResources;
	}

	/**
	 * Gets the preferred resources from this iteration. If no preferred resources have been set,
	 * it returns the default empty resource.
	 *
	 * @return The preferred resources of the iteration.
	 */
	@PublicEvolving
	public ResourceSpec getPreferredResources() {
		return this.preferredResources;
	}

	/**
	 * Registers an {@link Aggregator} for the iteration. Aggregators can be used to maintain simple statistics during the
	 * iteration, such as number of elements processed. The aggregators compute global aggregates: After each iteration step,
	 * the values are globally aggregated to produce one aggregate that represents statistics across all parallel instances.
	 * The value of an aggregator can be accessed in the next iteration.
	 *
	 * <p>Aggregators can be accessed inside a function via the
	 * {@link org.apache.flink.api.common.functions.AbstractRichFunction#getIterationRuntimeContext()} method.
	 *
	 * @param name The name under which the aggregator is registered.
	 * @param aggregator The aggregator class.
	 *
	 * @return The DeltaIteration itself, to allow chaining function calls.
	 */
	@PublicEvolving
	public DeltaIteration<ST, WT> registerAggregator(String name, Aggregator<?> aggregator) {
		this.aggregators.registerAggregator(name, aggregator);
		return this;
	}

	/**
	 * Registers an {@link Aggregator} for the iteration together with a {@link ConvergenceCriterion}. For a general description
	 * of aggregators, see {@link #registerAggregator(String, Aggregator)} and {@link Aggregator}.
	 * At the end of each iteration, the convergence criterion takes the aggregator's global aggregate value and decides whether
	 * the iteration should terminate. A typical use case is to have an aggregator that sums up the total error of change
	 * in an iteration step and have to have a convergence criterion that signals termination as soon as the aggregate value
	 * is below a certain threshold.
	 *
	 * @param name The name under which the aggregator is registered.
	 * @param aggregator The aggregator class.
	 * @param convergenceCheck The convergence criterion.
	 *
	 * @return The DeltaIteration itself, to allow chaining function calls.
	 */
	@PublicEvolving
	public <X extends Value> DeltaIteration<ST, WT> registerAggregationConvergenceCriterion(
			String name, Aggregator<X> aggregator, ConvergenceCriterion<X> convergenceCheck) {
		this.aggregators.registerAggregationConvergenceCriterion(name, aggregator, convergenceCheck);
		return this;
	}

	/**
	 * Gets the registry for aggregators for the iteration.
	 *
	 * @return The registry with all aggregators.
	 */
	@PublicEvolving
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
