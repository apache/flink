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

import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.typeutils.TypeInformation;

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
public class DeltaIteration<ST, WT> {

	private final DataSet<ST> initialSolutionSet;
	private final DataSet<WT> initialWorkset;
	
	private final SolutionSetPlaceHolder<ST> solutionSetPlaceholder;
	private final WorksetPlaceHolder<WT> worksetPlaceholder;

	private final Keys<ST> keys;
	
	private final int maxIterations;

	DeltaIteration(ExecutionEnvironment context, TypeInformation<ST> type, DataSet<ST> solutionSet, DataSet<WT> workset, Keys<ST> keys, int maxIterations) {
		initialSolutionSet = solutionSet;
		initialWorkset = workset;
		solutionSetPlaceholder = new SolutionSetPlaceHolder<ST>(context, solutionSet.getType());
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
	 * A {@link DataSet} that acts as a placeholder for the solution set during the iteration.
	 * 
	 * @param <ST> The type of the elements in the solution set.
	 */
	public static class SolutionSetPlaceHolder<ST> extends DataSet<ST>{
		private SolutionSetPlaceHolder(ExecutionEnvironment context, TypeInformation<ST> type) {
			super(context, type);
		}
	}

	/**
	 * A {@link DataSet} that acts as a placeholder for the workset during the iteration.
	 *
	 * @param <WT> The data type of the elements in the workset.
	 */
	public static class WorksetPlaceHolder<WT> extends DataSet<WT>{
		private WorksetPlaceHolder(ExecutionEnvironment context, TypeInformation<WT> type) {
			super(context, type);
		}
	}
}
