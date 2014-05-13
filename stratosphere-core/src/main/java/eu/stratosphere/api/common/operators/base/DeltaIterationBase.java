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

package eu.stratosphere.api.common.operators.base;

import eu.stratosphere.api.common.aggregators.AggregatorRegistry;
import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.operators.BinaryOperatorInformation;
import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.IterationOperator;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.OperatorInformation;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.util.Visitor;

/**
 * A DeltaIteration is similar to a {@link BulkIterationBase},
 * but maintains state across the individual iteration steps. The state is called the <i>solution set</i>, can be obtained
 * via {@link #getSolutionSet()}, and be accessed by joining (or CoGrouping) with it. The solution
 * set is updated by producing a delta for it, which is merged into the solution set at the end of each iteration step.
 * <p>
 * The delta iteration must be closed by setting a delta for the solution set ({@link #setSolutionSetDelta(eu.stratosphere.api.common.operators.Operator)})
 * and the new workset (the data set that will be fed back, {@link #setNextWorkset(eu.stratosphere.api.common.operators.Operator)}).
 * The DeltaIteration itself represents the result after the iteration has terminated.
 * Delta iterations terminate when the feed back data set (the workset) is empty.
 * In addition, a maximum number of steps is given as a fall back termination guard.
 * <p>
 * Elements in the solution set are uniquely identified by a key. When merging the solution set delta, contained elements
 * with the same key are replaced.
 * <p>
 * This class is a subclass of {@code DualInputOperator}. The solution set is considered the first input, the
 * workset is considered the second input.
 */
public class DeltaIterationBase<ST, WT> extends DualInputOperator<ST, WT, ST, AbstractFunction> implements IterationOperator {

	private final Operator<ST> solutionSetPlaceholder;

	private final Operator<WT> worksetPlaceholder;

	private Operator<ST> solutionSetDelta;

	private Operator<WT> nextWorkset;

	/**
	 * The positions of the keys in the solution tuple.
	 */
	private final int[] solutionSetKeyFields;

	/**
	 * The maximum number of iterations. Possibly used only as a safeguard.
	 */
	private int maxNumberOfIterations = -1;

	private final AggregatorRegistry aggregators = new AggregatorRegistry();

	// --------------------------------------------------------------------------------------------

	public DeltaIterationBase(BinaryOperatorInformation<ST, WT, ST> operatorInfo, int keyPosition) {
		this(operatorInfo, new int[] {keyPosition});
	}

	public DeltaIterationBase(BinaryOperatorInformation<ST, WT, ST> operatorInfo, int[] keyPositions) {
		this(operatorInfo, keyPositions, "<Unnamed Workset-Iteration>");
	}

	public DeltaIterationBase(BinaryOperatorInformation<ST, WT, ST> operatorInfo, int keyPosition, String name) {
		this(operatorInfo, new int[] {keyPosition}, name);
	}

	public DeltaIterationBase(BinaryOperatorInformation<ST, WT, ST> operatorInfo, int[] keyPositions, String name) {
		super(new UserCodeClassWrapper<AbstractFunction>(AbstractFunction.class), operatorInfo, name);
		this.solutionSetKeyFields = keyPositions;
		solutionSetPlaceholder = new SolutionSetPlaceHolder<ST>(this, new OperatorInformation<ST>(operatorInfo.getFirstInputType()));
		worksetPlaceholder = new WorksetPlaceHolder<WT>(this, new OperatorInformation<WT>(operatorInfo.getSecondInputType()));
	}
	
	// --------------------------------------------------------------------------------------------
	
	public int[] getSolutionSetKeyFields() {
		return this.solutionSetKeyFields;
	}
	
	public void setMaximumNumberOfIterations(int maxIterations) {
		this.maxNumberOfIterations = maxIterations;
	}
	
	public int getMaximumNumberOfIterations() {
		return this.maxNumberOfIterations;
	}
	
	@Override
	public AggregatorRegistry getAggregators() {
		return this.aggregators;
	}
	
	// --------------------------------------------------------------------------------------------
	// Getting / Setting of the step function input place-holders
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the contract that represents the solution set for the step function.
	 * 
	 * @return The solution set for the step function.
	 */
	public Operator<ST> getSolutionSet() {
		return this.solutionSetPlaceholder;
	}

	/**
	 * Gets the contract that represents the workset for the step function.
	 * 
	 * @return The workset for the step function.
	 */
	public Operator<WT> getWorkset() {
		return this.worksetPlaceholder;
	}

	/**
	 * Sets the contract of the step function that represents the next workset. This contract is considered 
	 * one of the two sinks of the step function (the other one being the solution set delta).
	 * 
	 * @param result The contract representing the next workset.
	 */
	public void setNextWorkset(Operator<WT> result) {
		this.nextWorkset = result;
	}
	
	/**
	 * Gets the contract that has been set as the next workset.
	 * 
	 * @return The contract that has been set as the next workset.
	 */
	public Operator<WT> getNextWorkset() {
		return this.nextWorkset;
	}
	
	/**
	 * Sets the contract of the step function that represents the solution set delta. This contract is considered
	 * one of the two sinks of the step function (the other one being the next workset).
	 * 
	 * @param delta The contract representing the solution set delta.
	 */
	public void setSolutionSetDelta(Operator<ST> delta) {
		this.solutionSetDelta = delta;
	}
	
	/**
	 * Gets the contract that has been set as the solution set delta.
	 * 
	 * @return The contract that has been set as the solution set delta.
	 */
	public Operator<ST> getSolutionSetDelta() {
		return this.solutionSetDelta;
	}

	// --------------------------------------------------------------------------------------------
	// Getting / Setting the Inputs
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns the initial solution set input, or null, if none is set.
	 * 
	 * @return The iteration's initial solution set input.
	 */
	public Operator<ST> getInitialSolutionSet() {
		return getFirstInput();
	}
	
	/**
	 * Returns the initial workset input, or null, if none is set.
	 * 
	 * @return The iteration's workset input.
	 */
	public Operator<WT> getInitialWorkset() {
		return getSecondInput();
	}
	
	/**
	 * Sets the given input as the initial solution set.
	 * 
	 * @param input The contract to set the initial solution set.
	 */
	public void setInitialSolutionSet(Operator<ST> input) {
		setFirstInput(input);
	}
	
	/**
	 * Sets the given input as the initial workset.
	 * 
	 * @param input The contract to set as the initial workset.
	 */
	public void setInitialWorkset(Operator<WT> input) {
		setSecondInput(input);
	}
	
	// --------------------------------------------------------------------------------------------
	// Place-holder Operators
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Specialized operator to use as a recognizable place-holder for the working set input to the
	 * step function.
	 */
	public static class WorksetPlaceHolder<WT> extends Operator<WT> {

		private final DeltaIterationBase<?, WT> containingIteration;

		public WorksetPlaceHolder(DeltaIterationBase<?, WT> container, OperatorInformation<WT> operatorInfo) {
			super(operatorInfo, "Workset Place Holder");
			this.containingIteration = container;
		}

		public DeltaIterationBase<?, WT> getContainingWorksetIteration() {
			return this.containingIteration;
		}

		@Override
		public void accept(Visitor<Operator<?>> visitor) {
			visitor.preVisit(this);
			visitor.postVisit(this);
		}

		@Override
		public UserCodeWrapper<?> getUserCodeWrapper() {
			return null;
		}
	}
	
	/**
	 * Specialized operator to use as a recognizable place-holder for the solution set input to the
	 * step function.
	 */
	public static class SolutionSetPlaceHolder<ST> extends Operator<ST> {

		private final DeltaIterationBase<ST, ?> containingIteration;

		public SolutionSetPlaceHolder(DeltaIterationBase<ST, ?> container, OperatorInformation<ST> operatorInfo) {
			super(operatorInfo, "Solution Set Place Holder");
			this.containingIteration = container;
		}

		public DeltaIterationBase<ST, ?> getContainingWorksetIteration() {
			return this.containingIteration;
		}

		@Override
		public void accept(Visitor<Operator<?>> visitor) {
			visitor.preVisit(this);
			visitor.postVisit(this);
		}

		@Override
		public UserCodeWrapper<?> getUserCodeWrapper() {
			return null;
		}
	}
}
