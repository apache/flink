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

package eu.stratosphere.api.operators;

import java.util.List;

import eu.stratosphere.api.functions.AbstractFunction;
import eu.stratosphere.api.functions.aggregators.AggregatorRegistry;
import eu.stratosphere.api.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.operators.util.UserCodeWrapper;
import eu.stratosphere.util.Visitor;

/**
 * This class is a subclass of {@code DualInputOperator}. The solution set is considered the first input, the
 * workset is considered the second input.
 */
public class WorksetIteration extends DualInputOperator<AbstractFunction> implements IterationOperator {
	
	private final Operator solutionSetPlaceholder = new SolutionSetPlaceHolder(this);

	private final Operator worksetPlaceholder = new WorksetPlaceHolder(this);

	private Operator solutionSetDelta;

	private Operator nextWorkset;
	
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

	public WorksetIteration(int keyPosition) {
		this(new int[] {keyPosition});
	}
	
	public WorksetIteration(int[] keyPositions) {
		this(keyPositions, "<Unnamed Workset-Iteration>");
	}

	public WorksetIteration(int keyPosition, String name) {
		this(new int[] {keyPosition}, name);
	}
	
	public WorksetIteration(int[] keyPositions, String name) {
		super(new UserCodeClassWrapper<AbstractFunction>(AbstractFunction.class), name);
		this.solutionSetKeyFields = keyPositions;
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
	public Operator getSolutionSet() {
		return this.solutionSetPlaceholder;
	}

	/**
	 * Gets the contract that represents the workset for the step function.
	 * 
	 * @return The workset for the step function.
	 */
	public Operator getWorkset() {
		return this.worksetPlaceholder;
	}

	/**
	 * Sets the contract of the step function that represents the next workset. This contract is considered 
	 * one of the two sinks of the step function (the other one being the solution set delta).
	 * 
	 * @param result The contract representing the next workset.
	 */
	public void setNextWorkset(Operator result) {
		this.nextWorkset = result;
	}
	
	/**
	 * Gets the contract that has been set as the next workset.
	 * 
	 * @return The contract that has been set as the next workset.
	 */
	public Operator getNextWorkset() {
		return this.nextWorkset;
	}
	
	/**
	 * Sets the contract of the step function that represents the solution set delta. This contract is considered
	 * one of the two sinks of the step function (the other one being the next workset).
	 * 
	 * @param delta The contract representing the solution set delta.
	 */
	public void setSolutionSetDelta(Operator delta) {
		this.solutionSetDelta = delta;
	}
	
	/**
	 * Gets the contract that has been set as the solution set delta.
	 * 
	 * @return The contract that has been set as the solution set delta.
	 */
	public Operator getSolutionSetDelta() {
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
	public List<Operator> getInitialSolutionSet() {
		return getFirstInputs();
	}
	
	/**
	 * Returns the initial workset input, or null, if none is set.
	 * 
	 * @return The iteration's workset input.
	 */
	public List<Operator> getInitialWorkset() {
		return getSecondInputs();
	}
	
	/**
	 * Sets the given input as the initial solution set.
	 * 
	 * @param input The contract to set the initial solution set.
	 */
	public void setInitialSolutionSet(Operator ... input) {
		setFirstInput(input);
	}
	
	/**
	 * Sets the given input as the initial workset.
	 * 
	 * @param input The contract to set as the initial workset.
	 */
	public void setInitialWorkset(Operator ... input) {
		setSecondInput(input);
	}

	/**
	 * Sets the given inputs as the initial solution set.
	 * 
	 * @param inputs The contracts to set as the initial solution set.
	 */
	public void setInitialSolutionSet(List<Operator> inputs) {
		setFirstInputs(inputs);
	}

	/**
	 * Sets the given inputs as the initial workset.
	 * 
	 * @param inputs The contracts to set as the initial workset.
	 */
	public void setInitialWorkset(List<Operator> inputs) {
		setSecondInputs(inputs);
	}
	
	/**
	 * Adds the given input to the initial solution set.
	 * 
	 * @param inputs The contract added to the initial solution set.
	 */
	public void addToInitialSolutionSet(Operator ... inputs) {
		addFirstInput(inputs);
	}
	
	/**
	 * Adds the given input to the initial workset.
	 * 
	 * @param inputs The contract added to the initial workset.
	 */
	public void addToInitialWorkset(Operator ... inputs) {
		addSecondInput(inputs);
	}

	/**
	 * Adds the given inputs to the initial solution set.
	 * 
	 * @param inputs The contracts added to the initial solution set.
	 */
	public void addToInitialSolutionSet(List<Operator> inputs) {
		addFirstInputs(inputs);
	}

	/**
	 * Adds the given inputs to the initial workset.
	 * 
	 * @param inputs The contracts added to the initial workset.
	 */
	public void addToInitialWorkset(List<Operator> inputs) {
		addSecondInputs(inputs);
	}
	
	// --------------------------------------------------------------------------------------------
	// Place-holder Contracts
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Specialized contract to use as a recognizable place-holder for the working set input to the
	 * step function, when composing the nested data flow.
	 */
	// Integer is only a dummy here but this whole placeholder shtick seems a tad bogus.
	public static class WorksetPlaceHolder extends Operator {

		private final WorksetIteration containingIteration;

		public WorksetPlaceHolder(WorksetIteration container) {
			super("Workset Place Holder");
			this.containingIteration = container;
		}

		public WorksetIteration getContainingWorksetIteration() {
			return this.containingIteration;
		}

		@Override
		public void accept(Visitor<Operator> visitor) {
			visitor.preVisit(this);
			visitor.postVisit(this);
		}

		@Override
		public UserCodeWrapper<?> getUserCodeWrapper() {
			return null;
		}
	}
	
	/**
	 * Specialized contract to use as a recognizable place-holder for the solution set input to the
	 * step function, when composing the nested data flow.
	 */
	// Integer is only a dummy here but this whole placeholder shtick seems a tad bogus.
	public static class SolutionSetPlaceHolder extends Operator {

		private final WorksetIteration containingIteration;

		public SolutionSetPlaceHolder(WorksetIteration container) {
			super("Solution Set Place Holder");
			this.containingIteration = container;
		}

		public WorksetIteration getContainingWorksetIteration() {
			return this.containingIteration;
		}

		@Override
		public void accept(Visitor<Operator> visitor) {
			visitor.preVisit(this);
			visitor.postVisit(this);
		}

		@Override
		public UserCodeWrapper<?> getUserCodeWrapper() {
			return null;
		}
	}
}
