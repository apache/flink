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

package eu.stratosphere.pact.generic.contract;

import java.util.List;

import eu.stratosphere.pact.common.util.Visitor;
import eu.stratosphere.pact.generic.stub.AbstractStub;

/**
 * 
 * <p>
 * This class is a subclass of {@code DualInputContract}. The solution set is considered the first input, the
 * workset is considered the second input.
 */
public class WorksetIteration extends DualInputContract<AbstractStub> {
	
	private final Contract solutionSetPlaceholder = new SolutionSetPlaceHolder(this);

	private final Contract worksetPlaceholder = new WorksetPlaceHolder(this);

	private Contract solutionSetDelta;

	private Contract nextWorkset;
	
	/**
	 * The positions of the keys in the solution tuple.
	 */
	private final int[] solutionSetKeyFields;
	
	/**
	 * The maximum number of iterations. Possibly used only as a safeguard.
	 */
	private int maxNumberOfIterations = -1;
	
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
		super(AbstractStub.class, name);
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
	
	// --------------------------------------------------------------------------------------------
	// Getting / Setting of the step function input place-holders
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the contract that represents the solution set for the step function.
	 * 
	 * @return The solution set for the step function.
	 */
	public Contract getSolutionSet() {
		return this.solutionSetPlaceholder;
	}

	/**
	 * Gets the contract that represents the workset for the step function.
	 * 
	 * @return The workset for the step function.
	 */
	public Contract getWorkset() {
		return this.worksetPlaceholder;
	}

	/**
	 * Sets the contract of the step function that represents the next workset. This contract is considered 
	 * one of the two sinks of the step function (the other one being the solution set delta).
	 * 
	 * @param result The contract representing the next workset.
	 */
	public void setNextWorkset(Contract result) {
		this.nextWorkset = result;
	}
	
	/**
	 * Gets the contract that has been set as the next workset.
	 * 
	 * @return The contract that has been set as the next workset.
	 */
	public Contract getNextWorkset() {
		return this.nextWorkset;
	}
	
	/**
	 * Sets the contract of the step function that represents the solution set delta. This contract is considered
	 * one of the two sinks of the step function (the other one being the next workset).
	 * 
	 * @param delta The contract representing the solution set delta.
	 */
	public void setSolutionSetDelta(Contract delta) {
		this.solutionSetDelta = delta;
	}
	
	/**
	 * Gets the contract that has been set as the solution set delta.
	 * 
	 * @return The contract that has been set as the solution set delta.
	 */
	public Contract getSolutionSetDelta() {
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
	public List<Contract> getInitialSolutionSet() {
		return getFirstInputs();
	}
	
	/**
	 * Returns the initial workset input, or null, if none is set.
	 * 
	 * @return The iteration's workset input.
	 */
	public List<Contract> getInitialWorkset() {
		return getSecondInputs();
	}
	
	/**
	 * Sets the given input as the initial solution set.
	 * 
	 * @param input The contract to set the initial solution set.
	 */
	public void setInitialSolutionSet(Contract ... input) {
		setFirstInput(input);
	}
	
	/**
	 * Sets the given input as the initial workset.
	 * 
	 * @param input The contract to set as the initial workset.
	 */
	public void setInitialWorkset(Contract ... input) {
		setSecondInput(input);
	}

	/**
	 * Sets the given inputs as the initial solution set.
	 * 
	 * @param input The contracts to set as the initial solution set.
	 */
	public void setInitialSolutionSet(List<Contract> inputs) {
		setFirstInputs(inputs);
	}

	/**
	 * Sets the given inputs as the initial workset.
	 * 
	 * @param input The contracts to set as the initial workset.
	 */
	public void setInitialWorkset(List<Contract> inputs) {
		setSecondInputs(inputs);
	}
	
	/**
	 * Adds the given input to the initial solution set.
	 * 
	 * @param input The contract added to the initial solution set.
	 */
	public void addToInitialSolutionSet(Contract ... input) {
		addFirstInput(input);
	}
	
	/**
	 * Adds the given input to the initial workset.
	 * 
	 * @param input The contract added to the initial workset.
	 */
	public void addToInitialWorkset(Contract ... input) {
		addSecondInput(input);
	}

	/**
	 * Adds the given inputs to the initial solution set.
	 * 
	 * @param input The contracts added to the initial solution set.
	 */
	public void addToInitialSolutionSet(List<Contract> inputs) {
		addFirstInputs(inputs);
	}

	/**
	 * Adds the given inputs to the initial workset.
	 * 
	 * @param input The contracts added to the initial workset.
	 */
	public void addToInitialWorkset(List<Contract> inputs) {
		addSecondInputs(inputs);
	}
	
	// --------------------------------------------------------------------------------------------
	// Place-holder Contracts
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Specialized contract to use as a recognizable place-holder for the working set input to the
	 * step function, when composing the nested data flow.
	 */
	public static final class WorksetPlaceHolder extends Contract {

		private final WorksetIteration containingIteration;

		private WorksetPlaceHolder(WorksetIteration container) {
			super("Workset Place Holder");
			this.containingIteration = container;
		}

		public WorksetIteration getContainingWorksetIteration() {
			return this.containingIteration;
		}

		@Override
		public void accept(Visitor<Contract> visitor) {
			visitor.preVisit(this);
			visitor.postVisit(this);
		}

		@Override
		public Class<?> getUserCodeClass() {
			return null;
		}
	}
	
	/**
	 * Specialized contract to use as a recognizable place-holder for the solution set input to the
	 * step function, when composing the nested data flow.
	 */
	public static final class SolutionSetPlaceHolder extends Contract {

		private final WorksetIteration containingIteration;

		private SolutionSetPlaceHolder(WorksetIteration container) {
			super("Solution Set Place Holder");
			this.containingIteration = container;
		}

		public WorksetIteration getContainingWorksetIteration() {
			return this.containingIteration;
		}

		@Override
		public void accept(Visitor<Contract> visitor) {
			visitor.preVisit(this);
			visitor.postVisit(this);
		}

		@Override
		public Class<?> getUserCodeClass() {
			return null;
		}
	}
}
