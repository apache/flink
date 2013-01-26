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

import eu.stratosphere.pact.common.plan.Visitor;

/**
 * 
 */
public class BulkIteration extends Contract
{
	private Contract initialPartialSolution;
	
	private Contract iterationResult;
	
	private Contract terminationCriterion;
	
	private final Contract inputPlaceHolder;
	
	private int numberOfIterations;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * 
	 */
	public BulkIteration() {
		this("Unnamed Iteration");
	}
	
	/**
	 * @param name
	 */
	public BulkIteration(String name) {
		super(name);
		this.inputPlaceHolder = new InputPlaceHolder();
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * @return
	 */
	public Contract getPartialSolution() {
		return this.inputPlaceHolder;
	}
	
	/**
	 * @param input
	 */
	public void setInitialPartialSolution(Contract input) {
		if (input == null) {
			throw new NullPointerException("Contract for initial partial solution must not be null.");
		}
		this.initialPartialSolution = input;
	}
	
	/**
	 * @return
	 */
	public Contract getInitialPartialSolution() {
		return this.initialPartialSolution;
	}
	
	/**
	 * @param result
	 */
	public void setNextPartialSolution(Contract result) {
		if (result == null) {
			throw new NullPointerException("Contract producing the next partial solution must not be null.");
		}
		this.iterationResult = result;
	}
	
	/**
	 * @return
	 */
	public Contract getNextPartialSolution() {
		return this.iterationResult;
	}
	
	/**
	 * @param criterion
	 */
	public void setTerminationCriterion(Contract criterion) {
		throw new UnsupportedOperationException("Termination criterion support is currently not implemented.");
	}
	
	/**
	 * @param num
	 */
	public void setNumberOfIterations(int num) {
		if (num < 1) {
			throw new IllegalArgumentException("The number of iterations must be at least one.");
		}
		this.numberOfIterations = num;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		if (visitor.preVisit(this)) {
			if (this.initialPartialSolution != null) {
				this.initialPartialSolution.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.Contract#getUserCodeClass()
	 */
	@Override
	public Class<?> getUserCodeClass() {
		return null;
	}
	
	/**
	 * @throws Exception
	 */
	public void validate() throws Exception {
		if (this.initialPartialSolution == null) {
			throw new Exception("Contract for initial partial solution is not set.");
		}
		if (this.iterationResult == null) {
			throw new Exception("Contract producing the next version of the partial " +
					"solution (iteration result) is not set.");
		}
		if (this.terminationCriterion == null && this.numberOfIterations == 0) {
			throw new Exception("No termination condition is set " +
					"(neither fix number of iteration nor termination criterion).");
		}
		if (this.terminationCriterion != null && this.numberOfIterations > 0) {
			throw new Exception("Termination condition is ambiguous. " +
				"Both a fix number of iteration and a termination criterion are set.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Specialized contract to use as a recognizable place-holder for the input to the
	 * iteration when composing the nested data flow.
	 */
	private static final class InputPlaceHolder extends Contract
	{
		private InputPlaceHolder() {
			super("Iteration Input Place Holder");
		}
		
		@Override
		public void accept(Visitor<Contract> visitor) {
			throw new RuntimeException();
		}

		@Override
		public Class<?> getUserCodeClass() {
			return null;
		}
	}
}
