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
import eu.stratosphere.pact.common.util.IterationPlaceHolderStub;

/**
 * 
 */
public class BulkIteration extends SingleInputContract<IterationPlaceHolderStub>
{
	private static String DEFAULT_NAME = "<Unnamed Bulk Iteration>";
	
	private Contract iterationResult;
	
	private final Contract inputPlaceHolder = new PartialSolutionPlaceHolder(this);
	
	private int numberOfIterations = -1;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * 
	 */
	public BulkIteration() {
		this(DEFAULT_NAME);
	}
	
	/**
	 * @param name
	 */
	public BulkIteration(String name) {
		super(IterationPlaceHolderStub.class, name);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * @return
	 */
	public Contract getPartialSolution() {
		return this.inputPlaceHolder;
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
	
	public int getNumberOfIterations() {
		return this.numberOfIterations;
	}
	
	/**
	 * @throws Exception
	 */
	public void validate() throws Exception {
		if (this.input == null || this.input.isEmpty()) {
			throw new Exception("Contract for initial partial solution is not set.");
		}
		if (this.iterationResult == null) {
			throw new Exception("Contract producing the next version of the partial " +
					"solution (iteration result) is not set.");
		}
		if (this.numberOfIterations == 0) {
			throw new Exception("No termination condition is set " +
					"(neither fix number of iteration nor termination criterion).");
		}
//		if (this.terminationCriterion != null && this.numberOfIterations > 0) {
//			throw new Exception("Termination condition is ambiguous. " +
//				"Both a fix number of iteration and a termination criterion are set.");
//		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Specialized contract to use as a recognizable place-holder for the input to the
	 * iteration when composing the nested data flow.
	 */
	public static final class PartialSolutionPlaceHolder extends Contract
	{
		private final BulkIteration containingIteration;
		
		private PartialSolutionPlaceHolder(BulkIteration container) {
			super("Iteration Input Place Holder");
			this.containingIteration = container;
		}
		
		public BulkIteration getContainingBulkIteration() {
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
