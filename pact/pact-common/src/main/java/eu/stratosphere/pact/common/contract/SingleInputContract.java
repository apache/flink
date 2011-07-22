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

package eu.stratosphere.pact.common.contract;

import java.lang.annotation.Annotation;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.Stub;

/**
 * Abstract contract superclass for for all contracts that have one input like "map" or "reduce".
 */
public abstract class SingleInputContract<T extends Stub> extends AbstractPact<T> implements OutputContractConfigurable
{
	/**
	 * The input which produces the data consumed by this Pact.
	 */
	protected Contract input;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new abstract single-input Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 */
	protected SingleInputContract(Class<? extends T> stubClass, String name)
	{
		super(stubClass, name);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the input, or null, if none is set.
	 * 
	 * @return The contract's input contract.
	 */
	public Contract getInput() {
		return input;
	}

	/**
	 * Connects the input to the task wrapped in this contract
	 * 
	 * @param input The contract will be set as input.
	 */
	public void setInput(Contract input) {
		this.input = input;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordcontract.OutputContractConfigurable#addOutputContract(java.lang.Class)
	 */
	@Override
	public void addOutputContract(Class<? extends Annotation> oc)
	{
		if (!oc.getEnclosingClass().equals(OutputContract.class)) {
			throw new IllegalArgumentException("The given annotation does not describe an output contract.");
		}

		this.ocs.add(oc);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordcontract.OutputContractConfigurable#getOutputContracts()
	 */
	@Override
	public Class<? extends Annotation>[] getOutputContracts() {
		@SuppressWarnings("unchecked")
		Class<? extends Annotation>[] targetArray = new Class[this.ocs.size()];
		return (Class<? extends Annotation>[]) this.ocs.toArray(targetArray);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Accepts the visitor and applies it this instance. The visitors pre-visit method is called and, if returning 
	 * <tt>true</tt>, the visitor is recursively applied on the single input. After the recursion returned,
	 * the post-visit method is called.
	 * 
	 * @param visitor The visitor.
	 *  
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor)
	{
		boolean descend = visitor.preVisit(this);	
		if (descend) {
			if (this.input != null) {
				this.input.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
