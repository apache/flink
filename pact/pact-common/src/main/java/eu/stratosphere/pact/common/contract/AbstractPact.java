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

import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;


/**
 * Abstract superclass for all contracts that represent actual Pacts and no data sources or sinks.
 *
 * @author Stephan Ewen
 */
public abstract class AbstractPact<OK extends Key, OV extends Value, T extends Stub<OK, OV>> extends Contract
implements OutputContractConfigurable
{
	/**
	 * The class containing the user function for this Pact.
	 */
	protected Class<? extends T> stubClass;
	
	/**
	 * This contract's output contract.
	 */
	protected Class<? extends Annotation> outputContract;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new abstract Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 */
	protected AbstractPact(Class<? extends T> stubClass, String name)
	{
		super(name);
		this.stubClass = stubClass;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the stub that is wrapped by this contract. The stub is the actual implementation of the
	 * user code.
	 * 
	 * @return The class with the user function for this Pact.
	 *
	 * @see eu.stratosphere.pact.common.contract.Contract#getUserCodeClass()
	 */
	@Override
	public Class<? extends T> getUserCodeClass()
	{
		return this.stubClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setOutputContract(Class<? extends Annotation> oc) {
		if (!oc.getEnclosingClass().equals(OutputContract.class)) {
			throw new IllegalArgumentException("The given annotation does not describe an output contract.");
		}

		this.outputContract = oc;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends Annotation> getOutputContract() {
		return this.outputContract;
	}
}
