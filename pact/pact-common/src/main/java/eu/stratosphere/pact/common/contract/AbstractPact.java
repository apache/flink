/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;


/**
 * Abstract superclass for all contracts that represent actual Pacts.
 *
 * @author Stephan Ewen, Aljoscha Krettek
 */
public abstract class AbstractPact<T extends Stub> extends Contract
{
	/**
	 * The class containing the user function for this Pact.
	 */
	protected final Class<? extends T> stubClass;

	/**
	 * The classes that represent the key data types.
	 */
	private final Class<? extends Key>[] keyClasses;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new abstract Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 */
	@SuppressWarnings("unchecked")
	protected AbstractPact(Class<? extends T> stubClass, String name)
	{
		super(name);
		this.stubClass = stubClass;
		this.keyClasses = (Class<? extends Key>[]) new Class[0];
	}
	
	/**
	 * Creates a new abstract Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 * @param keyClasses The classes describing the keys.
	 */
	protected AbstractPact(Class<? extends T> stubClass, Class<? extends Key>[] keyClasses, String name)
	{
		super(name);
		this.stubClass = stubClass;
		this.keyClasses = keyClasses;
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
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the types of the key fields on which this reduce contract groups.
	 * 
	 * @return The types of the key fields.
	 */
	public Class<? extends Key>[] getKeyClasses()
	{
		return this.keyClasses;
	}
	
	/**
	 * Gets the number of inputs for this Pact.
	 * 
	 * @return The number of inputs for this Pact.
	 */
	public abstract int getNumberOfInputs();
	
	/**
	 * Gets the column numbers of the key fields in the input records for the given input.
	 *  
	 * @return The column numbers of the key fields.
	 */
	public abstract int[] getKeyColumnNumbers(int inputNum);
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Generic utility function that wraps a single class object into an array of that class type.
	 * 
	 * @param <U> The type of the classes.
	 * @param clazz The class object to be wrapped.
	 * @return An array wrapping the class object.
	 */
	protected static final <U> Class<U>[] asArray(Class<U> clazz)
	{
		@SuppressWarnings("unchecked")
		Class<U>[] array = (Class<U>[]) new Class[] { clazz };
		return array;
	}
}
