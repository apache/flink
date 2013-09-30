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

import eu.stratosphere.pact.common.stubs.Stub;

/**
 * Abstract superclass for all contracts that represent actual Pacts.
 */
public abstract class AbstractPact<T extends Stub> extends Contract {
	
	/**
	 * The object or class containing the user function.
	 */
	protected final UserCodeWrapper<T> stub;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new abstract Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 */
	protected AbstractPact(UserCodeWrapper<T> stub, String name) {
		super(name);
		this.stub = stub;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the stub that is wrapped by this contract. The stub is the actual implementation of the
	 * user code.
	 * 
	 * This throws an exception if the pact does not contain an object but a class for the user
	 * code.
	 * 
	 * @return The object with the user function for this Pact.
	 *
	 * @see eu.stratosphere.pact.generic.contract.Contract#getUserCodeObject()
	 */
	@Override
	public UserCodeWrapper<T> getUserCodeWrapper() {
		return stub;
	}
	
	// --------------------------------------------------------------------------------------------
	
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
	public abstract int[] getKeyColumns(int inputNum);
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Generic utility function that wraps a single class object into an array of that class type.
	 * 
	 * @param <U> The type of the classes.
	 * @param clazz The class object to be wrapped.
	 * @return An array wrapping the class object.
	 */
	protected static final <U> Class<U>[] asArray(Class<U> clazz) {
		@SuppressWarnings("unchecked")
		Class<U>[] array = (Class<U>[]) new Class[] { clazz };
		return array;
	}
	
	/**
	 * Generic utility function that returns an empty class array.
	 * 
	 * @param <U> The type of the classes.
	 * @return An empty array of type <tt>Class&lt;U&gt;</tt>.
	 */
	protected static final <U> Class<U>[] emptyClassArray() {
		@SuppressWarnings("unchecked")
		Class<U>[] array = (Class<U>[]) new Class[0];
		return array;
	}
}
