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

package eu.stratosphere.api.common.operators;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;

/**
 * Abstract superclass for all contracts that represent actual operators.
 */
public abstract class AbstractUdfOperator<T extends Function> extends Operator {
	
	/**
	 * The object or class containing the user function.
	 */
	protected final UserCodeWrapper<T> udf;
	
	/**
	 * The extra inputs which parameterize the user function.
	 */
	protected final Map<String, Operator> broadcastInputs = new HashMap<String, Operator>();
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new abstract operator with the given name wrapping the given user function.
	 *
	 * @param function The wrapper object containing the user function.
	 * @param name The given name for the operator, used in plans, logs and progress messages.
	 */
	protected AbstractUdfOperator(UserCodeWrapper<T> function, String name) {
		super(name);
		this.udf = function;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the function that is held by this operator. The function is the actual implementation of the
	 * user code.
	 * 
	 * This throws an exception if the pact does not contain an object but a class for the user
	 * code.
	 * 
	 * @return The object with the user function for this operator.
	 *
	 * @see eu.stratosphere.api.common.operators.Operator#getUserCodeWrapper()
	 */
	@Override
	public UserCodeWrapper<T> getUserCodeWrapper() {
		return udf;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns the input, or null, if none is set.
	 * 
	 * @return The broadcast input root operator.
	 */
	public Map<String, Operator> getBroadcastInputs() {
		return this.broadcastInputs;
	}
	
	/**
	 * Binds the result produced by a plan rooted at {@code root} to a variable 
	 * used by the UDF wrapped in this operator.
	 * 
	 * @param root The root of the plan producing this input.
	 */
	public void setBroadcastVariable(String name, Operator root) {
		if (name == null) {
			throw new IllegalArgumentException("The broadcast input name may not be null.");
		}
		if (root == null) {
			throw new IllegalArgumentException("The broadcast input root operator may not be null.");
		}
		
		this.broadcastInputs.put(name, root);
	}
	
	/**
	 * Clears all previous broadcast inputs and binds the given inputs as
	 * broadcast variables of this operator.
	 * 
	 * @param inputs The <name, root> pairs to be set as broadcast inputs.
	 */
	public void setBroadcastVariables(Map<String, Operator> roots) {
		this.broadcastInputs.clear();
		this.broadcastInputs.putAll(roots);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the number of inputs for this operator.
	 * 
	 * @return The number of inputs for this operator.
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
		Class<U>[] array = new Class[] { clazz };
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
		Class<U>[] array = new Class[0];
		return array;
	}
}
