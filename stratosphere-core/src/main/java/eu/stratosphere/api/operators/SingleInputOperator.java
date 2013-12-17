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

package eu.stratosphere.api.operators;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.functions.Function;
import eu.stratosphere.api.operators.util.UserCodeWrapper;
import eu.stratosphere.util.Visitor;

/**
 * Abstract contract superclass for for all contracts that have one input like "map" or "reduce".
 */
public abstract class SingleInputOperator<T extends Function> extends AbstractUdfOperator<T> {
	
	/**
	 * The input which produces the data consumed by this Pact.
	 */
	protected final List<Operator> input = new ArrayList<Operator>();
	
	/**
	 * The positions of the keys in the tuple.
	 */
	private final int[] keyFields;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new abstract single-input Pact with the given name wrapping the given user function.
	 * 
	 * @param stub The object containing the user function.
	 * @param keyPositions The field positions of the input records that act as keys.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	protected SingleInputOperator(UserCodeWrapper<T> stub, int[] keyPositions, String name) {
		super(stub, name);
		this.keyFields = keyPositions;
	}
	
	/**
	 * Creates a new abstract single-input Pact with the given name wrapping the given user function.
	 * This constructor is specialized only for Pacts that require no keys for their processing.
	 * 
	 * @param stub The object containing the user function.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	protected SingleInputOperator(UserCodeWrapper<T> stub, String name) {
		super(stub, name);
		this.keyFields = new int[0];
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the input, or null, if none is set.
	 * 
	 * @return The contract's input contract.
	 */
	public List<Operator> getInputs() {
		return this.input;
	}
	
	/**
	 * Removes all inputs from this contract.
	 */
	public void clearInputs() {
		this.input.clear();
	}

	/**
	 * Connects the input to the task wrapped in this contract
	 * 
	 * @param input The contract will be set as input.
	 */
	public void addInput(Operator ... input) {
		for (int i = 0; i < input.length; i++) {
			if (input[i] == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			} else {
				this.input.add(input[i]);
			}
		}
	}
	
	/**
	 * Connects the inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts will be set as input.
	 */
	public void addInputs(List<Operator> inputs) {
		for (Operator element : inputs) {
			if (element == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			} else {
				this.input.add(element);
			}
		}
	}

	/**
	 * Clears all previous connections and sets the given contract as
	 * single input of this contract.
	 * 
	 * @param input The contract will be set as input.
	 */
	public void setInput(Operator ... input) {
		this.input.clear();
		addInput(input);
	}
	
	/**
	 * Clears all previous connections and sets the given contracts as
	 * inputs of this contract.
	 * 
	 * @param inputs The contracts will be set as inputs.
	 */
	public void setInputs(List<Operator> inputs) {
		this.input.clear();
		addInputs(inputs);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public final int getNumberOfInputs() {
		return 1;
	}

	@Override
	public int[] getKeyColumns(int inputNum) {
		if (inputNum == 0) {
			return this.keyFields;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Accepts the visitor and applies it this instance. The visitors pre-visit method is called and, if returning 
	 * <tt>true</tt>, the visitor is recursively applied on the single input. After the recursion returned,
	 * the post-visit method is called.
	 * 
	 * @param visitor The visitor.
	 *  
	 * @see eu.stratosphere.util.Visitable#accept(eu.stratosphere.util.Visitor)
	 */
	@Override
	public void accept(Visitor<Operator> visitor) {
		if (visitor.preVisit(this)) {
			for (Operator c : this.input) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
