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

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.util.Visitor;

/**
 * Abstract contract superclass for for all contracts that have two inputs, like "match" or "cross".
 */
public abstract class DualInputOperator<T extends Function> extends AbstractUdfOperator<T> {
	
	/**
	 * The contract producing the first input.
	 */
	protected Operator input1 = null;
	/**
	 * The contract producing the second input.
	 */
	protected Operator input2 = null;

	/**
	 * The positions of the keys in the tuples of the first input.
	 */
	private final int[] keyFields1;
	
	/**
	 * The positions of the keys in the tuples of the second input.
	 */
	private final int[] keyFields2;
	
	/**
	 * Semantic properties of the associated function.
	 */
	private DualInputSemanticProperties semanticProperties;
	
	// --------------------------------------------------------------------------------------------

	public DualInputOperator() {
		this(null, null);
	}
	
	/**
	 * Creates a new abstract dual-input Pact with the given name wrapping the given user function.
	 * 
	 * @param stub The class containing the user function.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	protected DualInputOperator(UserCodeWrapper<T> stub, String name) {
		super(stub, name);
		this.keyFields1 = this.keyFields2 = new int[0];
		this.semanticProperties = new DualInputSemanticProperties();
	}
	
	/**
	 * Creates a new abstract dual-input Pact with the given name wrapping the given user function.
	 * This constructor is specialized only for Pacts that require no keys for their processing.
	 * 
	 * @param stub The object containing the user function.
	 * @param keyPositions1 The positions of the fields in the first input that act as keys.
	 * @param keyPositions2 The positions of the fields in the second input that act as keys.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	protected DualInputOperator(UserCodeWrapper<T> stub, int[] keyPositions1, int[] keyPositions2, String name) {
		super(stub, name);
		this.keyFields1 = keyPositions1;
		this.keyFields2 = keyPositions2;
		this.semanticProperties = new DualInputSemanticProperties();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the first input, or null, if none is set.
	 * 
	 * @return The contract's first input.
	 */
	public Operator getFirstInput() {
		return this.input1;
	}
	
	/**
	 * Returns the first input as list, or null, if none is set.
	 * This function is here for compatibility=reasons of the old-java-API with the scala API
	 * 
	 * @return The contract's first input contract as list.
	 */
	public List<Operator> getFirstInputs() {
		if(this.input1 == null){
			return null;
		}
		ArrayList<Operator> inputs = new ArrayList<Operator>();
		inputs.add(this.input1);
		return inputs;
	}
	
	/**
	 * Returns the second input, or null, if none is set.
	 * 
	 * @return The contract's second input.
	 */
	public Operator getSecondInput() {
		return this.input2;
	}
	
	/**
	 * Returns the second input as list, or null, if none is set.
	 * This function is here for compatibility=reasons of the old-java-API with the scala API
	 * 
	 * @return The contract's second input contract as list
	 */
	public List<Operator> getSecondInputs() {
		if(this.input2 == null){
			return null;
		}
		ArrayList<Operator> inputs = new ArrayList<Operator>();
		inputs.add(this.input2);
		return inputs;
	}
	
	/**
	 * Removes all inputs from this contract's first input.
	 */
	public void clearFirstInput() {
		this.input1 = null;
	}
	
	/**
	 * Removes all inputs from this contract's second input.
	 */
	public void clearSecondInput() {
		this.input2 = null;
	}

	/**
	 * Connects the first input to the task wrapped in this contract.
	 * 
	 * @param input The contract will be set as the first input.
	 */
	public void addFirstInput(Operator ... input) {
		this.input1 = Operator.createUnionCascade(this.input1, input);
	}
	
	/**
	 * Connects the second input to the task wrapped in this contract.
	 * 
	 * @param input The contract will be set as the second input.
	 */
	public void addSecondInput(Operator ... input) {
		this.input2 = Operator.createUnionCascade(this.input2, input);
	}

	/**
	 * Connects the first inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the first inputs.
	 */
	public void addFirstInputs(List<Operator> inputs) {
		this.input1 = Operator.createUnionCascade(this.input1, inputs.toArray(new Operator[inputs.size()]));
	}

	/**
	 * Connects the second inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the second inputs.
	 */
	public void addSecondInputs(List<Operator> inputs) {
		this.input2 = Operator.createUnionCascade(this.input2, inputs.toArray(new Operator[inputs.size()]));	}
	
	/**
	 * Clears all previous connections and connects the first input to the task wrapped in this contract
	 * 
	 * @param input The contract that is connected as the first input.
	 */
	public void setFirstInput(Operator input) {
		this.input1 = input;
	}

	/**
	 * Clears all previous connections and connects the second input to the task wrapped in this contract
	 * 
	 * @param input The contract that is connected as the second input.
	 */
	public void setSecondInput(Operator input) {
		this.input2 = input;
	}
	
	/**
	 * Clears all previous connections and connects the first input to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that are connected as the first input.
	 */
	public void setFirstInput(Operator ... inputs) {
		this.input1 = null;
		addFirstInput(inputs);
	}

	/**
	 * Clears all previous connections and connects the second input to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that are connected as the second input.
	 */
	public void setSecondInput(Operator ... inputs) {
		this.input2 = null;
		addSecondInput(inputs);
	}
	
	/**
	 * Clears all previous connections and connects the first inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that are connected as the first inputs.
	 */
	public void setFirstInputs(List<Operator> inputs) {
		this.input1 = null;
		addFirstInputs(inputs);
	}

	/**
	 * Clears all previous connections and connects the second inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that are connected as the second inputs.
	 */
	public void setSecondInputs(List<Operator> inputs) {
		this.input2 = null;
		addSecondInputs(inputs);
	}
	
	// --------------------------------------------------------------------------------------------

	public DualInputSemanticProperties getSemanticProperties() {
		return this.semanticProperties;
	}
	
	public void setSemanticProperties(DualInputSemanticProperties semanticProperties) {
		this.semanticProperties = semanticProperties;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public final int getNumberOfInputs() {
		return 2;
	}

	@Override
	public int[] getKeyColumns(int inputNum) {
		if (inputNum == 0) {
			return this.keyFields1;
		}
		else if (inputNum == 1) {
			return this.keyFields2;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public void accept(Visitor<Operator> visitor) {
		boolean descend = visitor.preVisit(this);
		if (descend) {
			this.input1.accept(visitor);
			this.input2.accept(visitor);
			for (Operator c : this.broadcastInputs.values()) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
