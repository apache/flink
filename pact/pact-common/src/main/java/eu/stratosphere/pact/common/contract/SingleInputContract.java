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

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;

/**
 * Abstract contract superclass for for all contracts that have one input like "map" or "reduce".
 */
public abstract class SingleInputContract<T extends Stub> extends AbstractPact<T>
{
	/**
	 * The input which produces the data consumed by this Pact.
	 */
	final protected List<Contract> input = new ArrayList<Contract>();
	
	/**
	 * The positions of the keys in the tuple.
	 */
	private final int[] keyFields;
	
	/**
	 * The positions of the secondary sort keys in the tuple.
	 */
	private int[] secondarySortKeyFields;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new abstract single-input Pact with the given name wrapping the given user function.
	 * 
	 * @param stubClass The class containing the user function.
	 * @param keyTypes The classes of the data types that act as keys in this stub.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	protected SingleInputContract(Class<? extends T> stubClass, Class<? extends Key>[] keyTypes, int[] keyPositions, String name)
	{
		super(stubClass, keyTypes, name);
		this.keyFields = keyPositions;
		this.secondarySortKeyFields = new int[0];
	}
	
	/**
	 * Creates a new abstract single-input Pact with the given name wrapping the given user function.
	 * This constructor is specialized only for Pacts that require no keys for their processing.
	 * 
	 * @param stubClass The class containing the user function.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	protected SingleInputContract(Class<? extends T> stubClass, String name)
	{
		super(stubClass, name);
		this.keyFields = new int[0];
		this.secondarySortKeyFields = new int[0];
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the input, or null, if none is set.
	 * 
	 * @return The contract's input contract.
	 */
	public List<Contract> getInputs() {
		return this.input;
	}

	/**
	 * Connects the input to the task wrapped in this contract
	 * 
	 * @param input The contract will be set as input.
	 */
	public void addInput(Contract input) {
		this.input.add(input);
	}
	
	/**
	 * Connects the inputs to the task wrapped in this contract
	 * 
	 * @param input The contracts will be set as input.
	 */
	public void addInputs(List<Contract> inputs) {
		this.input.addAll(inputs);
	}

	/**
	 * Clears all previous connections and sets the given contract as
	 * single input of this contract.
	 * 
	 * @param input		The contract will be set as input.
	 */
	public void setInput(Contract input) {
		this.input.clear();
		this.input.add(input);
	}
	
	/**
	 * Clears all previous connections and sets the given contracts as
	 * inputs of this contract.
	 * 
	 * @param input		The contracts will be set as inputs.
	 */
	public void setInputs(List<Contract> inputs) {
		this.input.clear();
		this.input.addAll(inputs);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.AbstractPact#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.AbstractPact#getKeyColumnNumbers(int)
	 */
	@Override
	public int[] getKeyColumnNumbers(int inputNum) {
		if (inputNum == 0) {
			return this.keyFields;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.AbstractPact#getSecondarySortKeyColumnNumbers(int)
	 */
	public void setSecondarySortKeyColumnNumbers(int inputNum, int[] positions) {
		if (inputNum == 0) {
			this.secondarySortKeyFields = positions;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.AbstractPact#getSecondarySortKeyColumnNumbers(int)
	 */
	@Override
	public int[] getSecondarySortKeyColumnNumbers(int inputNum) {
		if (inputNum == 0) {
			return this.secondarySortKeyFields;
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
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor)
	{
		boolean descend = visitor.preVisit(this);	
		if (descend) {
			for(Contract c : this.input) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
