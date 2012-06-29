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
 * Abstract contract superclass for for all contracts that have two inputs, like "match" or "cross".
 */
public abstract class DualInputContract<T extends Stub> extends AbstractPact<T>
{
	/**
	 * The contract producing the first input.
	 */
	final protected List<Contract> input1 = new ArrayList<Contract>();
	/**
	 * The contract producing the second input.
	 */
	final protected List<Contract> input2 = new ArrayList<Contract>();

	/**
	 * The positions of the keys in the tuples of the first input.
	 */
	private final int[] keyFields1;
	
	/**
	 * The positions of the keys in the tuples of the second input.
	 */
	private final int[] keyFields2;
	
	/**
	 * The positions of the keys in the tuples of the first input.
	 */
	private final int[] secondarySortKeyFields1;
	
	/**
	 * The positions of the keys in the tuples of the second input.
	 */
	private final int[] secondarySortKeyFields2;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new abstract dual-input Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 */
	protected DualInputContract(Class<? extends T> stubClass, String name)
	{
		super(stubClass, name);
		this.keyFields1 = this.keyFields2 = new int[0];
		this.secondarySortKeyFields1 = this.secondarySortKeyFields2 = new int[0];
	}
	
	/**
	 * Creates a new abstract dual-input Pact with the given name wrapping the given user function.
	 * This constructor is specialized only for Pacts that require no keys for their processing.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param keyTypes The classes of the data types that act as keys in this stub.
	 * @param stubClass The class containing the user function.
	 */
	protected DualInputContract(Class<? extends T> stubClass, Class<? extends Key>[] keyTypes, int[] keyPositions1, int[] keyPositions2, String name)
	{
		super(stubClass, keyTypes, name);
		this.keyFields1 = keyPositions1;
		this.keyFields2 = keyPositions2;
		this.secondarySortKeyFields1 = this.secondarySortKeyFields2 = new int[0];
	}
	
	/**
	 * Creates a new abstract dual-input Pact with the given name wrapping the given user function.
	 * This constructor is specialized only for Pacts that require no keys for their processing.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param keyTypes The classes of the data types that act as keys in this stub.
	 * @param stubClass The class containing the user function.
	 */
	protected DualInputContract(Class<? extends T> stubClass, Class<? extends Key>[] keyTypes, int[] keyPositions1, int[] keyPositions2,
			Class<? extends Key>[] secondarySortKeyTypes, int[] secondarySortKeyPositions1, int[] secondarySortKeyPositions2, String name)
	{
		super(stubClass, keyTypes, secondarySortKeyTypes, name);
		this.keyFields1 = keyPositions1;
		this.keyFields2 = keyPositions2;
		this.secondarySortKeyFields1 = secondarySortKeyPositions1;
		this.secondarySortKeyFields2 = secondarySortKeyPositions2;
		
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the first input, or null, if none is set.
	 * 
	 * @return The contract's first input.
	 */
	public List<Contract> getFirstInputs() {
		return this.input1;
	}
	
	/**
	 * Returns the second input, or null, if none is set.
	 * 
	 * @return The contract's second input.
	 */
	public List<Contract> getSecondInputs() {
		return this.input2;
	}

	/**
	 * Connects the first input to the task wrapped in this contract.
	 * 
	 * @param input The contract will be set as the first input.
	 */
	public void addFirstInput(Contract input) {
		this.input1.add(input);
	}
	
	/**
	 * Connects the second input to the task wrapped in this contract.
	 * 
	 * @param input The contract will be set as the second input.
	 */
	public void addSecondInput(Contract input) {
		this.input2.add(input);
	}

	/**
	 * Connects the first inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the first inputs.
	 */
	public void addFirstInputs(List<Contract> inputs) {
		this.input1.addAll(inputs);
	}

	/**
	 * Connects the second inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the second inputs.
	 */
	public void addSecondInputs(List<Contract> inputs) {
		this.input2.addAll(inputs);
	}
	
	/**
	 * Clears all previous connections and connects the first input to the task wrapped in this contract
	 * 
	 * @param firstInput The contract that is connected as the first input.
	 */
	public void setFirstInput(Contract inputs) {
		this.input1.clear();
		this.input1.add(inputs);
	}

	/**
	 * Clears all previous connections and connects the second input to the task wrapped in this contract
	 * 
	 * @param secondInput The contract that is connected as the second input.
	 */
	public void setSecondInput(Contract inputs) {
		this.input2.clear();
		this.input2.add(inputs);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.AbstractPact#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.AbstractPact#getKeyColumnNumbers(int)
	 */
	@Override
	public int[] getKeyColumnNumbers(int inputNum) {
		if (inputNum == 0) {
			return this.keyFields1;
		}
		else if (inputNum == 1) {
			return this.keyFields2;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.AbstractPact#getSecondarySortKeyColumnNumbers(int)
	 */
	@Override
	public int[] getSecondarySortKeyColumnNumbers(int inputNum) {
		if (inputNum == 0) {
			return this.secondarySortKeyFields1;
		}
		else if (inputNum == 1) {
			return this.secondarySortKeyFields2;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Clears all previous connections and connects the first inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the first inputs.
	 */
	public void setFirstInputs(List<Contract> inputs) {
		this.input1.clear();
		this.input1.addAll(inputs);
	}

	/**
	 * Clears all previous connections and connects the second inputs to the task wrapped in this contract
	 * 
	 * @param secondInput The contracts that is connected as the second inputs.
	 */
	public void setSecondInputs(List<Contract> inputs) {
		this.input2.clear();
		this.input2.addAll(inputs);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		boolean descend = visitor.preVisit(this);	
		if (descend) {
			for(Contract c : this.input1) {
				c.accept(visitor);
			}
			for(Contract c : this.input2) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
