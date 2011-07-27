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

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stub.DualInputStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * Contract for all tasks that have two inputs.
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public abstract class DualInputContract<IK1 extends Key, IV1 extends Value, IK2 extends Key, IV2 extends Value, OK extends Key, OV extends Value>
		extends AbstractPact<OK, OV, DualInputStub<IK1, IV1, IK2, IV2, OK, OV>>
{
	// first input contract of this contract 
	protected Contract firstInput;
	// second input contract of this contract
	protected Contract secondInput;

	/**
	 * Creates a new contract using the given stub and the given name
	 * 
	 * @param clazz
	 *        the stub class that is represented by this contract
	 * @param name
	 *        name for the task represented by this contract
	 */
	public DualInputContract(Class<? extends DualInputStub<IK1, IV1, IK2, IV2, OK, OV>> clazz, String name) {
		super(clazz, name);
	}

	/**
	 * Returns the class type of the first input key
	 * 
	 * @return The class of the first input key.
	 */
	public Class<? extends Key> getFirstInputKeyClass() {
		return ReflectionUtil.getTemplateType1(this.getClass());
	}

	/**
	 * Returns the class type of the first input value
	 * 
	 * @return The class of the first input value.
	 */
	public Class<? extends Value> getFirstInputValueClass() {
		return ReflectionUtil.getTemplateType2(this.getClass());
	}

	/**
	 * Returns the class type of the second input key
	 * 
	 * @return The class of the second input key.
	 */
	public Class<? extends Key> getSecondInputKeyClass() {
		return ReflectionUtil.getTemplateType3(this.getClass());
	}

	/**
	 * Returns the class type of the second input value
	 * 
	 * @return The type of the second input value.
	 */
	public Class<? extends Value> getSecondInputValueClass() {
		return ReflectionUtil.getTemplateType4(this.getClass());
	}

	/**
	 * Returns the class type of the output key
	 * 
	 * @return The class of the output key.
	 */
	public Class<? extends Key> getOutputKeyClass() {
		return ReflectionUtil.getTemplateType5(this.getClass());
	}

	/**
	 * Returns the class type of the output value
	 * 
	 * @return The class of the output value.
	 */
	public Class<? extends Value> getOutputValueClass() {
		return ReflectionUtil.getTemplateType6(this.getClass());
	}

	/**
	 * Returns the first input or null if none is set
	 * 
	 * @return The contract's first input contract.
	 */
	public Contract getFirstInput() {
		return firstInput;
	}

	/**
	 * Returns the second input or null if none is set
	 * 
	 * @return The contract's second input contract.
	 */
	public Contract getSecondInput() {
		return secondInput;
	}

	/**
	 * Connects the first input to the task wrapped in this contract
	 * 
	 * @param firstInput The contract that is connected as the first input.
	 */
	public void setFirstInput(Contract firstInput) {
		this.firstInput = firstInput;
	}

	/**
	 * Connects the second input to the task wrapped in this contract
	 * 
	 * @param secondInput The contract that is connected as the second input.
	 */
	public void setSecondInput(Contract secondInput) {
		this.secondInput = secondInput;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		if (visitor.preVisit(this)) {
			if (firstInput != null)
				firstInput.accept(visitor);
			if (secondInput != null)
				secondInput.accept(visitor);
			visitor.postVisit(this);
		}
	}

}
