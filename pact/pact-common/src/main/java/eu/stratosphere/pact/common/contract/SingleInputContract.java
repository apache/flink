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
import eu.stratosphere.pact.common.stub.SingleInputStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * Contract for all tasks that have one input like "map".
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public abstract class SingleInputContract<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		Contract implements OutputContractConfigurable {
	
	// user implemented stub class
	protected final Class<? extends SingleInputStub<IK, IV, OK, OV>> clazz;

	// input contract of this contract
	protected Contract input;

	// output contract
	protected Class<? extends Annotation> outputContract;

	/**
	 * Creates a new contract using the given stub and the given name
	 * 
	 * @param clazz
	 *        the stub class that is represented by this contract
	 * @param name
	 *        name for the task represented by this contract
	 */
	public SingleInputContract(Class<? extends SingleInputStub<IK, IV, OK, OV>> clazz, String name) {
		super(name);
		this.clazz = clazz;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends SingleInputStub<IK, IV, OK, OV>> getStubClass() {
		return clazz;
	}

	/**
	 * Returns the class type of the input key
	 * 
	 * @return The class of the input key.
	 */
	public Class<? extends Key> getInputKeyClass() {
		return ReflectionUtil.getTemplateType1(this.getClass());
	}

	/**
	 * Returns the class type of the input value
	 * 
	 * @return The class of the input value.
	 */
	public Class<? extends Value> getInputValueClass() {
		return ReflectionUtil.getTemplateType2(this.getClass());
	}

	/**
	 * Returns the class type of the output key
	 * 
	 * @return The class of the output key.
	 */
	public Class<? extends Key> getOutputKeyClass() {
		return ReflectionUtil.getTemplateType3(this.getClass());
	}

	/**
	 * Returns the class type of the output value
	 * 
	 * @return The class of the output value.
	 */
	public Class<? extends Value> getOutputValueClass() {
		return ReflectionUtil.getTemplateType4(this.getClass());
	}

	/**
	 * Returns the input or null if none is set
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

	@Override
	public void accept(Visitor<Contract> visitor) {
		boolean descend = visitor.preVisit(this);
		
		if (descend) {
			if (input != null) {
				input.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
