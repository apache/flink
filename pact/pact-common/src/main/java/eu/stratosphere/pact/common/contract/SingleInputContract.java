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
import eu.stratosphere.pact.common.stub.SingleInputStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * Contract for all tasks that have one input like "map".
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * @author mjsax@informatik.hu-berlin.de
 * 
 * @param <IK> type of key of input key/value-pair
 * @param <IV> type of value of input key/value-pair
 * @param <OK> type of key of output key/value-pair
 * @param <OV> type of value of output key/value-pair
 */
public abstract class SingleInputContract<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		AbstractPact<OK, OV, SingleInputStub<IK, IV, OK, OV>>
{
	// input contract of this contract
	final protected List<Contract> input = new ArrayList<Contract>();

	/**
	 * Creates a new contract using the given stub and the given name
	 * 
	 * @param clazz
	 *        the stub class that is represented by this contract
	 * @param name
	 *        name for the task represented by this contract
	 */
	public SingleInputContract(Class<? extends SingleInputStub<IK, IV, OK, OV>> clazz, String name) {
		super(clazz, name);
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


	@Override
	public void accept(Visitor<Contract> visitor) {
		boolean descend = visitor.preVisit(this);
		
		if (descend) {
			for(Contract c : this.input) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
