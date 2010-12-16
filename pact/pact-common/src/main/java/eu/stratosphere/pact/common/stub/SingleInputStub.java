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

package eu.stratosphere.pact.common.stub;

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType3;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType4;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Abstract stub class for all PACT stubs with a single input.
 * PACT stubs must be overwritten to provide user implementations for PACT programs.
 * 
 * @author Fabian Hueske
 * @param <IK>
 *        Type of the input key.
 * @param <IV>
 *        Type of the input value.
 * @param <OK>
 *        Type of the output key.
 * @param <OV>
 *        Type of the output value.
 */
public abstract class SingleInputStub<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		Stub<OK, OV> {
	/**
	 * Input key type.
	 */
	protected Class<IK> ik;

	/**
	 * Input value type.
	 */
	protected Class<IV> iv;

	/**
	 * Dummy implementation of Stub's the configure() method.
	 * 
	 * @see eu.stratosphere.pact.common.stub.Stub
	 */
	@Override
	public void configure(Configuration parameters) {
	}

	/**
	 * Dummy implementation of Stub's the open() method.
	 * 
	 * @see eu.stratosphere.pact.common.stub.Stub
	 */
	@Override
	public void open() {
	}

	/**
	 * Dummy implementation of Stub's the close() method.
	 * 
	 * @see eu.stratosphere.pact.common.stub.Stub
	 */
	@Override
	public void close() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initTypes() {
		this.ik = getTemplateType1(getClass());
		this.iv = getTemplateType2(getClass());
		super.ok = getTemplateType3(getClass());
		super.ov = getTemplateType4(getClass());
	}

	/**
	 * Returns the type of the input key.
	 * 
	 * @return Type of the input key.
	 */
	public Class<IK> getInKeyType() {
		return ik;
	}

	/**
	 * Returns the type of the input value.
	 * 
	 * @return Type of the input value.
	 */
	public Class<IV> getInValueType() {
		return iv;
	}
}
