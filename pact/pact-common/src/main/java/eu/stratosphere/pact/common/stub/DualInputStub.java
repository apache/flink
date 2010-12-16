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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Abstract stub class for all PACT stubs with two inputs.
 * PACT stubs must be overwritten to provide user implementations for PACT programs.
 * 
 * @author Fabian Hueske
 * @param <IK1>
 *        Type of the first input's key
 * @param <IV1>
 *        Type of the first input's value
 * @param <IK2>
 *        Type of the second input's key
 * @param <IV2>
 *        Type of the second input's value
 * @param <OK>
 *        Type of the output key
 * @param <OV>
 *        Type of the output value
 */
public abstract class DualInputStub<IK1 extends Key, IV1 extends Value, IK2 extends Key, IV2 extends Value, OK extends Key, OV extends Value>
		extends Stub<OK, OV> {
	/**
	 * First input's key type.
	 */
	protected Class<IK1> firstIK;

	/**
	 * First input's value type.
	 */
	protected Class<IV1> firstIV;

	/**
	 * Second input's key type.
	 */
	protected Class<IK2> secondIK;

	/**
	 * Second input's value type.
	 */
	protected Class<IV2> secondIV;

	/**
	 * Dummy implementation of Stub's the configure() method.
	 * 
	 * @see eu.stratosphere.pact.common.stub.Stub
	 */
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
	 * Returns the first input's key type.
	 * 
	 * @return First input's key type.
	 */
	public Class<IK1> getFirstInKeyType() {
		return firstIK;
	}

	/**
	 * Returns the first input's value type.
	 * 
	 * @return First input's value type.
	 */
	public Class<IV1> getFirstInValueType() {
		return firstIV;
	}

	/**
	 * Returns the second input's key type.
	 * 
	 * @return Second input's key type.
	 */
	public Class<IK2> getSecondInKeyType() {
		return secondIK;
	}

	/**
	 * Returns the second input's value type.
	 * 
	 * @return Second input's value type.
	 */
	public Class<IV2> getSecondInValueType() {
		return secondIV;
	}
}
