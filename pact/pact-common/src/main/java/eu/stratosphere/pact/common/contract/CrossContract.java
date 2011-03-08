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

import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * CrossContract represents a Cross InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. 
 * They have one or multiple input sets of key/value-pairs and a first-order user function (stub implementation).
 * <p> 
 * Cross works on two inputs and calls the first-order function of a {@see eu.stratosphere.pact.common.stub.CrossStub} 
 * for each combination of key/value-pairs of both inputs (each element of the Cartesian Product) independently.
 * 
 * @see eu.stratosphere.pact.common.stub.CrossStub
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class CrossContract<IK1 extends Key, IV1 extends Value, IK2 extends Key, IV2 extends Value, OK extends Key, OV extends Value>
		extends DualInputContract<IK1, IV1, IK2, IV2, OK, OV> {
	
	private static String defaultName = "Cross #";

	private static int nextID = 1;

	/**
	 * Creates a CrossContract with the provided {@see eu.stratosphere.pact.common.stub.CrossStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param n The name of PACT.
	 */
	public CrossContract(Class<? extends CrossStub<IK1, IV1, IK2, IV2, OK, OV>> c, String n) {
		super(c, n);
	}

	/**
	 * Creates a CrossContract with the provided {@see eu.stratosphere.pact.common.stub.CrossStub} implementation 
	 * and a default name.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 */
	public CrossContract(Class<? extends CrossStub<IK1, IV1, IK2, IV2, OK, OV>> c) {
		super(c, defaultName + (nextID++));
	}
}
