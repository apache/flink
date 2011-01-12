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

import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * CoGroupContract represents a CoGroup InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. 
 * They have one or multiple input sets of key/value-pairs and a first-order user function (stub implementation).
 * <p> 
 * CoGroup works on two inputs and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.CoGroupStub} 
 * with the groups of key/value-pairs sharing the same key (one group per input) independently.
 * 
 * @see eu.stratosphere.pact.common.stub.CoGroupStub
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class CoGroupContract<IK extends Key, IV1 extends Value, IV2 extends Value, OK extends Key, OV extends Value>
		extends DualInputContract<IK, IV1, IK, IV2, OK, OV> {
	
	private static String defaultName = "CoGroup #";

	private static int nextID = 1;

	/**
	 * Creates a CoGroupContract with the provided {@see eu.stratosphere.pact.common.stub.CoGroupStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link CoGroupStub} implementation for this CoGroup InputContract.
	 * @param n The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub<IK, IV1, IV2, OK, OV>> c, String n) {
		super(c, n);
	}

	/**
	 * Creates a CoGroupContract with the provided {@see eu.stratosphere.pact.common.stub.CoGroupStub} implementation 
	 * and a default name.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this CoGroup InputContract.
	 */ 
	public CoGroupContract(Class<? extends CoGroupStub<IK, IV1, IV2, OK, OV>> c) {
		super(c, defaultName + (nextID++));
	}
}
