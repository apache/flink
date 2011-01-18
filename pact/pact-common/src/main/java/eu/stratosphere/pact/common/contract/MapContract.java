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

import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * MapContract represents a Map InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. 
 * They have one or multiple input sets of key/value-pairs and a first-order user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each key/value-pair independently.
 * 
 * @see eu.stratosphere.pact.common.stub.MapStub
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class MapContract<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		SingleInputContract<IK, IV, OK, OV> {
	
	private static String defaultName = "Map #";

	private static int nextID = 1;

	/**
	 * Creates a MapContract with the provided {@see eu.stratosphere.pact.common.stub.MapStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link MapStub} implementation for this Map InputContract.
	 * @param n The name of PACT.
	 */
	public MapContract(Class<? extends MapStub<IK, IV, OK, OV>> c, String n) {
		super(c, n);
	}

	/**
	 * Creates a MapContract with the provided {@see eu.stratosphere.pact.common.stub.MapStub} implementation
	 * and a default name.
	 * 
	 * @param c The {@link MapStub} implementation for this Map InputContract.
	 */
	public MapContract(Class<? extends MapStub<IK, IV, OK, OV>> c) {
		super(c, defaultName + (nextID++));
	}
}
