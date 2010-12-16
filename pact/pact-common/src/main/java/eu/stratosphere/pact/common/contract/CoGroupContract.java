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
 * @TODO
 * @author DIMA
 */
public class CoGroupContract<IK extends Key, IV1 extends Value, IV2 extends Value, OK extends Key, OV extends Value>
		extends DualInputContract<IK, IV1, IK, IV2, OK, OV> {
	private static String defaultName = "CoGroup #";

	private static int nextID = 1;

	public CoGroupContract(Class<? extends CoGroupStub<IK, IV1, IV2, OK, OV>> c, String n) {
		super(c, n);
	}

	public CoGroupContract(Class<? extends CoGroupStub<IK, IV1, IV2, OK, OV>> c) {
		super(c, defaultName + (nextID++));
	}
}
