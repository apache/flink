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
 * @TODO
 * @author DIMA
 */
public class CrossContract<IK1 extends Key, IV1 extends Value, IK2 extends Key, IV2 extends Value, OK extends Key, OV extends Value>
		extends DualInputContract<IK1, IV1, IK2, IV2, OK, OV> {
	private static String defaultName = "Cross #";

	private static int nextID = 1;

	public CrossContract(Class<? extends CrossStub<IK1, IV1, IK2, IV2, OK, OV>> c, String n) {
		super(c, n);
	}

	public CrossContract(Class<? extends CrossStub<IK1, IV1, IK2, IV2, OK, OV>> c) {
		super(c, defaultName + (nextID++));
	}
}
