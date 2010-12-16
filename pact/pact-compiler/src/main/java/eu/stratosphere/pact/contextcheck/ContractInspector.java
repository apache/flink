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

package eu.stratosphere.pact.contextcheck;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public class ContractInspector {
	private final StubInspector stubInspector;

	public ContractInspector(Contract contract) {
		stubInspector = new StubInspector(contract.getStubClass());
	}

	public Class<? extends Key> getOutputKeyClass() {
		return stubInspector.getOutputKeyClass();
	}

	public Class<? extends Value> getOutputValueClass() {
		return stubInspector.getOutputValueClass();
	}

	public Class<? extends Key> getInput1KeyClass() {
		return stubInspector.getInput1KeyClass();
	}

	public Class<? extends Value> getInput1ValueClass() {
		return stubInspector.getInput1ValueClass();
	}

	public Class<? extends Key> getInput2KeyClass() {
		return stubInspector.getInput2KeyClass();
	}

	public Class<? extends Value> getInput2ValueClass() {
		return stubInspector.getInput2ValueClass();
	}
}
