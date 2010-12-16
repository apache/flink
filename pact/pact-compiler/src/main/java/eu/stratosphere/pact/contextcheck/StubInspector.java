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

import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * @TODO
 * @author Erik Nijkamp
 */
@SuppressWarnings("unchecked")
public class StubInspector {
	private final Class<?>[] types;

	public StubInspector(Class<? extends Stub<?, ?>> stub) {
		this.types = ReflectionUtil.getSuperTemplateTypes(stub);
	}

	public Class<? extends Key> getOutputKeyClass() {
		return (Class<? extends Key>) types[types.length - 2];
	}

	public Class<? extends Value> getOutputValueClass() {
		return (Class<? extends Value>) types[types.length - 1];
	}

	public Class<? extends Key> getInput1KeyClass() {
		return (Class<? extends Key>) types[0];
	}

	public Class<? extends Value> getInput1ValueClass() {
		return (Class<? extends Value>) types[1];
	}

	public Class<? extends Key> getInput2KeyClass() {
		return (Class<? extends Key>) types[2];
	}

	public Class<? extends Value> getInput2ValueClass() {
		return (Class<? extends Value>) types[3];
	}
}
