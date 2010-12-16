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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

public final class PactNull implements Key {
	public final static PactNull INSTANCE = new PactNull();

	public static PactNull getInstance() {
		return INSTANCE;
	}

	// TODO: should be private in the future to avoid unnecessary creations
	// Would involve the deserialization code to use Constructor#setAccessible
	public PactNull() {
	}

	@Override
	public void read(DataInput in) throws IOException {
		// do nothing
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// do nothing
	}

	@Override
	public String toString() {
		return "(null)";
	}

	@Override
	public int compareTo(Key o) {
		if (o.getClass() != PactNull.class) {
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Null!");
		}

		return 0;
	}

	@Override
	public boolean equals(Object o) {
		return (o.getClass() == PactNull.class);
	}

	@Override
	public int hashCode() {
		return 53;
	}
}
