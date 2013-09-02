/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.CopyableValue;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NormalizableKey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PactBoolean implements NormalizableKey, CopyableValue<PactBoolean> {
	private static final long serialVersionUID = 1L;

	private boolean value;

	
	public PactBoolean() {}
	
	public PactBoolean(boolean value) {
		this.value = value;
	}

	
	public boolean get() {
		return value;
	}

	public void set(boolean value) {
		this.value = value;
	}
	
	public boolean getValue() {
		return value;
	}

	public void setValue(boolean value) {
		this.value = value;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.value);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.value = in.readBoolean();
	}
	
	@Override
	public int hashCode() {
		return this.value ? 1 : 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == PactBoolean.class) {
			return ((PactBoolean) obj).value == this.value;
		}
		else return false;
	}

	@Override
	public int compareTo(Key o) {
		if (o.getClass() == PactBoolean.class) {
			final int ov = ((PactBoolean) o).value ? 1 : 0;
			final int tv = this.value ? 1 : 0;
			return tv - ov;
		} else {
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Integer!");
		}
	}
	
	@Override
	public String toString() {
		return this.value ? "true" : "false";
	}

	@Override
	public int getBinaryLength() {
		return 1;
	}

	@Override
	public void copyTo(PactBoolean target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 1);
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return 1;
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		if (len > 0) {
			target.put(offset, (byte) (this.value ? 1 : 0));
			
			for (offset = offset + 1; len > 1; len--) {
				target.put(offset++, (byte) 0);
			}
		}
	}
}
