/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.types;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Boxed serializable and comparable boolean type, representing the primitive
 * type {@code boolean}.
 * 
 * @see eu.stratosphere.types.Key
 */
public class BooleanValue implements NormalizableKey, ResettableValue<BooleanValue>, CopyableValue<BooleanValue> {
	
	private static final long serialVersionUID = 1L;
	
	public static final BooleanValue TRUE = new BooleanValue(true);
	
	public static final BooleanValue FALSE = new BooleanValue(false);
	

	private boolean value;

	
	public BooleanValue() {}
	
	public BooleanValue(boolean value) {
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

    @Override
    public void setValue(BooleanValue value) {
        this.value = value.value;
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
		if (obj.getClass() == BooleanValue.class) {
			return ((BooleanValue) obj).value == this.value;
		}
		else return false;
	}

	@Override
	public int compareTo(Key o) {
		if (o.getClass() == BooleanValue.class) {
			final int ov = ((BooleanValue) o).value ? 1 : 0;
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
	public void copyTo(BooleanValue target) {
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
