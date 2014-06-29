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

package eu.stratosphere.nephele.types;

import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

/**
 * This class represents record for integer values.
 */
public class IntegerRecord implements IOReadableWritable {

	/**
	 * The integer value represented by the record.
	 */
	private int value = 0;

	/**
	 * Constructs a new integer record with the given integer value.
	 * 
	 * @param value
	 *        the integer value this record should wrap up
	 */
	public IntegerRecord(final int value) {
		this.value = value;
	}

	/**
	 * Constructs an empty integer record (Mainly used for
	 * serialization, do not call this constructor in your program).
	 */
	public IntegerRecord() {}

	/**
	 * Returns the value of this integer record.
	 * 
	 * @return the value of this integer record
	 */
	public int getValue() {
		return this.value;
	}

	/**
	 * Set the value of this integer record.
	 * 
	 * @param value
	 *        the new value for this integer record
	 */
	public void setValue(final int value) {
		this.value = value;
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		// Simply read the value from the stream
		this.value = in.readInt();
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		// Simply write the value to the stream
		out.writeInt(this.value);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof IntegerRecord)) {
			return false;
		}

		final IntegerRecord ir = (IntegerRecord) obj;
		return (this.value == ir.value);
	}


	@Override
	public int hashCode() {
		return this.value;
	}
}
