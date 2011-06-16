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

/**
 * String base type for PACT programs that implements the Key interface.
 * PactString encapsulates a Java String object.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * @see java.lang.String
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class PactString implements Key {

	private String value;

	/**
	 * Initializes the encapsulated String object with an empty string.	
	 */
	public PactString() {
		this.value = "";
	}

	/**
	 * Initializes the encapsulated String object with the provided value.
	 * 
	 * @param value Initial value of the encapsulated string.
	 */
	public PactString(final String value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated string.
	 * 
	 * @return The value of the encapsulated string.
	 */
	public String getValue() {
		return this.value;
	}

	/**
	 * Sets the value of the encapsulated string to the specified value.
	 * 
	 * @param value
	 *        The new value of the encapsulated string.
	 */
	public void setValue(final String value) {
		if (value == null)
			throw new NullPointerException("Value must not be null");

		this.value = value;
	}
	
	public void setValue(byte[] bytes) {
		if (bytes == null)
			throw new NullPointerException("Bytes must not be null");
		
		this.value = new String(bytes);
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		final int maxBit = 0x1 << 7;

		int len = in.readUnsignedByte();

		if (len >= maxBit) {
			int shift = 7;
			int curr;

			len = len & 0x7f;

			while ((curr = in.readUnsignedByte()) >= maxBit) {
				len |= (curr & 0x7f) << shift;
				shift += 7;
			}

			len |= curr << shift;
		}

		final char[] data = new char[len];

		for (int i = 0; i < len; i++) {
			int c = in.readUnsignedByte();

			if (c < maxBit)
				data[i] = (char) c;
			else {
				int shift = 7;
				int curr;

				c = c & 0x7f;

				while ((curr = in.readUnsignedByte()) >= maxBit) {
					c |= (curr & 0x7f) << shift;
					shift += 7;
				}

				c |= curr << shift;
				data[i] = (char) c;
			}
		}

		this.value = new String(data);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		final int maxBit = 0x1 << 7;

		int len = this.value.length();

		while (len >= maxBit) {
			out.write(len | maxBit);
			len >>= 7;
		}
		out.write(len);

		for (int i = 0; i < this.value.length(); i++) {
			int c = this.value.charAt(i);

			while (c >= maxBit) {
				out.write(c | maxBit);
				c >>= 7;
			}
			out.write(c);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactString))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_String!");

		return this.value.compareTo(((PactString) o).value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.value.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final PactString other = (PactString) obj;
		return this.value.equals(other.value);
	}

}
