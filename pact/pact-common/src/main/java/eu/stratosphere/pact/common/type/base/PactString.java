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
import java.nio.CharBuffer;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NormalizableKey;

/**
 * String base type for PACT programs that implements the Key interface.
 * PactString encapsulates a Java String object.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * @see eu.stratosphere.pact.common.type.NormalizableKey
 * @see java.lang.String
 * @see java.lang.CharSequence
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class PactString implements Key, NormalizableKey, CharSequence
{
	private static final char[] EMPTY_STRING = new char[0];
	
	private static final int HIGH_BIT = 0x1 << 7;
	
	private static final int HIGH_BIT2 = 0x1 << 13;
	
	private static final int HIGH_BIT2_MASK = 0x3 << 6;
	
	
	private char[] value;		// character value of the pact string, not necessarily completely filled
	
	private int len;			// length of the pact string
	
	private int hashCode;		// cache for the hashCode

	
	// --------------------------------------------------------------------------------------------
	//                                      Constructors
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initializes the encapsulated String object with an empty string.	
	 */
	public PactString()
	{
		this.value = EMPTY_STRING;
	}
	
	/**
	 * Initializes this PactString to the value of the given string.
	 * 
	 * @param value The string containing the value for this PactString.
	 */
	public PactString(final String value)
	{
		this.value = EMPTY_STRING;
		setValue(value);
	}
	
	/**
	 * Initializes this PactString to a copy the given PactString.
	 * 
	 * @param value The initial value.
	 */
	public PactString(final PactString value)
	{
		this.value = EMPTY_STRING;
		setValue(value);
	}
	
	/**
	 * Initializes the PactString to a sub-string of the given PactString. 
	 * 
	 * @param value The string containing the substring.
	 * @param offset The offset of the substring.
	 * @param len The length of the substring.
	 */
	public PactString(final PactString value, final int offset, final int len)
	{
		this.value = EMPTY_STRING;
		setValue(value, offset, len);
	}

	// --------------------------------------------------------------------------------------------
	//                                Getters and Setters
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets a new length for the string.
	 * 
	 * @param len The new length.
	 */
	public void setLength(int len)
	{
		if (len < 0 || len > this.len)
			throw new IllegalArgumentException("Length must be between 0 and the current length.");
		this.len = len;
	}
	/**
	 * Returns this PactString's internal character data.
	 * 
	 * @return The character data.
	 */
	public char[] getCharArray() {
		return this.value;
	}
	
	/**
	 * Gets this PactString as a String.
	 * 
	 * @return A String resembling the contents of this PactString.
	 */
	public String getValue() {
		return toString();
	}

	/**
	 * Sets the value of the PactString to the given string.
	 * 
	 * @param value The new string value.
	 */
	public void setValue(final String value)
	{
		if (value == null)
			throw new NullPointerException("Value must not be null");
		
		final int len = value.length(); 
		ensureSize(len);
		
		for (int i = 0; i < len; i++) {
			this.value[i] = value.charAt(i);
		}
		this.len = len;
		this.hashCode = 0;
	}
	
	/**
	 * Sets the value of the PactString to the given string.
	 * 
	 * @param value The new string value.
	 */
	public void setValue(final PactString value)
	{
		if (value == null)
			throw new NullPointerException("Value must not be null");

		ensureSize(value.len);
		this.len = value.len;
		System.arraycopy(value.value, 0, this.value, 0, value.len);
		this.hashCode = 0;
	}
	
	/**
	 * Sets the value of the PactString to a substring of the given string.
	 * 
	 * @param value The new string value.
	 * @param offset The position to start the substring.
	 * @param len The length of the substring.
	 */
	public void setValue(final PactString value, int offset, int len)
	{
		if (value == null)
			throw new NullPointerException();
		
		if (offset < 0 || len < 0 || offset > value.len - len)
			throw new IndexOutOfBoundsException();

		ensureSize(len);
		this.len = len;
		System.arraycopy(value.value, offset, this.value, 0, len);
		this.hashCode = 0;
	}
	
	/**
	 * Sets the contents of this string to the contents of the given <tt>CharBuffer</tt>.
	 * The characters between the buffer's current position (inclusive) and the buffer's
	 * limit (exclusive) will be stored in this string.
	 *  
	 * @param buffer The character buffer to read the characters from.
	 */
	public void setValue(CharBuffer buffer)
	{
		final int len = buffer.length();
		ensureSize(len);
		buffer.get(this.value, 0, len);
		this.len = len;
		this.hashCode = 0;
	}
	
	
	public void setValueUTF8(byte[] bytes, int offset, int len) {
		throw new UnsupportedOperationException();
	}

	
	/**
	 * Sets the value of this <code>PactString</code>, assuming that the binary data is ASCII coded. The n-th character of the
	 * <code>PactString</code> corresponds directly to the n-th byte in the given array after the offset.
	 * 
	 * @param bytes The binary character data.
	 * @param offset The offset in the array.
	 * @param len The number of bytes to read from the array.
	 */
	public void setValueAscii(byte[] bytes, int offset, int len)
	{
		if (bytes == null)
			throw new NullPointerException("Bytes must not be null");
		if (len < 0 | offset < 0 | offset > bytes.length - len)
			throw new IndexOutOfBoundsException();
		
		ensureSize(len);
		this.len = len;
		this.hashCode = 0;
		
		final char[] chars = this.value;
		
		for (int i = 0, limit = offset + len; offset < limit; offset++, i++) {
			chars[i] = (char) (bytes[offset] & 0xff);
		}
	}
	
	/**
     * Returns a new <tt>PactString</tt>string that is a substring of this string. The
     * substring begins at the given <code>start</code> index and ends at end of the string
     *
     * @param start The beginning index, inclusive.
     * @return The substring.
     * @exception  IndexOutOfBoundsException Thrown, if the start is negative.
     */
	public PactString substring(int start)
	{
		return substring(start, this.len);
	}
	
	/**
     * Returns a new <tt>PactString</tt>string that is a substring of this string. The
     * substring begins at the given <code>start</code> index and ends at <code>end - 1</code>.
     *
     * @param start The beginning index, inclusive.
     * @param end The ending index, exclusive.
     * @return The substring.
     * @exception  IndexOutOfBoundsException Thrown, if the start is negative, or the end is larger than the length.
     */
	public PactString substring(int start, int end)
	{
		return new PactString(this, start, end - start);
	}
	
	public void substring(PactString target, int start)
	{
		substring(target, start, this.len);
	}
	
	public void substring(PactString target, int start, int end)
	{
		target.setValue(this, start, end - start);
	}	
	
	
	// --------------------------------------------------------------------------------------------
	//                            Serialization / De-Serialization
	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException
	{
		int len = in.readUnsignedByte();

		if (len >= HIGH_BIT) {
			int shift = 7;
			int curr;
			len = len & 0x7f;
			while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
				len |= (curr & 0x7f) << shift;
				shift += 7;
			}
			len |= curr << shift;
		}
		
		this.len = len;
		this.hashCode = 0;
		ensureSize(len);
		final char[] data = this.value;

		for (int i = 0; i < len; i++) {
			int c = in.readUnsignedByte();
			if (c < HIGH_BIT)
				data[i] = (char) c;
			else {
				int shift = 7;
				int curr;
				c = c & 0x7f;
				while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
					c |= (curr & 0x7f) << shift;
					shift += 7;
				}
				c |= curr << shift;
				data[i] = (char) c;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException
	{
		int len = this.len;

		// write the length, variable-length encoded
		while (len >= HIGH_BIT) {
			out.write(len | HIGH_BIT);
			len >>>= 7;
		}
		out.write(len);

		// write the char data, variable length encoded
		for (int i = 0; i < this.len; i++) {
			int c = this.value[i];

			while (c >= HIGH_BIT) {
				out.write(c | HIGH_BIT);
				c >>>= 7;
			}
			out.write(c);
		}
	}

	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return new String(this.value, 0, this.len);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o)
	{
		if (o instanceof PactString) {
			PactString other = (PactString) o;

			int len1 = this.len;
			int len2 = other.len;
			int n = Math.min(len1, len2);
			char v1[] = value;
			char v2[] = other.value;

			for (int k = 0; k < n; k++) {
				char c1 = v1[k];
				char c2 = v2[k];
				if (c1 != c2) {
					return c1 - c2;
				}
			}
			return len1 - len2;
		} else
			throw new ClassCastException("Cannot compare PactString to " + o.getClass().getName());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		int h = this.hashCode;
		if (h == 0 && this.len > 0) {
			int off = 0;
			char val[] = this.value;
			int len = this.len;
			for (int i = 0; i < len; i++) {
				h = 31 * h + val[off++];
			}
			this.hashCode = h;
		}
		return h;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj)
	{
		if (this == obj) {
			return true;
		}
		
		if (obj.getClass() == PactString.class) {
			final PactString other = (PactString) obj;
			int len = this.len;
			
			if (len == other.len) {
				final char[] tc = this.value;
				final char[] oc = other.value;
				int i = 0, j = 0;
				
				while (len-- != 0) {
					if (tc[i++] != oc[j++]) return false;
				}
				return true;
			}
		}
		return false;
	}

	// --------------------------------------------------------------------------------------------
	//                              Char Sequence Implementation
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see java.lang.CharSequence#length()
	 */
	@Override
	public int length()
	{
		return this.len;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.CharSequence#charAt(int)
	 */
	@Override
	public char charAt(int index)
	{
		return this.value[index];
	}

	/* (non-Javadoc)
	 * @see java.lang.CharSequence#subSequence(int, int)
	 */
	@Override
	public CharSequence subSequence(int start, int end)
	{
		return new PactString(this, start, end - start);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                   Normalized Key
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.NormalizableKey#getNormalizedKeyLen()
	 */
	@Override
	public int getMaxNormalizedKeyLen() {
		return Integer.MAX_VALUE;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.NormalizableKey#copyNormalizedKey(byte[], int, int)
	 */
	@Override
	public void copyNormalizedKey(byte[] target, int offset, int len)
	{
		final char[] chars = this.value;
		final int limit = offset + len;
		final int end = this.len;
		int pos = 0;
		
		while (pos < end && offset < limit) {
			char c = chars[pos++];
			if (c < HIGH_BIT) {
				target[offset++] = (byte) c;
			}
			else if (c < HIGH_BIT2) {
				target[offset++] = (byte) ((c >>> 7) | HIGH_BIT);
				if (offset < limit)
					target[offset++] = (byte) c;
			}
			else {
				target[offset++] = (byte) ((c >>> 10) | HIGH_BIT2_MASK);
				if (offset < limit)
					target[offset++] = (byte) (c >>> 2);
				if (offset < limit)
					target[offset++] = (byte) c;
			}
		}
		while (offset < limit) {
			target[offset++] = 0;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                      Utilities
	// --------------------------------------------------------------------------------------------

	private final void ensureSize(int size) {
		if (this.value.length < size) {
			this.value = new char[size];
		}
	}
}
