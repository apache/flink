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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.CharBuffer;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import org.apache.commons.lang3.Validate;

/**
 * Mutable string data type that implements the Key interface.
 * StringValue encapsulates the basic functionality of a {@link String}, in a serializable and mutable way.
 * <p>
 * The mutability allows to reuse the object inside the user code, also across invocations. Reusing a StringValue object
 * helps to increase the performance, as string objects are rather heavy-weight objects and incur a lot of garbage
 * collection overhead, if created and destroyed in masses.
 * 
 * 
 * @see eu.stratosphere.types.Key
 * @see eu.stratosphere.types.NormalizableKey
 * @see java.lang.String
 * @see java.lang.CharSequence
 */
public class StringValue implements NormalizableKey<StringValue>, CharSequence, ResettableValue<StringValue>, 
		CopyableValue<StringValue>, Appendable
{
	private static final long serialVersionUID = 1L;
	
	private static final char[] EMPTY_STRING = new char[0];
	
	private static final int HIGH_BIT = 0x1 << 7;
	
	private static final int HIGH_BIT2 = 0x1 << 13;
	
	private static final int HIGH_BIT2_MASK = 0x3 << 6;
	
	private char[] value;		// character value of the string value, not necessarily completely filled
	
	private int len;			// length of the string value
	
	private int hashCode;		// cache for the hashCode

	
	// --------------------------------------------------------------------------------------------
	//                                      Constructors
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initializes the encapsulated String object with an empty string.	
	 */
	public StringValue() {
		this.value = EMPTY_STRING;
	}
	
	/**
	 * Initializes this StringValue to the value of the given string.
	 * 
	 * @param value The string containing the value for this StringValue.
	 */
	public StringValue(CharSequence value) {
		this.value = EMPTY_STRING;
		setValue(value);
	}
	
	/**
	 * Initializes this StringValue to a copy the given StringValue.
	 * 
	 * @param value The initial value.
	 */
	public StringValue(StringValue value) {
		this.value = EMPTY_STRING;
		setValue(value);
	}
	
	/**
	 * Initializes the StringValue to a sub-string of the given StringValue. 
	 * 
	 * @param value The string containing the substring.
	 * @param offset The offset of the substring.
	 * @param len The length of the substring.
	 */
	public StringValue(StringValue value, int offset, int len) {
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
	public void setLength(int len) {
		if (len < 0 || len > this.len) {
			throw new IllegalArgumentException("Length must be between 0 and the current length.");
		}
		this.len = len;
	}
	/**
	 * Returns this StringValue's internal character data. The array might be larger than the string
	 * which is currently stored in the StringValue.
	 * 
	 * @return The character data.
	 */
	public char[] getCharArray() {
		return this.value;
	}
	
	/**
	 * Gets this StringValue as a String.
	 * 
	 * @return A String resembling the contents of this StringValue.
	 */
	public String getValue() {
		return toString();
	}

	/**
	 * Sets the value of the StringValue to the given string.
	 * 
	 * @param value The new string value.
	 */
	public void setValue(CharSequence value) {
		Validate.notNull(value);
		setValue(value, 0, value.length());
	}
	
	/**
	 * Sets the value of the StringValue to the given string.
	 * 
	 * @param value The new string value.
	 */
	@Override
	public void setValue(StringValue value) {
		Validate.notNull(value);
		setValue(value.value, 0, value.len);
	}

	/**
	 * Sets the value of the StringValue to a substring of the given string.
	 * 
	 * @param value The new string value.
	 * @param offset The position to start the substring.
	 * @param len The length of the substring.
	 */
	public void setValue(StringValue value, int offset, int len) {
		Validate.notNull(value);
		setValue(value.value, offset, len);
	}
	
	/**
	 * Sets the value of the StringValue to a substring of the given string.
	 * 
	 * @param value The new string value.
	 * @param offset The position to start the substring.
	 * @param len The length of the substring.
	 */
	public void setValue(CharSequence value, int offset, int len) {
		Validate.notNull(value);
		if (offset < 0 || len < 0 || offset > value.length() - len) {
			throw new IndexOutOfBoundsException("offset: " + offset + " len: " + len + " value.len: " + len);
		}

		ensureSize(len);
		this.len = len;		
		for (int i = 0; i < len; i++) {
			this.value[i] = value.charAt(offset + i);
		}
		this.len = len;
		this.hashCode = 0;
	}
	
	/**
	 * Sets the contents of this string to the contents of the given <tt>CharBuffer</tt>.
	 * The characters between the buffer's current position (inclusive) and the buffer's
	 * limit (exclusive) will be stored in this string.
	 *  
	 * @param buffer The character buffer to read the characters from.
	 */
	public void setValue(CharBuffer buffer) {
		Validate.notNull(buffer);
		final int len = buffer.length();
		ensureSize(len);
		buffer.get(this.value, 0, len);
		this.len = len;
		this.hashCode = 0;
	}
	
	/**
	 * Sets the value of the StringValue to a substring of the given value.
	 * 
	 * @param chars The new string value (as a character array).
	 * @param offset The position to start the substring.
	 * @param len The length of the substring.
	 */
	public void setValue(char[] chars, int offset, int len) {
		Validate.notNull(chars);
		if (offset < 0 || len < 0 || offset > chars.length - len) {
			throw new IndexOutOfBoundsException();
		}

		ensureSize(len);
		System.arraycopy(chars, offset, this.value, 0, len);
		this.len = len;
		this.hashCode = 0;
	}
	
	/**
	 * Sets the value of this <code>StringValue</code>, assuming that the binary data is ASCII coded. The n-th character of the
	 * <code>StringValue</code> corresponds directly to the n-th byte in the given array after the offset.
	 * 
	 * @param bytes The binary character data.
	 * @param offset The offset in the array.
	 * @param len The number of bytes to read from the array.
	 */
	public void setValueAscii(byte[] bytes, int offset, int len) {
		if (bytes == null) {
			throw new NullPointerException("Bytes must not be null");
		}
		if (len < 0 | offset < 0 | offset > bytes.length - len) {
			throw new IndexOutOfBoundsException();
		}
		
		ensureSize(len);
		this.len = len;
		this.hashCode = 0;
		
		final char[] chars = this.value;
		
		for (int i = 0, limit = offset + len; offset < limit; offset++, i++) {
			chars[i] = (char) (bytes[offset] & 0xff);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    String Methods
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns a new <tt>StringValue</tt>string that is a substring of this string. The
	 * substring begins at the given <code>start</code> index and ends at end of the string
	 *
	 * @param start The beginning index, inclusive.
	 * @return The substring.
	 * @exception  IndexOutOfBoundsException Thrown, if the start is negative.
	 */
	public StringValue substring(int start) {
		return substring(start, this.len);
	}
	
	/**
	 * Returns a new <tt>StringValue</tt>string that is a substring of this string. The
	 * substring begins at the given <code>start</code> index and ends at <code>end - 1</code>.
	 * 
	 * @param start The beginning index, inclusive.
	 * @param end The ending index, exclusive.
	 * @return The substring.
	 * @exception IndexOutOfBoundsException
	 *            Thrown, if the start is negative, or the end is larger than the length.
	 */
	public StringValue substring(int start, int end) {
		return new StringValue(this, start, end - start);
	}
	
	/**
	 * Copies a substring of this string into the given target StringValue. The
	 * substring begins at the given <code>start</code> index and ends at end of the string
	 *
	 * @param target The StringValue object to copy the substring to.
	 * @param start The beginning index, inclusive.
	 * @exception  IndexOutOfBoundsException Thrown, if the start is negative.
	 */
	public void substring(StringValue target, int start) {
		substring(target, start, this.len);
	}
	
	/**
	 * Copies a substring of this string into the given target StringValue. The
	 * substring begins at the given <code>start</code> index and ends at <code>end - 1</code>.
	 * 
	 * @param target The StringValue object to copy the substring to.
	 * @param start The beginning index, inclusive.
	 * @param end The ending index, exclusive.
	 * @exception IndexOutOfBoundsException
	 *            Thrown, if the start is negative, or the end is larger than the length.
	 */
	public void substring(StringValue target, int start, int end) {
		target.setValue(this, start, end - start);
	}
	
	/**
	 * Finds any occurrence of the <code>str</code> character sequence in this StringValue.
	 * 
	 * @return The position of the first occurrence of the search string in the string value, or <code>-1</code>, if
	 *         the character sequence was not found.
	 */
	public int find(CharSequence str) {
		return find(str, 0);
	}

	/**
	 * Finds any occurrence of the <code>str</code> character sequence in this StringValue.
	 * The search starts at position <code>start</code>.
	 * 
	 * @return The position of the first occurrence of the search string in the string value, or <code>-1</code>, if
	 *         the character sequence was not found.
	 */
	public int find(CharSequence str, int start) {
		final int pLen = this.len;
		final int sLen = str.length();
		
		if (sLen == 0) {
			throw new IllegalArgumentException("Cannot find empty string.");
		}
		
		int pPos = start;
		
		final char first = str.charAt(0);
		
		while (pPos < pLen) {
			if (first == this.value[pPos++]) {
				// matching first character
				final int fallBackPosition = pPos;
				int sPos = 1;
				boolean found = true;
				
				while (sPos < sLen) {
					if (pPos >= pLen) {
						// no more characters in string value
						pPos = fallBackPosition;
						found = false;
						break;
					}
					
					if (str.charAt(sPos++) != this.value[pPos++]) {
						pPos = fallBackPosition;
						found = false;
						break;
					}
				}
				if (found) {
					return fallBackPosition - 1;
				}
			}
		}
		return -1;
	}
	
	/**
	 * Checks whether the substring, starting at the specified index, starts with the given prefix string.
	 * 
	 * @param prefix The prefix character sequence.
	 * @param startIndex The position to start checking for the prefix.
	 * 
	 * @return True, if this StringValue substring, starting at position <code>startIndex</code> has </code>prefix</code>
	 *         as its prefix.
	 */
	public boolean startsWith(CharSequence prefix, int startIndex) {
		final char[] thisChars = this.value;
		final int pLen = this.len;
		final int sLen = prefix.length();
	
		if ((startIndex < 0) || (startIndex > pLen - sLen)) {
			return false;
		}
		
		int sPos = 0;
		while (sPos < sLen) {
			if (thisChars[startIndex++] != prefix.charAt(sPos++)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks whether this StringValue starts with the given prefix string.
	 * 
	 * @param prefix The prefix character sequence.
	 * 
	 * @return True, if this StringValue has </code>prefix</code> as its prefix.
	 */
	public boolean startsWith(CharSequence prefix) {
		return startsWith(prefix, 0);
	}
	
	// --------------------------------------------------------------------------------------------
	// Appendable Methods
	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(char)
	 */
	@Override
	public Appendable append(char c) {
		grow(this.len + 1);
		this.value[this.len++] = c;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence)
	 */
	@Override
	public Appendable append(CharSequence csq) {
		append(csq, 0, csq.length());
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence, int, int)
	 */
	@Override
	public Appendable append(CharSequence csq, int start, int end) {
		final int otherLen = end - start;
		grow(this.len + otherLen);
		for (int pos = start; pos < end; pos++) {
			this.value[this.len + pos] = csq.charAt(pos);
		}
		this.len += otherLen;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence)
	 */
	public Appendable append(StringValue csq) {
		append(csq, 0, csq.length());
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence, int, int)
	 */
	public Appendable append(StringValue csq, int start, int end) {
		final int otherLen = end - start;
		grow(this.len + otherLen);
		System.arraycopy(csq.value, start, this.value, this.len, otherLen);
		this.len += otherLen;
		return this;
	}
		
	// --------------------------------------------------------------------------------------------
	//                            Serialization / De-Serialization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(final DataInput in) throws IOException {
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
			if (c < HIGH_BIT) {
				data[i] = (char) c;
			} else {
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

	@Override
	public void write(final DataOutput out) throws IOException {
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
	
	@Override
	public String toString() {
		return new String(this.value, 0, this.len);
	}

	@Override
	public int compareTo(StringValue other) {
		int len1 = this.len;
		int len2 = other.len;
		int n = Math.min(len1, len2);
		char[] v1 = value;
		char[] v2 = other.value;

		for (int k = 0; k < n; k++) {
			char c1 = v1[k];
			char c2 = v2[k];
			if (c1 != c2) {
				return c1 - c2;
			}
		}
		return len1 - len2;
	}

	@Override
	public int hashCode() {
		int h = this.hashCode;
		if (h == 0 && this.len > 0) {
			int off = 0;
			char[] val = this.value;
			int len = this.len;
			for (int i = 0; i < len; i++) {
				h = 31 * h + val[off++];
			}
			this.hashCode = h;
		}
		return h;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (obj.getClass() == StringValue.class) {
			final StringValue other = (StringValue) obj;
			int len = this.len;
			
			if (len == other.len) {
				final char[] tc = this.value;
				final char[] oc = other.value;
				int i = 0, j = 0;
				
				while (len-- != 0) {
					if (tc[i++] != oc[j++]) {
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}

	// --------------------------------------------------------------------------------------------
	//                              Char Sequence Implementation
	// --------------------------------------------------------------------------------------------

	@Override
	public int length() {
		return this.len;
	}
	
	@Override
	public char charAt(int index) {
		if (index < len) {
			return this.value[index];	
		}
		else {
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		return new StringValue(this, start, end - start);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                   Normalized Key
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getMaxNormalizedKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		// cache variables on stack, avoid repeated dereferencing of "this"
		final char[] chars = this.value;
		final int limit = offset + len;
		final int end = this.len;
		int pos = 0;
		
		while (pos < end && offset < limit) {
			char c = chars[pos++];
			if (c < HIGH_BIT) {
				target.put(offset++, (byte) c);
			}
			else if (c < HIGH_BIT2) {
				target.put(offset++, (byte) ((c >>> 7) | HIGH_BIT));
				if (offset < limit) {
					target.put(offset++, (byte) c);
				}
			}
			else {
				target.put(offset++, (byte) ((c >>> 10) | HIGH_BIT2_MASK));
				if (offset < limit) {
					target.put(offset++, (byte) (c >>> 2));
				}
				if (offset < limit) {
					target.put(offset++, (byte) c);
				}
			}
		}
		while (offset < limit) {
			target.put(offset++, (byte) 0);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return -1;
	}
	
	@Override
	public void copyTo(StringValue target) {
		target.len = this.len;
		target.hashCode = this.hashCode;
		target.ensureSize(this.len);
		System.arraycopy(this.value, 0, target.value, 0, this.len);
	}
	
	@Override
	public void copy(DataInputView in, DataOutputView target) throws IOException {
		int len = in.readUnsignedByte();
		target.writeByte(len);

		if (len >= HIGH_BIT) {
			int shift = 7;
			int curr;
			len = len & 0x7f;
			while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
				len |= (curr & 0x7f) << shift;
				shift += 7;
				target.writeByte(curr);
			}
			len |= curr << shift;
		}

		for (int i = 0; i < len; i++) {
			int c = in.readUnsignedByte();
			target.writeByte(c);
			if (c >= HIGH_BIT) {
				int curr;
				while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
					target.writeByte(curr);
				}
			}
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
	
	/**
	 * Grow and retain content.
	 */
	private final void grow(int size) {
		if (this.value.length < size) {
			char[] value = new char[ Math.max(this.value.length * 3 / 2, size)];
			System.arraycopy(this.value, 0, value, 0, this.len);
			this.value = value;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                           Static Helpers for String Serialization
	// --------------------------------------------------------------------------------------------
	
	public static final String readString(DataInput in) throws IOException {
		// the length we read is offset by one, because a length of zero indicates a null value
		int len = in.readUnsignedByte();
		
		if (len == 0) {
			return null;
		}

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
		
		// subtract one for the null length
		len -= 1;
		
		final char[] data = new char[len];

		for (int i = 0; i < len; i++) {
			int c = in.readUnsignedByte();
			if (c < HIGH_BIT) {
				data[i] = (char) c;
			} else {
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
		
		return new String(data, 0, len);
	}

	public static final void writeString(CharSequence cs, DataOutput out) throws IOException {
		if (cs != null) {
			// the length we write is offset by one, because a length of zero indicates a null value
			int lenToWrite = cs.length()+1;
			if (lenToWrite < 0) {
				throw new IllegalArgumentException("CharSequence is too long.");
			}
	
			// write the length, variable-length encoded
			while (lenToWrite >= HIGH_BIT) {
				out.write(lenToWrite | HIGH_BIT);
				lenToWrite >>>= 7;
			}
			out.write(lenToWrite);
	
			// write the char data, variable length encoded
			for (int i = 0; i < cs.length(); i++) {
				int c = cs.charAt(i);
	
				while (c >= HIGH_BIT) {
					out.write(c | HIGH_BIT);
					c >>>= 7;
				}
				out.write(c);
			}
		} else {
			out.write(0);
		}
	}
	
	public static final void copyString(DataInput in, DataOutput out) throws IOException {
		int len = in.readUnsignedByte();
		out.writeByte(len);

		if (len >= HIGH_BIT) {
			int shift = 7;
			int curr;
			len = len & 0x7f;
			while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
				out.writeByte(curr);
				len |= (curr & 0x7f) << shift;
				shift += 7;
			}
			out.writeByte(curr);
			len |= curr << shift;
		}

		// note that the length is one larger than the actual length (length 0 is a null string, not a zero length string)
		len--;

		for (int i = 0; i < len; i++) {
			int c = in.readUnsignedByte();
			out.writeByte(c);
			while (c >= HIGH_BIT) {
				c = in.readUnsignedByte();
				out.writeByte(c);
			}
		}
	}
	
	/**
	 Writes a CharSequence as a variable-length encoded Unicode String.
	 @param cs CharSequence to write
	 @param out output channel
	 @throws IOException 
	 */
	public static final void writeUnicode(CharSequence cs, DataOutput out) throws IOException {
		if (cs == null) {
			writeLength(0, out);
		} else {
			writeLength(Character.codePointCount(cs, 0, cs.length()) + 1, out);
			for (int i = 0; i < Character.codePointCount(cs, 0, cs.length()); i++) {
				int c = Character.codePointAt(cs, i);
				if (c >= 65536) {
					//Non-BMP Unicode character, two characters are treated as one
					i++;
				}

				int shift = 0;
				int count = 0;
				//determine number of bytes needed
				while (c >= (HIGH_BIT << (shift - count))) {
					shift += 8;
					count++;
				}
				//write bytes
				while (shift >= 0) {
					if (shift == 0) {
						out.write(c & 0x7F);
					} else {
						out.write((c >>> (shift - count)) | 0x80);
					}
					shift -= 8;
					count--;
				}
			}
		}

	}
	
	/**
	Writes the given int variable-length encoded to the given DataOutput. NOT adjusted for null offset.
	@param lenToWrite int to write
	@param out output
	@throws IOException 
	*/
	private static void writeLength(int lenToWrite, DataOutput out) throws IOException {
		if (lenToWrite < 0) {
			throw new IllegalArgumentException("CharSequence is too long.");
		}

		// write the length, variable-length encoded
		while (lenToWrite >= HIGH_BIT) {
			out.write(lenToWrite | HIGH_BIT);
			lenToWrite >>>= 7;
		}
		out.write(lenToWrite);
	}

	/**
	 Reads and returns a variable-length encoded Unicode String.
	 @param in input channel
	 @return Unicode String
	 @throws IOException 
	 */
	public static final String readUnicode(DataInput in) throws IOException {
		int len = readLength(in);
		
		if(len==0){
			return "";
		}
		
		final int[] data = new int[len];
		
		for (int i = 0; i < len; i++) {
			data[i] = readUnicodeChar(in);
		}
		return new String(data, 0, len);
	}
	
	/**
	Reads and returns a variable-length encoded Unicode Character.
	@param in input channel
	@return Unicode Character
	@throws IOException 
	*/
	private static int readUnicodeChar(DataInput in) throws IOException {
		int r = 0;
		int c;
		while ((c = in.readUnsignedByte()) >= HIGH_BIT) {
			r |= (c & 0x7F) ;
			r <<= 7;
		}
		r |= c;
		return r;
	}
	
	/**
	Reads a variable-length encoded int from the given DataInput. Adjusted for null offset.
	@param in input
	@return read int
	@throws IOException 
	*/
	private static int readLength(DataInput in) throws IOException {
		// the length we read is offset by one, because a length of zero indicates a null value
		int len = in.readUnsignedByte();

		if (len == 0) {
			return 0;
		}

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

		// subtract one for the null length
		len -= 1;

		return len;
	}

	/**
	 Copies a serialized variable-length encoded Unicode String.
	 @param in input channel
	 @param out output channel
	 @throws IOException 
	 */
	public static final void copyUnicode(DataInput in, DataOutput out) throws IOException {
		//copy length
		int length = readLength(in);
		// the length we write is offset by one, because a length of zero indicates a null value
		writeLength(length + 1, out);

		//copy data
		for (int i = 0; i < length; i++) {
			int c;
			while ((c = in.readUnsignedByte()) >= HIGH_BIT) {
				out.writeByte(c);
			}
			out.writeByte(c);
		}
	}

	/**
	 Compares two serialized variable-length encoded Unicode String.
	 @param firstSource input channel
	 @param secondSource input channel
	 @return A negative value if the first String is less than the second, 0 if equal, a positive value if greater.
	 @throws IOException 
	 */
	public static final int compareUnicode(DataInputView firstSource, DataInputView secondSource) throws IOException {
		int lengthFirst = readLength(firstSource);
		int lengthSecond = readLength(secondSource);

		for (int i = 0; i < Math.min(lengthFirst, lengthSecond); i++) {			
			int c1 = readUnicodeChar(firstSource);
			int c2 = readUnicodeChar(secondSource);
			int cmp = c1 - c2;
			if (cmp != 0) {
				return cmp;
			}
		}
		//the first min(lengthFirst, lengthSecond) characterss are equal, longer String > shorter String
		return lengthFirst - lengthSecond;
	}
}
