/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.test.testPrograms.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class Tuple implements Value {

	private byte[] bytes;

	private short[] offsets;

	private int numCols;

	/**
	 * Instantiates an empty tuple.
	 */
	public Tuple() {
		numCols = 0;
	}

	/**
	 * Creates a new tuple with a given set of attributes.
	 * 
	 * @param bytes
	 *        The bytes array. Attributes are separated by a single character. The last attribute
	 *        is also terminated with a single character. 
	 * @param offsets
	 *        The offsets of the columns in the byte array. The last entry gives the offset of the terminating
	 *        character + 1 (if the byte array exactly holds all attributes and delimiting characters this is 
	 *        the length of the array).
	 * @param cols
	 *        The number of columns.
	 */
	public Tuple(byte[] bytes, short[] offsets, int cols) {
		this.bytes = bytes;
		this.offsets = offsets;
		this.numCols = cols;
	}

	// ------------------------------------------------------------------------
	// Accessors
	// ------------------------------------------------------------------------

	/**
	 * Returns the number of attributes / columns of the tuple.
	 * 
	 * @return The number of columns of the tuple.
	 */
	public int getNumberOfColumns() {
		return numCols;
	}

	/**
	 * Returns the internal byte array of the tuple.
	 * 
	 * @return The internal byte array of the tuple.
	 */
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * Returns the length of the column with the specified index. Column indices start at 0.
	 * 
	 * @param colNumber Index of the column. Indices start at 0.
	 * @return The length of the specified column.
	 */
	public int getColumnLength(int colNumber) {
		if(offsets == null) return -1;
		if(colNumber < 0) return -1;
		if(colNumber >= offsets.length) return -1;
		return offsets[colNumber + 1] - offsets[colNumber] - 1;
	}

	// ------------------------------------------------------------------------
	// Modification
	// ------------------------------------------------------------------------

	/**
	 * Appends all columns of the specified tuple to this tuple.
	 * 
	 * @param other	The tuple whose columns are appended to this tuple.
	 */
	public void concatenate(Tuple other) {
		
		if(other.getBytes() == null) return;
		
		if (bytes == null) {
			bytes = (byte[]) other.bytes.clone();
			offsets = (short[]) other.offsets.clone();
			numCols = other.numCols;
		} else {
			int len = offsets[numCols];
			int otherLen = other.offsets[other.numCols];
			int totalLen = len + otherLen;

			// bytes:
			// our content
			if (bytes.length < totalLen) {
				byte[] tmp = new byte[totalLen];
				System.arraycopy(bytes, 0, tmp, 0, len);
				bytes = tmp;
			}

			// the other's content
			System.arraycopy(other.bytes, 0, bytes, len, otherLen);

			// offsets
			if (offsets.length < numCols + other.numCols + 1) {
				short[] tmp = new short[numCols + other.numCols + 1];
				System.arraycopy(offsets, 0, tmp, 0, numCols + 1);
				offsets = tmp;
			}

			// other offsets
			for (int i = 1; i < other.numCols + 1; i++) {
				offsets[numCols + i] = (short) (other.offsets[i] + len);
			}

			numCols += other.numCols;
		}
	}

	/**
	 * Performs a projection on the tuple.
	 * The int parameter is interpreted as a bitmap on the columns.
	 * I.e. a bitmap value of 1 projects to the first column, 2 to the second, 3 to the first two columns, and so on.
	 * 
	 * @param bitmap the projection bitmap.
	 */
	public void project(int bitmap) {
		int[] lengths = new int[numCols];
		int lenCount = 0;

		if(bytes == null || offsets == null) return;
		
		// go through the bitmap and find the indexes of the columns to retain
		int k = 0;
		for (int i = 0; bitmap != 0 && i < numCols; i++, bitmap >>>= 1) {
			if ((bitmap & 0x1) != 0) {
				int len = offsets[i + 1] - offsets[i];
				lengths[k] = len;
				lenCount += len;
				offsets[k] = offsets[i];
				k++;
			}
		}
		numCols = k;

		// allocate the new (smaller) array
		byte[] tmp = new byte[lenCount];
		lenCount = 0;

		// copy the columns to the beginning and adjust the offsets to the new array
		for (int i = 0; i < k; i++) {
			System.arraycopy(bytes, offsets[i], tmp, lenCount, lengths[i]);
			offsets[i] = (short) lenCount;
			lenCount += lengths[i];
		}

		bytes = tmp;
		offsets[numCols] = (short) tmp.length;
	}

	/**
	 * Compares a String attribute of this tuple with a String attribute of another tuple.
	 * The strings are compared lexicographic.
	 * 
	 * @param other The other tuple.
	 * @param thisColumn The index of this tuple's String attribute.
	 * @param otherColumn The index of the other tuple's String attribute.
	 * @return 1 if this tuple's attribute is greater, 0 if both attributes have the same value, 
	 *         -1 if this tuple's attribute is smaller.
	 * @throws IndexOutOfBoundsException Thrown if one of the column indices is invalid (smaller than 0 or bigger 
	 *            than the attribute count).
	 */
	public int compareStringAttribute(Tuple other, int thisColumn, int otherColumn) {
		
		if(thisColumn < 0) throw new IndexOutOfBoundsException();
		if(otherColumn < 0) throw new IndexOutOfBoundsException();
		if(thisColumn >= numCols) throw new IndexOutOfBoundsException();
		if(otherColumn >= other.numCols) throw new IndexOutOfBoundsException();
		
		int len = getColumnLength(thisColumn);
		int otherLen = other.getColumnLength(otherColumn);
		int min = Math.min(len, otherLen);

		int startPos = offsets[thisColumn];
		int otherStartPos = other.offsets[otherColumn];
		
		for (int i = 0; i < min; i++) {
			if (bytes[startPos + i] < other.bytes[otherStartPos + i]) {
				return -1;
			} else if (bytes[startPos + i] > other.bytes[otherStartPos + i]) {
				return 1;
			}
		}

		if (len < otherLen) {
			return -1;
		} else if (len > otherLen) {
			return 1;
		} else {
			return 0;
		}
	}

	/**
	 * Compares an Integer attribute of this tuple with an Integer attribute of another tuple.
	 * 
	 * @param other The other tuple.
	 * @param thisColumn The index of this tuple's int attribute.
	 * @param otherColumn The index of the other tuple's int attribute.
	 * @return 1 if this tuple's attribute is greater, 0 if both attributes have the same value, 
	 *         -1 if this tuple's attribute is smaller.
	 * @throws IndexOutOfBoundsException Thrown if one of the column indices is invalid (smaller than 0 or bigger 
	 *            than the attribute count).
	 */
	public int compareIntAttribute(Tuple other, int thisColumn, int otherColumn) {
		int len = getColumnLength(thisColumn);
		int otherLen = other.getColumnLength(otherColumn);

		if(thisColumn < 0) throw new IndexOutOfBoundsException();
		if(otherColumn < 0) throw new IndexOutOfBoundsException();
		if(thisColumn >= numCols) throw new IndexOutOfBoundsException();
		if(otherColumn >= other.numCols) throw new IndexOutOfBoundsException();
		
		short thisNegative = 1;
		short otherNegative = 1;
		
		if(this.bytes[offsets[thisColumn]] == '-') {
			thisNegative = -1;
		}
		
		if(other.getBytes()[other.offsets[otherColumn]] == '-') {
			otherNegative = -1;
		}
		
		// check one int is negative
		if(thisNegative != otherNegative) {
			return thisNegative;
		}

		// check if they vary in length
		if (len < otherLen) {
			return -1 * thisNegative;
		} else if (len > otherLen) {
			return 1 * thisNegative;
		}

		// both have the same orientation and length, check digit-wise
		int myStartPos = offsets[thisColumn];
		int compStartPos = other.offsets[otherColumn];

		for (int i = 0; i < len; i++) {
			if (bytes[myStartPos + i] < other.bytes[compStartPos + i]) {
				return -1 * thisNegative;
			} else if (bytes[myStartPos + i] > other.bytes[compStartPos + i]) {
				return 1 * thisNegative;
			}
		}
		return 0;

	}

	/**
	 * Returns the String value of the attribute with the specified index.
	 * 
	 * @param column The index of the attribute whose String value is returned.
	 * @return The String value of the specified attribute.
	 * @throws IndexOutOfBoundsException Thrown if the index of the column is invalid (smaller than 0 or bigger 
	 *            than the attribute count).
	 */
	public String getStringValueAt(int column) throws IndexOutOfBoundsException {
		// check for validity of column index
		if(column < 0) throw new IndexOutOfBoundsException();
		if(column >= numCols) throw new IndexOutOfBoundsException();
		
		int off = offsets[column];
		int len = getColumnLength(column);

		char[] chars = new char[len];
		for (int i = 0; i < len; i++) {
			chars[i] = (char) (bytes[off + i] & 0xff);
		}

		return new String(chars);
	}

	/**
	 * Returns the Long value of the attribute with the specified index.
	 * The value must be represented in the decimal system.
	 *  
	 * @param column The index of the attribute whose value is returned as long.
	 * @return The long value of the specified attribute.
	 * @throws IndexOutOfBoundsException Thrown if the index of the column is invalid (smaller than 0 or bigger 
	 *            than the attribute count).
	 * @throws NumberFormatException Thrown if the attribute is not a valid long value 
	 *            (contains any other character than digits or '-'.)
	 */
	public long getLongValueAt(int column) throws IndexOutOfBoundsException, NumberFormatException {
		
		if(column < 0) throw new IndexOutOfBoundsException();
		if(column >= numCols) throw new IndexOutOfBoundsException();
		
		int off = offsets[column];
		int len = getColumnLength(column);

		boolean isNegative = false;

		if(bytes[off] == '-') {
			isNegative = true;
			off++;
			len--;
		}
		
		long value = 0;
		for (int i = off; i < off + len; i++) {
			
			if(bytes[i] < '0' || bytes[i] > '9') throw new NumberFormatException();
			
			value *= 10;
			value += (bytes[i] - 48);
		}
		
		if(isNegative) {
			value *= -1;
		}

		return value;
	}

	/**
	 * Returns an attribute which is specified by an index as byte array. 
	 * 
	 * @param column The index of the attribute which is returned as byte array.
	 * @return The value of the specified attribute as byte array value.
	 * @throws IndexOutOfBoundsException Thrown if the index of the column is invalid (smaller than 0 or bigger 
	 *            than the attribute count).
	 */
	public byte[] getByteArrayValueAt(int column) throws IndexOutOfBoundsException {
		
		if(column < 0) throw new IndexOutOfBoundsException();
		if(column >= numCols) throw new IndexOutOfBoundsException();
		
		int len = getColumnLength(column);
		byte[] buffer = new byte[len];
		System.arraycopy(bytes, offsets[column], buffer, 0, len);
		return buffer;
	}

	/**
	 * Sets the size of the internal byte array of the tuple to the minimum capacity.
	 * If the minimum capacity is smaller than the current size of the tuple's byte array,
	 * nothing is done. Otherwise a new byte array is allocated and the content of the old one copied.
	 * 
	 * @param minCapacity The new size of the internal byte array. 
	 */
	public void reserveSpace(int minCapacity) {
		if (bytes.length < minCapacity) {
			byte[] tmp = new byte[minCapacity];
			System.arraycopy(bytes, 0, tmp, 0, offsets[numCols]);
			bytes = tmp;
		}
	}

	/**
	 * Reduces the size of the internal byte and offset arrays to the currently used size.
	 */
	public void compact() {
		int len = offsets[numCols];

		if (bytes.length > len) {
			byte[] tmp = new byte[len];
			System.arraycopy(bytes, 0, tmp, 0, len);
			bytes = tmp;
		}

		if (offsets.length > numCols + 1) {
			short[] tmp = new short[numCols + 1];
			System.arraycopy(offsets, 0, tmp, 0, numCols + 1);
			offsets = tmp;
		}
	}

	/**
	 * Appends an attribute at the end of the tuple.
	 * 
	 * @param attValue The attribute to append.
	 */
	public void addAttribute(byte[] attValue) {
		int end;

		if (numCols == 0) {
			offsets = new short[5];
			bytes = new byte[Math.max(256, attValue.length + 1)];
			end = 0;
		} else {
			end = offsets[numCols];

			// increase offset array, if necessary
			if (numCols + 1 >= offsets.length) {
				short[] tmp = new short[offsets.length * 2];
				System.arraycopy(offsets, 0, tmp, 0, numCols + 1);
				offsets = tmp;
			}

			// increase byte buffer, if necessary
			if (bytes.length < end + attValue.length + 1) {
				byte[] tmp = new byte[bytes.length + attValue.length + 1];
				System.arraycopy(bytes, 0, tmp, 0, end);
				bytes = tmp;
			}
		}

		// copy bytes, offsets and increase columns
		System.arraycopy(attValue, 0, bytes, end, attValue.length);
		end += attValue.length;
		bytes[end++] = '|';
		numCols++;
		offsets[numCols] = (short) end;
	}

	/**
	 * Appends an attribute at the end of the tuple.
	 * 
	 * @param attValue The attribute to append.
	 */
	public void addAttribute(String attValue) {
		int end;

		if (numCols == 0) {
			offsets = new short[5];
			bytes = new byte[Math.max(256, attValue.length() + 1)];
			end = 0;
		} else {
			end = offsets[numCols];

			// increase offset array, if necessary
			if (numCols + 1 >= offsets.length) {
				short[] tmp = new short[offsets.length * 2];
				System.arraycopy(offsets, 0, tmp, 0, numCols + 1);
				offsets = tmp;
			}

			// increase byte buffer, if necessary
			if (bytes.length < end + attValue.length() + 1) {
				byte[] tmp = new byte[bytes.length + attValue.length() + 1];
				System.arraycopy(bytes, 0, tmp, 0, end);
				bytes = tmp;
			}
		}

		// copy bytes, offsets and increase columns
		for (int i = 0; i < attValue.length(); i++, end++) {
			bytes[end] = (byte) (attValue.charAt(i) & 0xff);
		}
		bytes[end++] = '|';
		numCols++;
		offsets[numCols] = (short) end;
	}
	
	/**
	 * Appends an attribute by copying it from another tuple.
	 * 
	 * @param other The other tuple to copy from.
	 * @param column The index of the attribute to copy within the other tuple.
	 */
	public void addAttributeFromKVRecord(Tuple other, int column) {
		int len = other.getColumnLength(column) + 1;
		int end;

		if (numCols == 0) {
			offsets = new short[5];
			bytes = new byte[Math.max(256, len)];
			end = 0;
		} else {
			end = offsets[numCols];

			// increase offset array, if necessary
			if (numCols + 1 >= offsets.length) {
				short[] tmp = new short[offsets.length * 2];
				System.arraycopy(offsets, 0, tmp, 0, numCols + 1);
				offsets = tmp;
			}

			// increase byte buffer, if necessary
			if (bytes.length < end + len) {
				byte[] tmp = new byte[end + len];
				System.arraycopy(bytes, 0, tmp, 0, end);
				bytes = tmp;
			}
		}

		System.arraycopy(other.bytes, other.offsets[column], bytes, end, len);
		numCols++;
		offsets[numCols] = (short) (end + len);
	}
	
	public void setContents(byte[] bytes, int offset, int len, char delimiter)
	{
		// make space
		if (this.bytes == null || this.bytes.length < len) {
			this.bytes = new byte[len];
		}
		
		// copy the binary data
		System.arraycopy(bytes, offset, this.bytes, 0, len);
		
		int readPos = offset;
		
		// allocate the offsets array
		if (this.offsets == null) {
			this.offsets = new short[4];
		}

		int col = 1; // the column we are in

		int startPos = readPos;

		while (readPos < offset + len) {
			if (bytes[readPos++] == delimiter) {
				if (offsets.length <= col) {
					short newOffsets[] = new short[this.offsets.length * 2];
					System.arraycopy(this.offsets, 0, newOffsets, 0, this.offsets.length);
					this.offsets = newOffsets;
				}
				this.offsets[col++] = (short) (readPos - startPos);
			}
		}
		
		this.numCols = col - 1;
	}


	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	@Override
	public void read(DataInput in) throws IOException {
		// read the bytes
		int numBytes = in.readInt();
		if (numBytes > 0) {
			bytes = new byte[numBytes];
			in.readFully(bytes);

			// read the offsets
			numCols = in.readInt() + 1;
			offsets = new short[numCols + 1];
			for (int i = 1; i < numCols; i++) {
				offsets[i] = in.readShort();
			}
			// set last offset
			offsets[numCols] = (short) numBytes;
		} else {
			numCols = 0;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// write the bytes
		int numBytes = (numCols > 0 ? offsets[numCols] : 0);
		out.writeInt(numBytes);
		if (numBytes > 0) {
			out.write(bytes, 0, numBytes);

			// write the offsets
			// exclude first and last
			out.writeInt(numCols - 1);
			for (int i = 1; i < numCols; i++) {
				out.writeShort(offsets[i]);
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder();

		for (int i = 0; i < numCols; i++) {
			for (int k = 0; k < getColumnLength(i); k++) {
				bld.append((char) (bytes[offsets[i] + k] & 0xff));
			}
			bld.append('|');
		}

		return bld.toString();
	}

}
