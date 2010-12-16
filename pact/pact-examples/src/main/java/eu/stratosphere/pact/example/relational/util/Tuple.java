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

package eu.stratosphere.pact.example.relational.util;

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
	 * Creates a new tuple with the following properties:
	 * - The key is initially 0 and must be extracted via <tt>extractKey</tt> - The bytes have one char delimiter
	 * between the columns.
	 * - The offsets array is one bigger than the number of columns and contains
	 * the length of the bytes as its last index.
	 * 
	 * @param bytes
	 *        The bytes array.
	 * @param offsets
	 *        The offsets of the columns in the byte array.
	 */
	public Tuple(byte[] bytes, short[] offsets, int cols) {
		this.bytes = bytes;
		this.offsets = offsets;
		this.numCols = cols;
	}

	// ------------------------------------------------------------------------
	// Accessors
	// ------------------------------------------------------------------------

	public int getNumberOfColumns() {
		return numCols;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public int getColumnLength(int colNumber) {
		return offsets[colNumber + 1] - offsets[colNumber] - 1;
	}

	// ------------------------------------------------------------------------
	// Modification
	// ------------------------------------------------------------------------

	public void concatenate(Tuple other) {
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

	public void project(int bitmap) {
		int[] lengths = new int[numCols];
		int lenCount = 0;

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

	public int compareStringAttribute(Tuple other, int thisColumn, int otherColumn) {
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

	public int compareIntAttribute(Tuple other, int thisColumn, int otherColumn) {
		int len = getColumnLength(thisColumn);
		int otherLen = other.getColumnLength(otherColumn);

		if (len < otherLen) {
			return -1;
		} else if (len > otherLen) {
			return 1;
		} else {
			int myStartPos = offsets[thisColumn];
			int compStartPos = other.offsets[otherColumn];

			for (int i = 0; i < len; i++) {
				if (bytes[myStartPos + i] < other.bytes[compStartPos + i]) {
					return -1;
				} else if (bytes[myStartPos + i] > other.bytes[compStartPos + i]) {
					return 1;
				}
			}
			return 0;
		}
	}

	public String getStringValueAt(int column) {
		int off = offsets[column];
		int len = getColumnLength(column);

		char[] chars = new char[len];
		for (int i = 0; i < len; i++) {
			chars[i] = (char) (bytes[off + i] & 0xff);
		}

		return new String(chars);
	}

	public long getLongValueAt(int column) {
		int off = offsets[column];
		int len = getColumnLength(column);

		long value = 0;
		for (int i = off; i < off + len; i++) {
			value *= 10;
			value += (bytes[i] - 48);
		}

		return value;
	}

	public byte[] getByteArrayValueAt(int column) {
		int len = getColumnLength(column);
		byte[] buffer = new byte[len];
		System.arraycopy(bytes, offsets[column], buffer, 0, len);
		return buffer;
	}

	public void reserveSpace(int minCapacity) {
		if (bytes.length < minCapacity) {
			byte[] tmp = new byte[minCapacity];
			System.arraycopy(bytes, 0, tmp, 0, offsets[numCols]);
			bytes = tmp;
		}
	}

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

	public boolean checkDelimiters() {
		for (int i = 0; i < numCols; i++) {
			if (bytes[offsets[i] + getColumnLength(i)] != '|') {
				return false;
			}
		}

		return true;
	}

}
