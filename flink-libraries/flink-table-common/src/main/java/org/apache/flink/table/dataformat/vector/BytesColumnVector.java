/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.vector;

/**
 * This class supports string and binary data by value reference -- i.e. each field is
 * explicitly present, as opposed to provided by a dictionary reference.
 * In some cases, all the values will be in the same byte array to begin with,
 * but this need not be the case. If each value is in a separate byte
 * array to start with, or not all of the values are in the same original
 * byte array, you can still assign data by reference into this column vector.
 * This gives flexibility to use this in multiple situations.
 *
 * <p>When setting data by reference, the caller
 * is responsible for allocating the byte arrays used to hold the data.
 * You can also set data by value, as long as you call the initBuffer() method first.
 * You can mix "by value" and "by reference" in the same column vector,
 * though that use is probably not typical.
 */
public class BytesColumnVector extends ColumnVector {
	private static final long serialVersionUID = -8529155738773478597L;

	public int[] start;          // start offset of each field
	/*
	 * The length of each field. If the value repeats for every entry, then it is stored
	 * * in vector[0] and isRepeating from the superclass is set to true.
	 */
	public int[] length;
	// A call to increaseBufferSpace() or ensureValPreallocated() will ensure that buffer[] points to
	// a byte[] with sufficient space for the specified size.
	public byte[] buffer;   // buffer to use when actually copying in data
	// Hang onto a byte array for holding smaller byte values
	private int elementsAppended = 0;
	private int capacity;

	/**
	 * Don't call this constructor except for testing purposes.
	 *
	 * @param size number of elements in the column vector
	 */
	public BytesColumnVector(int size) {
		super(size);
		capacity = size;
		buffer = new byte[capacity];
		start = new int[size];
		length = new int[size];
	}

	@Override
	public void reset() {
		super.reset();
		elementsAppended = 0;
	}

	@Override
	public Object get(int index) {
		return buffer;
	}

	/**
	 * @return amount of buffer space currently allocated
	 */
	public int bufferSize() {
		if (buffer == null) {
			return 0;
		}
		return buffer.length;
	}

	/**
	 * Set a field by actually copying in to a local buffer.
	 * If you must actually copy data in to the array, use this method.
	 * DO NOT USE this method unless it's not practical to set data by reference with setRef().
	 * Setting data by reference tends to run a lot faster than copying data in.
	 *
	 * @param elementNum index within column vector to set
	 * @param sourceBuf  container of source data
	 * @param start      start byte position within source
	 * @param length     length of source byte sequence
	 */
	public void setVal(int elementNum, byte[] sourceBuf, int start, int length) {
		reserve(elementsAppended + length);
		System.arraycopy(sourceBuf, start, buffer, elementsAppended, length);
		this.start[elementNum] = elementsAppended;
		this.length[elementNum] = length;
		elementsAppended += length;
	}

	/**
	 * Set a field by actually copying in to a local buffer.
	 * If you must actually copy data in to the array, use this method.
	 * DO NOT USE this method unless it's not practical to set data by reference with setRef().
	 * Setting data by reference tends to run a lot faster than copying data in.
	 *
	 * @param elementNum index within column vector to set
	 * @param sourceBuf  container of source data
	 */
	public void setVal(int elementNum, byte[] sourceBuf) {
		setVal(elementNum, sourceBuf, 0, sourceBuf.length);
	}


	/**
	 * Copy the current object contents into the output. Only copy selected entries,
	 * as indicated by selectedInUse and the sel array.
	 */
	@Override
	public void copySelected(boolean selectedInUse, int[] sel, int size, ColumnVector output) {

		// Output has nulls if and only if input has nulls.
		output.noNulls = noNulls;

		// Copy data values over
		if (selectedInUse) {
			for (int j = 0; j < size; j++) {
				int i = sel[j];
				((BytesColumnVector) output).setVal(i, buffer, start[i], length[i]);
			}
		} else {
			for (int i = 0; i < size; i++) {
				((BytesColumnVector) output).setVal(i, buffer, start[i], length[i]);
			}
		}

		// Copy nulls over if needed
		super.copySelected(selectedInUse, sel, size, output);
	}

	@Override
	public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
		if (inputVector.noNulls || !inputVector.isNull[inputElementNum]) {
			isNull[outElementNum] = false;
			BytesColumnVector in = (BytesColumnVector) inputVector;
			setVal(outElementNum, in.buffer, in.start[inputElementNum], in.length[inputElementNum]);
		} else {
			isNull[outElementNum] = true;
			noNulls = false;
		}
	}

	private void reserve(int requiredCapacity) {
		if (requiredCapacity > capacity) {
			int newCapacity = requiredCapacity * 2;
				try {
					byte[] newData = new byte[newCapacity];
					System.arraycopy(buffer, 0, newData, 0, elementsAppended);
					buffer = newData;
					capacity = newCapacity;
				} catch (OutOfMemoryError outOfMemoryError) {
					throw new UnsupportedOperationException(requiredCapacity + " cannot be satisfied.", outOfMemoryError);
				}
		}
	}

	@Override
	public void shallowCopyTo(ColumnVector otherCv) {
		BytesColumnVector other = (BytesColumnVector) otherCv;
		super.shallowCopyTo(other);
		other.start = start;
		other.length = length;
		other.buffer = buffer;
	}

	public String toString(int row) {
		if (noNulls || !isNull[row]) {
			return new String(buffer, start[row], length[row]);
		} else {
			return null;
		}
	}
}
