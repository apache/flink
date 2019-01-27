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
 * This class supports string and binary data -- i.e. each field is
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
public class BinaryColumnVector extends ColumnVector {
	private static final long serialVersionUID = -8529155738773478597L;

	public byte[][] vector;
	private int nextFree;    // next free position in buffer

	/**
	 * Don't call this constructor except for testing purposes.
	 *
	 * @param size number of elements in the column vector
	 */
	public BinaryColumnVector(int size) {
		super(size);
		vector = new byte[size][];
	}

	@Override
	public Object get(int index) {
		return vector[index];
	}

	/**
	 * Additional reset work for BytesColumnVector (releasing scratch bytes for by value strings).
	 */
	@Override
	public void reset() {
		super.reset();
	}

	/**
	 * Set a field by reference.
	 *
	 * @param elementNum index within column vector to set
	 * @param sourceBuf  container of source data
	 * @param start      start byte position within source
	 * @param length     length of source byte sequence
	 */
	public void setRef(int elementNum, byte[] sourceBuf, int start, int length) {
		vector[elementNum] = sourceBuf;
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
		vector[elementNum] = sourceBuf;
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
				((BinaryColumnVector) output).setVal(i, vector[i]);
			}
		} else {
			for (int i = 0; i < size; i++) {
				((BinaryColumnVector) output).setVal(i, vector[i]);
			}
		}

		// Copy nulls over if needed
		super.copySelected(selectedInUse, sel, size, output);
	}

	@Override
	public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
		if (inputVector.noNulls || !inputVector.isNull[inputElementNum]) {
			isNull[outElementNum] = false;
			BinaryColumnVector in = (BinaryColumnVector) inputVector;
			setVal(outElementNum, in.vector[inputElementNum]);
		} else {
			isNull[outElementNum] = true;
			noNulls = false;
		}
	}

	@Override
	public void init() {
	}

	@Override
	public void shallowCopyTo(ColumnVector otherCv) {
		BinaryColumnVector other = (BinaryColumnVector) otherCv;
		super.shallowCopyTo(other);
		other.nextFree = nextFree;
		other.vector = vector;
	}
}
