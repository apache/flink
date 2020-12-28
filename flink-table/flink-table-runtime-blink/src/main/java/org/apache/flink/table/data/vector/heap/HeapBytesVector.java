/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.vector.heap;

import org.apache.flink.table.data.vector.writable.WritableBytesVector;

import java.util.Arrays;

/**
 * This class supports string and binary data by value reference -- i.e. each field is explicitly
 * present, as opposed to provided by a dictionary reference. In some cases, all the values will be
 * in the same byte array to begin with, but this need not be the case. If each value is in a
 * separate byte array to start with, or not all of the values are in the same original byte array,
 * you can still assign data by reference into this column vector. This gives flexibility to use
 * this in multiple situations.
 *
 * <p>When setting data by reference, the caller is responsible for allocating the byte arrays used
 * to hold the data. You can also set data by value, as long as you call the initBuffer() method
 * first. You can mix "by value" and "by reference" in the same column vector, though that use is
 * probably not typical.
 */
public class HeapBytesVector extends AbstractHeapVector implements WritableBytesVector {

    private static final long serialVersionUID = -8529155738773478597L;

    /** start offset of each field. */
    public int[] start;

    /** The length of each field. */
    public int[] length;

    /** buffer to use when actually copying in data. */
    public byte[] buffer;

    /** Hang onto a byte array for holding smaller byte values. */
    private int elementsAppended = 0;

    private int capacity;

    /**
     * Don't call this constructor except for testing purposes.
     *
     * @param size number of elements in the column vector
     */
    public HeapBytesVector(int size) {
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
    public void appendBytes(int elementNum, byte[] sourceBuf, int start, int length) {
        reserve(elementsAppended + length);
        System.arraycopy(sourceBuf, start, buffer, elementsAppended, length);
        this.start[elementNum] = elementsAppended;
        this.length[elementNum] = length;
        elementsAppended += length;
    }

    @Override
    public void fill(byte[] value) {
        reserve(start.length * value.length);
        for (int i = 0; i < start.length; i++) {
            System.arraycopy(value, 0, buffer, i * value.length, value.length);
        }
        for (int i = 0; i < start.length; i++) {
            this.start[i] = i * value.length;
        }
        Arrays.fill(this.length, value.length);
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
                throw new UnsupportedOperationException(
                        requiredCapacity + " cannot be satisfied.", outOfMemoryError);
            }
        }
    }

    @Override
    public Bytes getBytes(int i) {
        if (dictionary == null) {
            return new Bytes(buffer, start[i], length[i]);
        } else {
            byte[] bytes = dictionary.decodeToBinary(dictionaryIds.vector[i]);
            return new Bytes(bytes, 0, bytes.length);
        }
    }
}
