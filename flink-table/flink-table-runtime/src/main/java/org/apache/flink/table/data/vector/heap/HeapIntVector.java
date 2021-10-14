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

import org.apache.flink.table.data.vector.writable.WritableIntVector;

import java.util.Arrays;

/** This class represents a nullable int column vector. */
public class HeapIntVector extends AbstractHeapVector implements WritableIntVector {

    private static final long serialVersionUID = -2749499358889718254L;

    public int[] vector;

    /**
     * Don't use this except for testing purposes.
     *
     * @param len the number of rows
     */
    public HeapIntVector(int len) {
        super(len);
        vector = new int[len];
    }

    @Override
    public int getInt(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return dictionary.decodeToInt(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setInt(int i, int value) {
        vector[i] = value;
    }

    @Override
    public void setIntsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (rowId + count > vector.length || srcIndex + count * 4L > src.length) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Index out of bounds, row id is %s, count is %s, binary src index is %s, binary"
                                    + " length is %s, int array src index is %s, int array length is %s.",
                            rowId, count, srcIndex, src.length, rowId, vector.length));
        }
        if (LITTLE_ENDIAN) {
            UNSAFE.copyMemory(
                    src,
                    BYTE_ARRAY_OFFSET + srcIndex,
                    vector,
                    INT_ARRAY_OFFSET + rowId * 4L,
                    count * 4L);
        } else {
            long srcOffset = srcIndex + BYTE_ARRAY_OFFSET;
            for (int i = 0; i < count; ++i, srcOffset += 4) {
                vector[i + rowId] = Integer.reverseBytes(UNSAFE.getInt(src, srcOffset));
            }
        }
    }

    @Override
    public void setInts(int rowId, int count, int value) {
        for (int i = 0; i < count; ++i) {
            vector[i + rowId] = value;
        }
    }

    @Override
    public void setInts(int rowId, int count, int[] src, int srcIndex) {
        System.arraycopy(src, srcIndex, vector, rowId, count);
    }

    @Override
    public void fill(int value) {
        Arrays.fill(vector, value);
    }
}
