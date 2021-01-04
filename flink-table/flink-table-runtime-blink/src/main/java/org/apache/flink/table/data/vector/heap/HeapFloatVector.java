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

import org.apache.flink.table.data.vector.writable.WritableFloatVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * This class represents a nullable double precision floating point column vector. This class will
 * be used for operations on all floating point float types.
 */
public class HeapFloatVector extends AbstractHeapVector implements WritableFloatVector {

    private static final long serialVersionUID = 8928878923550041110L;

    public float[] vector;

    /**
     * Don't use this except for testing purposes.
     *
     * @param len the number of rows
     */
    public HeapFloatVector(int len) {
        super(len);
        vector = new float[len];
    }

    @Override
    public float getFloat(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return dictionary.decodeToFloat(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setFloat(int i, float value) {
        vector[i] = value;
    }

    @Override
    public void setFloatsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (rowId + count > vector.length || srcIndex + count * 4L > src.length) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Index out of bounds, row id is %s, count is %s, binary src index is %s, binary"
                                    + " length is %s, float array src index is %s, float array length is %s.",
                            rowId, count, srcIndex, src.length, rowId, vector.length));
        }
        if (LITTLE_ENDIAN) {
            UNSAFE.copyMemory(
                    src,
                    BYTE_ARRAY_OFFSET + srcIndex,
                    vector,
                    FLOAT_ARRAY_OFFSET + rowId * 4L,
                    count * 4L);
        } else {
            ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < count; ++i) {
                vector[i + rowId] = bb.getFloat(srcIndex + (4 * i));
            }
        }
    }

    @Override
    public void fill(float value) {
        Arrays.fill(vector, value);
    }
}
