/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/** An array of {@link ByteValue}. */
public class ByteValueArray implements ValueArray<ByteValue> {

    protected static final int ELEMENT_LENGTH_IN_BYTES = 1;

    protected static final int DEFAULT_CAPACITY_IN_BYTES = 1024;

    // see note in ArrayList, HashTable, ...
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private boolean isBounded;

    private byte[] data;

    // the number of elements currently stored
    private int position;

    // location of the bookmark used by mark() and reset()
    private transient int mark;

    // hasher used to generate the normalized key
    private MurmurHash hash = new MurmurHash(0x18d7b696);

    // hash result stored as normalized key
    private IntValue hashValue = new IntValue();

    /** Initializes an expandable array with default capacity. */
    public ByteValueArray() {
        isBounded = false;
        initialize(DEFAULT_CAPACITY_IN_BYTES);
    }

    /**
     * Initializes a fixed-size array with the provided number of bytes.
     *
     * @param bytes number of bytes of the encapsulated array
     */
    public ByteValueArray(int bytes) {
        isBounded = true;
        initialize(bytes);
    }

    /**
     * Initializes the array with the provided number of bytes.
     *
     * @param bytes initial size of the encapsulated array in bytes
     */
    private void initialize(int bytes) {
        int capacity = bytes / ELEMENT_LENGTH_IN_BYTES;

        Preconditions.checkArgument(capacity > 0, "Requested array with zero capacity");
        Preconditions.checkArgument(
                capacity <= MAX_ARRAY_SIZE,
                "Requested capacity exceeds limit of " + MAX_ARRAY_SIZE);

        data = new byte[capacity];
    }

    // --------------------------------------------------------------------------------------------

    /**
     * If the size of the array is insufficient to hold the given capacity then copy the array into
     * a new, larger array.
     *
     * @param minCapacity minimum required number of elements
     */
    private void ensureCapacity(int minCapacity) {
        long currentCapacity = data.length;

        if (minCapacity <= currentCapacity) {
            return;
        }

        // increase capacity by at least ~50%
        long expandedCapacity = Math.max(minCapacity, currentCapacity + (currentCapacity >> 1));
        int newCapacity = (int) Math.min(MAX_ARRAY_SIZE, expandedCapacity);

        if (newCapacity < minCapacity) {
            // throw exception as unbounded arrays are not expected to fill
            throw new RuntimeException(
                    "Requested array size " + minCapacity + " exceeds limit of " + MAX_ARRAY_SIZE);
        }

        data = Arrays.copyOf(data, newCapacity);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int idx = 0; idx < this.position; idx++) {
            sb.append(data[idx]);
            if (idx < position - 1) {
                sb.append(",");
            }
        }
        sb.append("]");

        return sb.toString();
    }

    // --------------------------------------------------------------------------------------------
    // Iterable
    // --------------------------------------------------------------------------------------------

    private final ReadIterator iterator = new ReadIterator();

    @Override
    public Iterator<ByteValue> iterator() {
        iterator.reset();
        return iterator;
    }

    private class ReadIterator implements Iterator<ByteValue> {
        private ByteValue value = new ByteValue();

        private int pos;

        @Override
        public boolean hasNext() {
            return pos < position;
        }

        @Override
        public ByteValue next() {
            value.setValue(data[pos++]);
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        public void reset() {
            pos = 0;
        }
    }

    // --------------------------------------------------------------------------------------------
    // IOReadableWritable
    // --------------------------------------------------------------------------------------------

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(position);

        for (int i = 0; i < position; i++) {
            out.writeByte(data[i]);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        position = in.readInt();
        mark = 0;

        ensureCapacity(position);

        for (int i = 0; i < position; i++) {
            data[i] = in.readByte();
        }
    }

    // --------------------------------------------------------------------------------------------
    // NormalizableKey
    // --------------------------------------------------------------------------------------------

    @Override
    public int getMaxNormalizedKeyLen() {
        return hashValue.getMaxNormalizedKeyLen();
    }

    @Override
    public void copyNormalizedKey(MemorySegment target, int offset, int len) {
        hash.reset();

        hash.hash(position);
        for (int i = 0; i < position; i++) {
            hash.hash(data[i]);
        }

        hashValue.setValue(hash.hash());
        hashValue.copyNormalizedKey(target, offset, len);
    }

    // --------------------------------------------------------------------------------------------
    // Comparable
    // --------------------------------------------------------------------------------------------

    @Override
    public int compareTo(ValueArray<ByteValue> o) {
        ByteValueArray other = (ByteValueArray) o;

        int min = Math.min(position, other.position);
        for (int i = 0; i < min; i++) {
            int cmp = Byte.compare(data[i], other.data[i]);

            if (cmp != 0) {
                return cmp;
            }
        }

        return Integer.compare(position, other.position);
    }

    // --------------------------------------------------------------------------------------------
    // Key
    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int hash = 0;

        for (int i = 0; i < position; i++) {
            hash = 31 * hash + data[i];
        }

        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ByteValueArray) {
            ByteValueArray other = (ByteValueArray) obj;

            if (position != other.position) {
                return false;
            }

            for (int i = 0; i < position; i++) {
                if (data[i] != other.data[i]) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    // --------------------------------------------------------------------------------------------
    // ResettableValue
    // --------------------------------------------------------------------------------------------

    @Override
    public void setValue(ValueArray<ByteValue> value) {
        value.copyTo(this);
    }

    // --------------------------------------------------------------------------------------------
    // CopyableValue
    // --------------------------------------------------------------------------------------------

    @Override
    public int getBinaryLength() {
        return -1;
    }

    @Override
    public void copyTo(ValueArray<ByteValue> target) {
        ByteValueArray other = (ByteValueArray) target;

        other.position = position;
        other.mark = mark;

        other.ensureCapacity(position);
        System.arraycopy(data, 0, other.data, 0, position);
    }

    @Override
    public ValueArray<ByteValue> copy() {
        ValueArray<ByteValue> copy = new ByteValueArray();

        this.copyTo(copy);

        return copy;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        copyInternal(source, target);
    }

    protected static void copyInternal(DataInputView source, DataOutputView target)
            throws IOException {
        int count = source.readInt();
        target.writeInt(count);

        int bytes = ELEMENT_LENGTH_IN_BYTES * count;
        target.write(source, bytes);
    }

    // --------------------------------------------------------------------------------------------
    // ValueArray
    // --------------------------------------------------------------------------------------------

    @Override
    public int size() {
        return position;
    }

    @Override
    public boolean isFull() {
        if (isBounded) {
            return position == data.length;
        } else {
            return position == MAX_ARRAY_SIZE;
        }
    }

    @Override
    public boolean add(ByteValue value) {
        int newPosition = position + 1;

        if (newPosition > data.length) {
            if (isBounded) {
                return false;
            } else {
                ensureCapacity(newPosition);
            }
        }

        data[position] = value.getValue();
        position = newPosition;

        return true;
    }

    @Override
    public boolean addAll(ValueArray<ByteValue> other) {
        ByteValueArray source = (ByteValueArray) other;

        int sourceSize = source.position;
        int newPosition = position + sourceSize;

        if (newPosition > data.length) {
            if (isBounded) {
                return false;
            } else {
                ensureCapacity(newPosition);
            }
        }

        System.arraycopy(source.data, 0, data, position, sourceSize);
        position = newPosition;

        return true;
    }

    @Override
    public void clear() {
        position = 0;
    }

    @Override
    public void mark() {
        mark = position;
    }

    @Override
    public void reset() {
        position = mark;
    }
}
