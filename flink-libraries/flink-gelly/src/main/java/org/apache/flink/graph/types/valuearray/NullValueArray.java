/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import java.io.IOException;
import java.util.Iterator;

/** An array of {@link NullValue}. */
public class NullValueArray implements ValueArray<NullValue> {

    // the number of elements currently stored
    private int position;

    // location of the bookmark used by mark() and reset()
    private transient int mark;

    // hash result stored as normalized key
    private IntValue hashValue = new IntValue();

    /** Initializes an expandable array with default capacity. */
    public NullValueArray() {}

    /**
     * Initializes a fixed-size array with the provided number of bytes.
     *
     * @param bytes number of bytes of the encapsulated array
     */
    public NullValueArray(int bytes) {
        this();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int idx = 0; idx < this.position; idx++) {
            sb.append("∅");
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
    public Iterator<NullValue> iterator() {
        iterator.reset();
        return iterator;
    }

    private class ReadIterator implements Iterator<NullValue> {
        private int pos;

        @Override
        public boolean hasNext() {
            return pos < position;
        }

        @Override
        public NullValue next() {
            pos++;
            return NullValue.getInstance();
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
    }

    @Override
    public void read(DataInputView in) throws IOException {
        position = in.readInt();
        mark = 0;
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
        hashValue.setValue(position);
        hashValue.copyNormalizedKey(target, offset, len);
    }

    // --------------------------------------------------------------------------------------------
    // Comparable
    // --------------------------------------------------------------------------------------------

    @Override
    public int compareTo(ValueArray<NullValue> o) {
        NullValueArray other = (NullValueArray) o;

        return Integer.compare(position, other.position);
    }

    // --------------------------------------------------------------------------------------------
    // Key
    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return position;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NullValueArray) {
            NullValueArray other = (NullValueArray) obj;

            return position == other.position;
        }

        return false;
    }

    // --------------------------------------------------------------------------------------------
    // ResettableValue
    // --------------------------------------------------------------------------------------------

    @Override
    public void setValue(ValueArray<NullValue> value) {
        value.copyTo(this);
    }

    // --------------------------------------------------------------------------------------------
    // CopyableValue
    // --------------------------------------------------------------------------------------------

    @Override
    public int getBinaryLength() {
        return hashValue.getBinaryLength();
    }

    @Override
    public void copyTo(ValueArray<NullValue> target) {
        NullValueArray other = (NullValueArray) target;

        other.position = position;
    }

    @Override
    public ValueArray<NullValue> copy() {
        ValueArray<NullValue> copy = new NullValueArray();

        this.copyTo(copy);

        return copy;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, getBinaryLength());
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
        return position == Integer.MAX_VALUE;
    }

    @Override
    public boolean add(NullValue value) {
        if (position == Integer.MAX_VALUE) {
            return false;
        }

        position++;

        return true;
    }

    @Override
    public boolean addAll(ValueArray<NullValue> other) {
        NullValueArray source = (NullValueArray) other;

        long newPosition = position + (long) source.position;

        if (newPosition > Integer.MAX_VALUE) {
            return false;
        }

        position = (int) newPosition;

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
