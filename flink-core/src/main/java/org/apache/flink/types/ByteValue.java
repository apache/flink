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

package org.apache.flink.types;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

/**
 * Boxed serializable and comparable byte type, representing the primitive type {@code byte} (signed
 * 8 bit integer).
 */
@Public
public class ByteValue
        implements NormalizableKey<ByteValue>,
                ResettableValue<ByteValue>,
                CopyableValue<ByteValue> {
    private static final long serialVersionUID = 1L;

    private byte value;

    /** Initializes the encapsulated byte with 0. */
    public ByteValue() {
        this.value = 0;
    }

    /**
     * Initializes the encapsulated byte with the provided value.
     *
     * @param value Initial value of the encapsulated byte.
     */
    public ByteValue(byte value) {
        this.value = value;
    }

    /**
     * Returns the value of the encapsulated byte.
     *
     * @return the value of the encapsulated byte.
     */
    public byte getValue() {
        return this.value;
    }

    /**
     * Sets the encapsulated byte to the specified value.
     *
     * @param value the new value of the encapsulated byte.
     */
    public void setValue(byte value) {
        this.value = value;
    }

    @Override
    public void setValue(ByteValue value) {
        this.value = value.value;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void read(DataInputView in) throws IOException {
        this.value = in.readByte();
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeByte(this.value);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    @Override
    public int compareTo(ByteValue o) {
        final byte other = o.value;
        return this.value < other ? -1 : this.value > other ? 1 : 0;
    }

    @Override
    public int hashCode() {
        return this.value;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ByteValue) {
            return ((ByteValue) obj).value == this.value;
        }
        return false;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int getMaxNormalizedKeyLen() {
        return 1;
    }

    @Override
    public void copyNormalizedKey(MemorySegment target, int offset, int len) {
        if (len == 1) {
            // default case, full normalized key. need to explicitly convert to int to
            // avoid false results due to implicit type conversion to int when subtracting
            // the min byte value
            int highByte = this.value & 0xff;
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
        } else if (len > 1) {
            int highByte = this.value & 0xff;
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
            for (int i = 1; i < len; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int getBinaryLength() {
        return 1;
    }

    @Override
    public void copyTo(ByteValue target) {
        target.value = this.value;
    }

    @Override
    public ByteValue copy() {
        return new ByteValue(this.value);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, 1);
    }
}
