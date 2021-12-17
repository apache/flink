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

import java.io.IOException;

/**
 * Boxed serializable and comparable single precision floating point type, representing the
 * primitive type {@code float}.
 */
@Public
public class FloatValue
        implements Comparable<FloatValue>,
                ResettableValue<FloatValue>,
                CopyableValue<FloatValue>,
                Key<FloatValue> {
    private static final long serialVersionUID = 1L;

    private float value;

    /** Initializes the encapsulated float with 0.0. */
    public FloatValue() {
        this.value = 0;
    }

    /**
     * Initializes the encapsulated float with the provided value.
     *
     * @param value Initial value of the encapsulated float.
     */
    public FloatValue(float value) {
        this.value = value;
    }

    /**
     * Returns the value of the encapsulated primitive float.
     *
     * @return the value of the encapsulated primitive float.
     */
    public float getValue() {
        return this.value;
    }

    /**
     * Sets the value of the encapsulated primitive float.
     *
     * @param value the new value of the encapsulated primitive float.
     */
    public void setValue(float value) {
        this.value = value;
    }

    @Override
    public void setValue(FloatValue value) {
        this.value = value.value;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void read(DataInputView in) throws IOException {
        this.value = in.readFloat();
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeFloat(this.value);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    @Override
    public int compareTo(FloatValue o) {
        final double other = o.value;
        return this.value < other ? -1 : this.value > other ? 1 : 0;
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(this.value);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof FloatValue) {
            final FloatValue other = (FloatValue) obj;
            return Float.floatToIntBits(this.value) == Float.floatToIntBits(other.value);
        }
        return false;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int getBinaryLength() {
        return 4;
    }

    @Override
    public void copyTo(FloatValue target) {
        target.value = this.value;
    }

    @Override
    public FloatValue copy() {
        return new FloatValue(this.value);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, 4);
    }
}
