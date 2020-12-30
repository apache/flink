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

package org.apache.flink.table.runtime.arrow.writers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.vector.Float4Vector;

/** {@link ArrowFieldWriter} for Float. */
@Internal
public abstract class FloatWriter<T> extends ArrowFieldWriter<T> {

    public static FloatWriter<RowData> forRow(Float4Vector floatVector) {
        return new FloatWriterForRow(floatVector);
    }

    public static FloatWriter<ArrayData> forArray(Float4Vector floatVector) {
        return new FloatWriterForArray(floatVector);
    }

    // ------------------------------------------------------------------------------------------

    private FloatWriter(Float4Vector floatVector) {
        super(floatVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract float readFloat(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((Float4Vector) getValueVector()).setNull(getCount());
        } else {
            ((Float4Vector) getValueVector()).setSafe(getCount(), readFloat(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link FloatWriter} for {@link RowData} input. */
    public static final class FloatWriterForRow extends FloatWriter<RowData> {

        private FloatWriterForRow(Float4Vector floatVector) {
            super(floatVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        float readFloat(RowData in, int ordinal) {
            return in.getFloat(ordinal);
        }
    }

    /** {@link FloatWriter} for {@link ArrayData} input. */
    public static final class FloatWriterForArray extends FloatWriter<ArrayData> {

        private FloatWriterForArray(Float4Vector floatVector) {
            super(floatVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        float readFloat(ArrayData in, int ordinal) {
            return in.getFloat(ordinal);
        }
    }
}
