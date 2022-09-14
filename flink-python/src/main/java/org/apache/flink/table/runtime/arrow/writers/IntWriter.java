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

import org.apache.arrow.vector.IntVector;

/** {@link ArrowFieldWriter} for Int. */
@Internal
public abstract class IntWriter<T> extends ArrowFieldWriter<T> {

    public static IntWriter<RowData> forRow(IntVector intVector) {
        return new IntWriterForRow(intVector);
    }

    public static IntWriter<ArrayData> forArray(IntVector intVector) {
        return new IntWriterForArray(intVector);
    }

    // ------------------------------------------------------------------------------------------

    private IntWriter(IntVector intVector) {
        super(intVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract int readInt(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((IntVector) getValueVector()).setNull(getCount());
        } else {
            ((IntVector) getValueVector()).setSafe(getCount(), readInt(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link IntWriter} for {@link RowData} input. */
    public static final class IntWriterForRow extends IntWriter<RowData> {

        private IntWriterForRow(IntVector intVector) {
            super(intVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readInt(RowData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }

    /** {@link IntWriter} for {@link ArrayData} input. */
    public static final class IntWriterForArray extends IntWriter<ArrayData> {

        private IntWriterForArray(IntVector intVector) {
            super(intVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readInt(ArrayData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }
}
