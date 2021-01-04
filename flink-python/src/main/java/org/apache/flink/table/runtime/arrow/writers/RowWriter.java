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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.vector.complex.StructVector;

/** {@link ArrowFieldWriter} for Row. */
@Internal
public abstract class RowWriter<T> extends ArrowFieldWriter<T> {

    public static RowWriter<RowData> forRow(
            StructVector structVector, ArrowFieldWriter<RowData>[] fieldsWriters) {
        return new RowWriterForRow(structVector, fieldsWriters);
    }

    public static RowWriter<ArrayData> forArray(
            StructVector structVector, ArrowFieldWriter<RowData>[] fieldsWriters) {
        return new RowWriterForArray(structVector, fieldsWriters);
    }

    // ------------------------------------------------------------------------------------------

    protected final ArrowFieldWriter<RowData>[] fieldsWriters;
    private final GenericRowData nullRow;

    private RowWriter(StructVector structVector, ArrowFieldWriter<RowData>[] fieldsWriters) {
        super(structVector);
        this.fieldsWriters = fieldsWriters;
        this.nullRow = new GenericRowData(fieldsWriters.length);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract RowData readRow(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        RowData row;
        if (isNullAt(in, ordinal)) {
            row = nullRow;
            ((StructVector) getValueVector()).setNull(getCount());
        } else {
            row = readRow(in, ordinal);
            ((StructVector) getValueVector()).setIndexDefined(getCount());
        }
        for (int i = 0; i < fieldsWriters.length; i++) {
            fieldsWriters[i].write(row, i);
        }
    }

    @Override
    public void finish() {
        super.finish();
        for (ArrowFieldWriter<?> fieldsWriter : fieldsWriters) {
            fieldsWriter.finish();
        }
    }

    @Override
    public void reset() {
        super.reset();
        for (ArrowFieldWriter<?> fieldsWriter : fieldsWriters) {
            fieldsWriter.reset();
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link RowWriter} for {@link RowData} input. */
    public static final class RowWriterForRow extends RowWriter<RowData> {

        private RowWriterForRow(
                StructVector structVector, ArrowFieldWriter<RowData>[] fieldsWriters) {
            super(structVector, fieldsWriters);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        RowData readRow(RowData in, int ordinal) {
            return in.getRow(ordinal, fieldsWriters.length);
        }
    }

    /** {@link RowWriter} for {@link ArrayData} input. */
    public static final class RowWriterForArray extends RowWriter<ArrayData> {

        private RowWriterForArray(
                StructVector structVector, ArrowFieldWriter<RowData>[] fieldsWriters) {
            super(structVector, fieldsWriters);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        RowData readRow(ArrayData in, int ordinal) {
            return in.getRow(ordinal, fieldsWriters.length);
        }
    }
}
