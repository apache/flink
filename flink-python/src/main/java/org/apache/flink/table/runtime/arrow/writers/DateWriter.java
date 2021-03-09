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

import org.apache.arrow.vector.DateDayVector;

/** {@link ArrowFieldWriter} for Date. */
@Internal
public abstract class DateWriter<T> extends ArrowFieldWriter<T> {

    public static DateWriter<RowData> forRow(DateDayVector dateDayVector) {
        return new DateWriterForRow(dateDayVector);
    }

    public static DateWriter<ArrayData> forArray(DateDayVector dateDayVector) {
        return new DateWriterForArray(dateDayVector);
    }

    // ------------------------------------------------------------------------------------------

    private DateWriter(DateDayVector dateDayVector) {
        super(dateDayVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract int readDate(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((DateDayVector) getValueVector()).setNull(getCount());
        } else {
            ((DateDayVector) getValueVector()).setSafe(getCount(), readDate(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link DateWriter} for {@link RowData} input. */
    public static final class DateWriterForRow extends DateWriter<RowData> {

        private DateWriterForRow(DateDayVector dateDayVector) {
            super(dateDayVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readDate(RowData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }

    /** {@link DateWriter} for {@link ArrayData} input. */
    public static final class DateWriterForArray extends DateWriter<ArrayData> {

        private DateWriterForArray(DateDayVector dateDayVector) {
            super(dateDayVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readDate(ArrayData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }
}
