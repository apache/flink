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

import org.apache.arrow.vector.BigIntVector;

/** {@link ArrowFieldWriter} for BigInt. */
@Internal
public abstract class BigIntWriter<T> extends ArrowFieldWriter<T> {

    public static BigIntWriter<RowData> forRow(BigIntVector bigIntVector) {
        return new BigIntWriterForRow(bigIntVector);
    }

    public static BigIntWriter<ArrayData> forArray(BigIntVector bigIntVector) {
        return new BigIntWriterForArray(bigIntVector);
    }

    // ------------------------------------------------------------------------------------------

    private BigIntWriter(BigIntVector bigIntVector) {
        super(bigIntVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract long readLong(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((BigIntVector) getValueVector()).setNull(getCount());
        } else {
            ((BigIntVector) getValueVector()).setSafe(getCount(), readLong(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link BigIntWriter} for {@link RowData} input. */
    public static final class BigIntWriterForRow extends BigIntWriter<RowData> {

        private BigIntWriterForRow(BigIntVector bigIntVector) {
            super(bigIntVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        long readLong(RowData in, int ordinal) {
            return in.getLong(ordinal);
        }
    }

    /** {@link BigIntWriter} for {@link ArrayData} input. */
    public static final class BigIntWriterForArray extends BigIntWriter<ArrayData> {

        private BigIntWriterForArray(BigIntVector bigIntVector) {
            super(bigIntVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        long readLong(ArrayData in, int ordinal) {
            return in.getLong(ordinal);
        }
    }
}
