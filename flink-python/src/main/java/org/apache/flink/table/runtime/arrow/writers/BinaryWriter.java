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

import org.apache.arrow.vector.FixedSizeBinaryVector;

/** {@link ArrowFieldWriter} for Binary. */
@Internal
public abstract class BinaryWriter<T> extends ArrowFieldWriter<T> {

    public static BinaryWriter<RowData> forRow(FixedSizeBinaryVector fixedSizeBinaryVector) {
        return new BinaryWriterForRow(fixedSizeBinaryVector);
    }

    public static BinaryWriter<ArrayData> forArray(FixedSizeBinaryVector fixedSizeBinaryVector) {
        return new BinaryWriterForArray(fixedSizeBinaryVector);
    }

    // ------------------------------------------------------------------------------------------

    private BinaryWriter(FixedSizeBinaryVector fixedSizeBinaryVector) {
        super(fixedSizeBinaryVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract byte[] readBinary(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((FixedSizeBinaryVector) getValueVector()).setNull(getCount());
        } else {
            ((FixedSizeBinaryVector) getValueVector()).setSafe(getCount(), readBinary(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link BinaryWriter} for {@link RowData} input. */
    public static final class BinaryWriterForRow extends BinaryWriter<RowData> {

        private BinaryWriterForRow(FixedSizeBinaryVector fixedSizeBinaryVector) {
            super(fixedSizeBinaryVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        byte[] readBinary(RowData in, int ordinal) {
            return in.getBinary(ordinal);
        }
    }

    /** {@link BinaryWriter} for {@link ArrayData} input. */
    public static final class BinaryWriterForArray extends BinaryWriter<ArrayData> {

        private BinaryWriterForArray(FixedSizeBinaryVector fixedSizeBinaryVector) {
            super(fixedSizeBinaryVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        byte[] readBinary(ArrayData in, int ordinal) {
            return in.getBinary(ordinal);
        }
    }
}
