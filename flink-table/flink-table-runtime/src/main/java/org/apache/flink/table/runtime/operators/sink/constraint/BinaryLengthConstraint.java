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

package org.apache.flink.table.runtime.operators.sink.constraint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.UpdatableRowData;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.BitSet;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER;

/** Enforces length constraints on the input {@link RowData}. */
@Internal
final class BinaryLengthConstraint implements Constraint {

    private final TypeLengthEnforcementStrategy typeLengthEnforcementStrategy;
    private final int[] fieldIndices;
    private final int[] fieldLengths;
    private final String[] fieldNames;
    private final BitSet fieldCouldPad;

    BinaryLengthConstraint(
            final TypeLengthEnforcementStrategy typeLengthEnforcementStrategy,
            final int[] fieldIndices,
            final int[] fieldLengths,
            final String[] fieldNames,
            final BitSet fieldCouldPad) {
        this.typeLengthEnforcementStrategy = typeLengthEnforcementStrategy;
        this.fieldIndices = fieldIndices;
        this.fieldLengths = fieldLengths;
        this.fieldNames = fieldNames;
        this.fieldCouldPad = fieldCouldPad;
    }

    @Nullable
    @Override
    public RowData enforce(RowData rowData) {
        UpdatableRowData updatedRowData = null;

        for (int i = 0; i < fieldLengths.length; i++) {
            final int fieldIdx = fieldIndices[i];
            final int expectedLength = fieldLengths[i];
            final byte[] binaryData = rowData.getBinary(fieldIdx);
            final int actualLength = binaryData.length;
            final boolean shouldPad = fieldCouldPad.get(i);

            // Trimming takes places because of the shorter length used in `Arrays.copyOf` and
            // padding because of the longer length, as implicitly the trailing bytes are 0.
            if ((actualLength > expectedLength) || (shouldPad && actualLength < expectedLength)) {
                switch (typeLengthEnforcementStrategy) {
                    case TRIM_PAD:
                        updatedRowData =
                                trimOrPad(
                                        rowData,
                                        updatedRowData,
                                        fieldIdx,
                                        binaryData,
                                        expectedLength);
                        break;
                    case THROW:
                        throw new EnforcerException(
                                "Column '%s'"
                                        + String.format(
                                                " is %s, however, a string of length %s is being written into it. "
                                                        + "You can set job configuration '%s' "
                                                        + "to control this behaviour.",
                                                (shouldPad ? "BINARY(" : "VARBINARY(")
                                                        + expectedLength
                                                        + ")",
                                                actualLength,
                                                TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key()),
                                fieldNames[i]);
                }
            }
        }

        return updatedRowData != null ? updatedRowData : rowData;
    }

    private static UpdatableRowData trimOrPad(
            RowData rowData,
            UpdatableRowData updatedRowData,
            int fieldIdx,
            byte[] binaryData,
            int length) {
        if (updatedRowData == null) {
            updatedRowData = new UpdatableRowData(rowData, rowData.getArity());
        }
        updatedRowData.setField(fieldIdx, Arrays.copyOf(binaryData, length));
        return updatedRowData;
    }

    @Override
    public String toString() {
        return String.format("LengthEnforcer(fields=[%s])", String.join(", ", fieldNames));
    }
}
