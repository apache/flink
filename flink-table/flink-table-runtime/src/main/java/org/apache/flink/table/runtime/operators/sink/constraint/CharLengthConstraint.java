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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.util.SegmentsUtil;

import javax.annotation.Nullable;

import java.util.BitSet;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER;

/** Enforces length constraints on the input {@link RowData}. */
@Internal
final class CharLengthConstraint implements Constraint {

    private final TypeLengthEnforcementStrategy enforcementStrategy;
    private final int[] fieldIndices;
    private final int[] fieldLengths;
    private final String[] fieldNames;
    private final BitSet fieldCouldPad;

    CharLengthConstraint(
            final TypeLengthEnforcementStrategy enforcementStrategy,
            final int[] charFieldIndices,
            final int[] charFieldLengths,
            final String[] charFieldNames,
            final BitSet charFieldCouldPad) {
        this.enforcementStrategy = enforcementStrategy;
        this.fieldIndices = charFieldIndices;
        this.fieldLengths = charFieldLengths;
        this.fieldNames = charFieldNames;
        this.fieldCouldPad = charFieldCouldPad;
    }

    @Nullable
    @Override
    public RowData enforce(RowData rowData) {
        UpdatableRowData updatedRowData = null;

        for (int i = 0; i < fieldIndices.length; i++) {
            final int fieldIdx = fieldIndices[i];
            final int expectedLength = fieldLengths[i];
            final BinaryStringData stringData = (BinaryStringData) rowData.getString(fieldIdx);
            final int actualLength = stringData.numChars();
            final boolean shouldPad = fieldCouldPad.get(i);

            switch (enforcementStrategy) {
                case TRIM_PAD:
                    updatedRowData =
                            trimOrPad(
                                    rowData,
                                    actualLength,
                                    expectedLength,
                                    updatedRowData,
                                    stringData,
                                    fieldIdx,
                                    shouldPad);
                    break;
                case THROW:
                    if (actualLength > expectedLength
                            || shouldPad && actualLength < expectedLength) {
                        throw new EnforcerException(
                                "Column '%s'"
                                        + String.format(
                                                " is %s, however, a string of length %s is being written into it. "
                                                        + "You can set job configuration '%s' "
                                                        + "to control this behaviour.",
                                                (shouldPad ? "CHAR(" : "VARCHAR(")
                                                        + expectedLength
                                                        + ")",
                                                actualLength,
                                                TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key()),
                                fieldNames[i]);
                    }
                    break;
            }
        }

        return updatedRowData != null ? updatedRowData : rowData;
    }

    private UpdatableRowData trimOrPad(
            RowData rowData,
            int actualLength,
            int expectedLength,
            UpdatableRowData updatedRowData,
            BinaryStringData stringData,
            int fieldIdx,
            boolean canPad) {
        if (canPad && actualLength < expectedLength) {
            if (updatedRowData == null) {
                updatedRowData = new UpdatableRowData(rowData, rowData.getArity());
            }
            final int srcSizeInBytes = stringData.getSizeInBytes();
            final byte[] newString = new byte[srcSizeInBytes + expectedLength - actualLength];
            for (int j = srcSizeInBytes; j < newString.length; j++) {
                newString[j] = (byte) 32; // space
            }
            SegmentsUtil.copyToBytes(
                    stringData.getSegments(), stringData.getOffset(), newString, 0, srcSizeInBytes);
            updatedRowData.setField(fieldIdx, StringData.fromBytes(newString));
        } else if (actualLength > expectedLength) {
            if (updatedRowData == null) {
                updatedRowData = new UpdatableRowData(rowData, rowData.getArity());
            }
            updatedRowData.setField(fieldIdx, stringData.substring(0, expectedLength));
        }
        return updatedRowData;
    }

    @Override
    public String toString() {
        return String.format("LengthEnforcer(fields=[%s])", String.join(", ", fieldNames));
    }
}
