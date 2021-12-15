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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.CharLengthEnforcer;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Processes {@link RowData} to enforce the following constraints:
 *
 * <ul>
 *   <li>{@code NOT NULL} column constraint of a sink table
 *   <li>{@code CHAR(length)}/@{code VARCHAR(length)}: trim string values to comply with the {@code
 *       length} defined in their corresponding types.
 * </ul>
 */
@Internal
public class ConstraintEnforcer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final NotNullEnforcer notNullEnforcer;
    private final int[] notNullFieldIndices;
    private final String[] allFieldNames;

    private final ExecutionConfigOptions.CharLengthEnforcer charLengthEnforcer;
    private final int[] charFieldIndices;
    private final int[] charFieldLengths;
    private final BitSet charFieldShouldPad;

    private final String operatorName;

    private transient StreamRecordCollector<RowData> collector;

    private ConstraintEnforcer(
            NotNullEnforcer notNullEnforcer,
            int[] notNullFieldIndices,
            ExecutionConfigOptions.CharLengthEnforcer charLengthEnforcer,
            int[] charFieldIndices,
            int[] charFieldLengths,
            BitSet charFieldShouldPad,
            String[] allFieldNames,
            String operatorName) {
        this.notNullEnforcer = notNullEnforcer;
        this.notNullFieldIndices = notNullFieldIndices;
        this.charLengthEnforcer = charLengthEnforcer;
        this.charFieldIndices = charFieldIndices;
        this.charFieldLengths = charFieldLengths;
        this.charFieldShouldPad = charFieldShouldPad;
        this.allFieldNames = allFieldNames;
        this.operatorName = operatorName;
    }

    @Override
    public String getOperatorName() {
        return operatorName;
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new StreamRecordCollector<>(output);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Helper builder, so that the {@link ConstraintEnforcer} can be instantiated with only the NOT
     * NULL constraint validation, only the CHAR/VARCHAR length validation, or both.
     */
    public static class Builder {

        private NotNullEnforcer notNullEnforcer;
        private int[] notNullFieldIndices;

        private CharLengthEnforcer charLengthEnforcer;
        private List<CharFieldInfo> charFieldInfo;
        private String[] allFieldNames;

        private final List<String> operatorNames = new ArrayList<>();

        private boolean isConfigured = false;

        public void addNotNullConstraint(
                NotNullEnforcer notNullEnforcer,
                int[] notNullFieldIndices,
                List<String> notNullFieldNames,
                String[] allFieldNames) {
            checkArgument(
                    notNullFieldIndices.length > 0,
                    "ConstraintValidator requires that there are not-null fields.");
            this.notNullFieldIndices = notNullFieldIndices;
            this.notNullEnforcer = notNullEnforcer;
            this.allFieldNames = allFieldNames;
            if (notNullEnforcer != null) {
                operatorNames.add(
                        String.format(
                                "NotNullEnforcer(fields=[%s])",
                                String.join(", ", notNullFieldNames)));
                this.isConfigured = true;
            }
        }

        public void addCharLengthConstraint(
                ExecutionConfigOptions.CharLengthEnforcer charLengthEnforcer,
                List<CharFieldInfo> charFieldInfo,
                List<String> charFieldNames,
                String[] allFieldNames) {
            this.charLengthEnforcer = charLengthEnforcer;
            if (this.charLengthEnforcer == CharLengthEnforcer.TRIM_PAD) {
                checkArgument(
                        charFieldInfo.size() > 0,
                        "ConstraintValidator requires that there are CHAR/VARCHAR fields.");
                this.charFieldInfo = charFieldInfo;
                this.allFieldNames = allFieldNames;

                operatorNames.add(
                        String.format(
                                "CharLengthEnforcer(fields=[%s])",
                                String.join(", ", charFieldNames)));
                this.isConfigured = true;
            }
        }

        /**
         * If neither of NOT NULL or CHAR/VARCHAR length enforcers are configured, null is returned.
         */
        public ConstraintEnforcer build() {
            if (isConfigured) {
                String operatorName =
                        "ConstraintEnforcer[" + String.join(", ", operatorNames) + "]";
                return new ConstraintEnforcer(
                        notNullEnforcer,
                        notNullFieldIndices,
                        charLengthEnforcer,
                        charFieldInfo != null
                                ? charFieldInfo.stream().mapToInt(cfi -> cfi.fieldIdx).toArray()
                                : null,
                        charFieldInfo != null
                                ? charFieldInfo.stream().mapToInt(cfi -> cfi.length).toArray()
                                : null,
                        charFieldInfo != null ? buildShouldPad(charFieldInfo) : null,
                        allFieldNames,
                        operatorName);
            }
            return null;
        }
    }

    private static BitSet buildShouldPad(List<CharFieldInfo> charFieldInfo) {
        BitSet shouldPad = new BitSet(charFieldInfo.size());
        for (int i = 0; i < charFieldInfo.size(); i++) {
            if (charFieldInfo.get(i).shouldPad) {
                shouldPad.set(i);
            }
        }
        return shouldPad;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData processedRowData = processNotNullConstraint(element.getValue());
        if (processedRowData != null) {
            collector.collect(processCharConstraint(processedRowData));
        }
    }

    private @Nullable RowData processNotNullConstraint(RowData rowData) {
        if (notNullEnforcer == null) {
            return rowData;
        }

        for (int index : notNullFieldIndices) {
            if (rowData.isNullAt(index)) {
                switch (notNullEnforcer) {
                    case ERROR:
                        throw new TableException(
                                String.format(
                                        "Column '%s' is NOT NULL, however, a null value is being written into it. "
                                                + "You can set job configuration '%s'='%s' "
                                                + "to suppress this exception and drop such records silently.",
                                        allFieldNames[index],
                                        TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key(),
                                        NotNullEnforcer.DROP.name()));
                    case DROP:
                        return null;
                }
            }
        }
        return rowData;
    }

    private RowData processCharConstraint(RowData rowData) {
        if (charLengthEnforcer == null
                || charLengthEnforcer == ExecutionConfigOptions.CharLengthEnforcer.IGNORE) {
            return rowData;
        }

        UpdatableRowData updatedRowData = null;

        for (int i = 0; i < charFieldIndices.length; i++) {
            final int fieldIdx = charFieldIndices[i];
            final int length = charFieldLengths[i];
            final BinaryStringData stringData = (BinaryStringData) rowData.getString(fieldIdx);
            final int sourceStrLength = stringData.numChars();

            if (charFieldShouldPad.get(i) && sourceStrLength < length) {
                if (updatedRowData == null) {
                    updatedRowData = new UpdatableRowData(rowData, allFieldNames.length);
                }
                final int srcSizeInBytes = stringData.getSizeInBytes();
                final byte[] newString = new byte[srcSizeInBytes + length - sourceStrLength];
                for (int j = srcSizeInBytes; j < newString.length; j++) {
                    newString[j] = (byte) 32; // space
                }
                SegmentsUtil.copyToBytes(stringData.getSegments(), 0, newString, 0, srcSizeInBytes);
                updatedRowData.setField(fieldIdx, StringData.fromBytes(newString));
            } else if (sourceStrLength > length) {
                if (updatedRowData == null) {
                    updatedRowData = new UpdatableRowData(rowData, allFieldNames.length);
                }
                updatedRowData.setField(fieldIdx, stringData.substring(0, length));
            }
        }

        return updatedRowData != null ? updatedRowData : rowData;
    }

    /**
     * Helper POJO to keep info about CHAR/VARCHAR Fields, used to determine if trimming or padding
     * is needed.
     */
    @Internal
    public static class CharFieldInfo {
        private final int fieldIdx;
        private final Integer length;
        private final boolean shouldPad;

        public CharFieldInfo(int fieldIdx, @Nullable Integer length, boolean shouldPad) {
            this.fieldIdx = fieldIdx;
            this.length = length;
            this.shouldPad = shouldPad;
        }

        public int fieldIdx() {
            return fieldIdx;
        }
    }
}
