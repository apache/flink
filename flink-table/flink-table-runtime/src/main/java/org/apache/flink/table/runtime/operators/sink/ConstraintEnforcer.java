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

import static org.apache.flink.table.api.config.ExecutionConfigOptions.CharPrecisionEnforcer;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Processes {@link RowData} to enforce the following constraints:
 *
 * <ul>
 *   <li>{@code NOT NULL} column constraint of a sink table
 *   <li>{@code CHAR(precision)}/@{code VARCHAR(precision)}: trim string values to comply with the
 *       {@code precision} defined in their corresponding types.
 * </ul>
 */
@Internal
public class ConstraintEnforcer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final NotNullEnforcer notNullEnforcer;
    private final int[] notNullFieldIndices;
    private final String[] allFieldNames;

    private final CharPrecisionEnforcer charPrecisionEnforcer;
    private final int[] charFieldIndices;
    private final int[] charFieldPrecisions;
    private final BitSet charFieldShouldPad;

    private final String operatorName;

    private transient StreamRecordCollector<RowData> collector;

    private ConstraintEnforcer(
            NotNullEnforcer notNullEnforcer,
            int[] notNullFieldIndices,
            CharPrecisionEnforcer charPrecisionEnforcer,
            int[] charFieldIndices,
            int[] charFieldPrecisions,
            BitSet charFieldShouldPad,
            String[] allFieldNames,
            String operatorName) {
        this.notNullEnforcer = notNullEnforcer;
        this.notNullFieldIndices = notNullFieldIndices;
        this.charPrecisionEnforcer = charPrecisionEnforcer;
        this.charFieldIndices = charFieldIndices;
        this.charFieldPrecisions = charFieldPrecisions;
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
     * NULL constraint validation, only the CHAR/VARCHAR precision validation, or both.
     */
    public static class Builder {

        private NotNullEnforcer notNullEnforcer;
        private int[] notNullFieldIndices;

        private CharPrecisionEnforcer charPrecisionEnforcer;
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

        public void addCharPrecisionConstraint(
                CharPrecisionEnforcer charPrecisionEnforcer,
                List<CharFieldInfo> charFieldInfo,
                List<String> charFieldNames,
                String[] allFieldNames) {
            this.charPrecisionEnforcer = charPrecisionEnforcer;
            if (this.charPrecisionEnforcer == CharPrecisionEnforcer.TRIM_PAD) {
                checkArgument(
                        charFieldInfo.size() > 0,
                        "ConstraintValidator requires that there are CHAR/VARCHAR fields.");
                this.charFieldInfo = charFieldInfo;
                this.allFieldNames = allFieldNames;

                operatorNames.add(
                        String.format(
                                "CharPrecisionEnforcer(fields=[%s])",
                                String.join(", ", charFieldNames)));
                this.isConfigured = true;
            }
        }

        /**
         * If neither of NOT NULL or CHAR/VARCHAR precision enforcers are configured, null is
         * returned.
         */
        public ConstraintEnforcer build() {
            if (isConfigured) {
                String operatorName =
                        "ConstraintEnforcer[" + String.join(", ", operatorNames) + "]";
                return new ConstraintEnforcer(
                        notNullEnforcer,
                        notNullFieldIndices,
                        charPrecisionEnforcer,
                        charFieldInfo != null
                                ? charFieldInfo.stream().mapToInt(cfi -> cfi.fieldIdx).toArray()
                                : null,
                        charFieldInfo != null
                                ? charFieldInfo.stream().mapToInt(cfi -> cfi.precision).toArray()
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
        if (charPrecisionEnforcer == null
                || charPrecisionEnforcer == CharPrecisionEnforcer.IGNORE) {
            return rowData;
        }

        UpdatableRowData updatedRowData = null;

        for (int i = 0; i < charFieldIndices.length; i++) {
            final int fieldIdx = charFieldIndices[i];
            final int precision = charFieldPrecisions[i];
            final BinaryStringData stringData = (BinaryStringData) rowData.getString(fieldIdx);
            final int sourceStrLength = stringData.numChars();

            if (charFieldShouldPad.get(i) && sourceStrLength < precision) {
                if (updatedRowData == null) {
                    updatedRowData = new UpdatableRowData(rowData, allFieldNames.length);
                }
                final int srcSizeInBytes = stringData.getSizeInBytes();
                final byte[] newString = new byte[srcSizeInBytes + precision - sourceStrLength];
                for (int j = srcSizeInBytes; j < newString.length; j++) {
                    newString[j] = (byte) 32; // space
                }
                SegmentsUtil.copyToBytes(stringData.getSegments(), 0, newString, 0, srcSizeInBytes);
                updatedRowData.setField(fieldIdx, StringData.fromBytes(newString));
            } else if (sourceStrLength > precision) {
                if (updatedRowData == null) {
                    updatedRowData = new UpdatableRowData(rowData, allFieldNames.length);
                }
                updatedRowData.setField(fieldIdx, stringData.substring(0, precision));
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
        private final Integer precision;
        private final boolean shouldPad;

        public CharFieldInfo(int fieldIdx, @Nullable Integer precision, boolean shouldPad) {
            this.fieldIdx = fieldIdx;
            this.precision = precision;
            this.shouldPad = shouldPad;
        }

        public int fieldIdx() {
            return fieldIdx;
        }
    }
}
