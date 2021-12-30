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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TypeLengthEnforcer;
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

    private final TypeLengthEnforcer typeLengthEnforcer;
    private final int[] charFieldIndices;
    private final int[] charFieldLengths;
    private final BitSet charFieldCouldPad;
    private final int[] binaryFieldIndices;
    private final int[] binaryFieldLengths;
    private final BitSet binaryFieldCouldPad;

    private final String operatorName;

    private transient StreamRecordCollector<RowData> collector;

    private ConstraintEnforcer(
            NotNullEnforcer notNullEnforcer,
            int[] notNullFieldIndices,
            TypeLengthEnforcer typeLengthEnforcer,
            int[] charFieldIndices,
            int[] charFieldLengths,
            BitSet charFieldCouldPad,
            int[] binaryFieldIndices,
            int[] binaryFieldLengths,
            BitSet binaryFieldCouldPad,
            String[] allFieldNames,
            String operatorName) {
        this.notNullEnforcer = notNullEnforcer;
        this.notNullFieldIndices = notNullFieldIndices;
        this.typeLengthEnforcer = typeLengthEnforcer;
        this.charFieldIndices = charFieldIndices;
        this.charFieldLengths = charFieldLengths;
        this.charFieldCouldPad = charFieldCouldPad;
        this.binaryFieldIndices = binaryFieldIndices;
        this.binaryFieldLengths = binaryFieldLengths;
        this.binaryFieldCouldPad = binaryFieldCouldPad;
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
     * NULL constraint validation, only the CHAR/VARCHAR length validation, only the
     * BINARY/VARBINARY length validation or combinations of them, or all of them.
     */
    public static class Builder {

        private NotNullEnforcer notNullEnforcer;
        private int[] notNullFieldIndices;

        private TypeLengthEnforcer typeLengthEnforcer;
        private List<FieldInfo> charFieldInfo;
        private List<FieldInfo> binaryFieldInfo;
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
                TypeLengthEnforcer typeLengthEnforcer,
                List<FieldInfo> charFieldInfo,
                List<String> charFieldNames,
                String[] allFieldNames) {
            this.typeLengthEnforcer = typeLengthEnforcer;
            if (this.typeLengthEnforcer == TypeLengthEnforcer.TRIM_PAD) {
                checkArgument(
                        charFieldInfo.size() > 0,
                        "ConstraintValidator requires that there are CHAR/VARCHAR fields.");
                this.charFieldInfo = charFieldInfo;
                this.allFieldNames = allFieldNames;

                operatorNames.add(
                        String.format(
                                "LengthEnforcer(fields=[%s])", String.join(", ", charFieldNames)));
                this.isConfigured = true;
            }
        }

        public void addBinaryLengthConstraint(
                TypeLengthEnforcer typeLengthEnforcer,
                List<FieldInfo> binaryFieldInfo,
                List<String> binaryFieldNames,
                String[] allFieldNames) {
            this.typeLengthEnforcer = typeLengthEnforcer;
            if (this.typeLengthEnforcer == TypeLengthEnforcer.TRIM_PAD) {
                checkArgument(
                        binaryFieldInfo.size() > 0,
                        "ConstraintValidator requires that there are BINARY/VARBINARY fields.");
                this.binaryFieldInfo = binaryFieldInfo;
                this.allFieldNames = allFieldNames;

                operatorNames.add(
                        String.format(
                                "LengthEnforcer(fields=[%s])",
                                String.join(", ", binaryFieldNames)));
                this.isConfigured = true;
            }
        }

        /**
         * If neither of NOT NULL or CHAR/VARCHAR length or BINARY/VARBINARY enforcers are
         * configured, null is returned.
         */
        public ConstraintEnforcer build() {
            if (isConfigured) {
                String operatorName =
                        "ConstraintEnforcer[" + String.join(", ", operatorNames) + "]";
                return new ConstraintEnforcer(
                        notNullEnforcer,
                        notNullFieldIndices,
                        typeLengthEnforcer,
                        charFieldInfo != null
                                ? charFieldInfo.stream().mapToInt(fi -> fi.fieldIdx).toArray()
                                : null,
                        charFieldInfo != null
                                ? charFieldInfo.stream().mapToInt(fi -> fi.length).toArray()
                                : null,
                        charFieldInfo != null ? buildCouldPad(charFieldInfo) : null,
                        binaryFieldInfo != null
                                ? binaryFieldInfo.stream().mapToInt(fi -> fi.fieldIdx).toArray()
                                : null,
                        binaryFieldInfo != null
                                ? binaryFieldInfo.stream().mapToInt(fi -> fi.length).toArray()
                                : null,
                        binaryFieldInfo != null ? buildCouldPad(binaryFieldInfo) : null,
                        allFieldNames,
                        operatorName);
            }
            return null;
        }
    }

    private static BitSet buildCouldPad(List<FieldInfo> charFieldInfo) {
        BitSet couldPad = new BitSet(charFieldInfo.size());
        for (int i = 0; i < charFieldInfo.size(); i++) {
            if (charFieldInfo.get(i).couldPad) {
                couldPad.set(i);
            }
        }
        return couldPad;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData processedRowData = processNotNullConstraint(element.getValue());
        // If NOT NULL constraint is not respected don't proceed to process the other constraints,
        // simply drop the record.
        if (processedRowData != null) {
            processedRowData = processCharConstraint(processedRowData);
            processedRowData = processBinaryConstraint(processedRowData);
            collector.collect(processedRowData);
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
        if (typeLengthEnforcer == null
                || typeLengthEnforcer == TypeLengthEnforcer.IGNORE
                || charFieldIndices == null) {
            return rowData;
        }

        UpdatableRowData updatedRowData = null;

        for (int i = 0; i < charFieldIndices.length; i++) {
            final int fieldIdx = charFieldIndices[i];
            final int length = charFieldLengths[i];
            final BinaryStringData stringData = (BinaryStringData) rowData.getString(fieldIdx);
            final int sourceStrLength = stringData.numChars();

            if (charFieldCouldPad.get(i) && sourceStrLength < length) {
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

    private RowData processBinaryConstraint(RowData rowData) {
        if (typeLengthEnforcer == null
                || typeLengthEnforcer == TypeLengthEnforcer.IGNORE
                || binaryFieldIndices == null) {
            return rowData;
        }

        UpdatableRowData updatedRowData = null;

        for (int i = 0; i < binaryFieldLengths.length; i++) {
            final int fieldIdx = binaryFieldIndices[i];
            final int length = binaryFieldLengths[i];
            final byte[] binaryData = rowData.getBinary(fieldIdx);
            final int sourceLength = binaryData.length;

            // Trimming takes places because of the shorter length used in `Arrays.copyOf` and
            // padding because of the longer length, as implicitly the trailing bytes are 0.
            if ((sourceLength > length) || (binaryFieldCouldPad.get(i) && sourceLength < length)) {
                if (updatedRowData == null) {
                    updatedRowData = new UpdatableRowData(rowData, allFieldNames.length);
                }
                updatedRowData.setField(fieldIdx, Arrays.copyOf(binaryData, length));
            }
        }

        return updatedRowData != null ? updatedRowData : rowData;
    }

    /**
     * Helper POJO to keep info about CHAR/VARCHAR/BINARY/VARBINARY fields, used to determine if
     * trimming or padding is needed.
     */
    @Internal
    public static class FieldInfo {
        private final int fieldIdx;
        private final Integer length;
        private final boolean couldPad;

        public FieldInfo(int fieldIdx, @Nullable Integer length, boolean couldPad) {
            this.fieldIdx = fieldIdx;
            this.length = length;
            this.couldPad = couldPad;
        }

        public int fieldIdx() {
            return fieldIdx;
        }
    }
}
