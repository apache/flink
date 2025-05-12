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

package org.apache.flink.table.runtime.operators.join.stream.keyselector;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link JoinKeyExtractor} that derives keys based on {@link AttributeRef} mappings provided in
 * {@code joinAttributeMap}. It defines how attributes from different input streams are related
 * through equi-join conditions, assuming input 0 is the base and subsequent inputs join to
 * preceding ones.
 */
public class AttributeBasedJoinKeyExtractor implements JoinKeyExtractor {
    private static final long serialVersionUID = 1L;

    // Default key/type used when no specific join keys are applicable (e.g., input 0, cross joins).
    private static final GenericRowData DEFAULT_KEY = new GenericRowData(1);

    static {
        DEFAULT_KEY.setField(0, "__DEFAULT_MULTI_JOIN_STATE_KEY__");
    }

    private static final InternalTypeInfo<RowData> DEFAULT_KEY_TYPE =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                // Fixed type for the default key. Length matches the static key
                                // value.
                                new VarCharType(false, 31)
                            },
                            new String[] {"default_key"}));

    private final transient Map<Integer, Map<AttributeRef, AttributeRef>>
            joinAttributeMap; // Transient as it's configuration
    private final List<InternalTypeInfo<RowData>> inputTypes;

    // Cache for pre-computed key extraction structures
    private final Map<Integer, List<KeyExtractor>> extractorCache;

    /**
     * Creates an AttributeBasedJoinKeyExtractor.
     *
     * @param joinAttributeMap Map defining equi-join conditions. Outer key: inputId (>= 1). Inner
     *     key: {@link AttributeRef} to a field in a *previous* input. Inner value: {@link
     *     AttributeRef} to the corresponding field in the *current* input (inputId == outer key).
     * @param inputTypes Type information for all input streams (indexed 0 to N-1).
     */
    public AttributeBasedJoinKeyExtractor(
            final Map<Integer, Map<AttributeRef, AttributeRef>> joinAttributeMap,
            final List<InternalTypeInfo<RowData>> inputTypes) {
        this.joinAttributeMap = joinAttributeMap;
        this.inputTypes = inputTypes;
        this.extractorCache = new HashMap<>();
    }

    @Override
    public RowData getKeyForInput(RowData row, int inputId) {
        if (inputId == 0) {
            // Input 0 uses the fixed default key as it's the start of the join chain.
            return DEFAULT_KEY;
        }

        // For inputs > 0, storage key derived from current row's equi-join fields.
        final Map<AttributeRef, AttributeRef> attributeMapping = joinAttributeMap.get(inputId);
        if (attributeMapping == null || attributeMapping.isEmpty()) {
            // No equi-join conditions defined for this input, use default key.
            return DEFAULT_KEY;
        }

        // Indices of fields in the *current* input (inputId) used as the *right* side of joins.
        final List<Integer> keyFieldIndices = determineKeyFieldIndices(inputId);

        if (keyFieldIndices.isEmpty()) {
            // Mappings exist, but none point to fields *within* this inputId (config error?), use
            // default key.
            return DEFAULT_KEY;
        }

        return buildKeyRow(row, inputId, keyFieldIndices);
    }

    @Override
    public RowData getKeyForDepthFromCurrentRows(int depth, RowData[] currentRows) {
        if (depth == 0) {
            return DEFAULT_KEY;
        }
        List<KeyExtractor> keyExtractors =
                extractorCache.computeIfAbsent(depth, this::createExtractorsForDepth);
        if (keyExtractors.isEmpty()) {
            return DEFAULT_KEY;
        }
        return buildKeyRow(keyExtractors, currentRows);
    }

    private List<KeyExtractor> createExtractorsForDepth(int depth) {
        Map<AttributeRef, AttributeRef> attributeMapping = joinAttributeMap.get(depth);
        if (attributeMapping == null || attributeMapping.isEmpty()) {
            return Collections.emptyList();
        }
        List<KeyExtractor> keyExtractors = new ArrayList<>();
        for (Map.Entry<AttributeRef, AttributeRef> entry : attributeMapping.entrySet()) {
            AttributeRef leftAttrRef = entry.getKey();
            if (leftAttrRef.inputId >= depth) {
                throw new IllegalStateException(
                        "Invalid joinAttributeMap configuration for depth "
                                + depth
                                + ". Left attribute "
                                + leftAttrRef
                                + " does not reference a previous input (< "
                                + depth
                                + ").");
            }
            InternalTypeInfo<RowData> typeInfo = inputTypes.get(leftAttrRef.inputId);
            RowType rowType = typeInfo.toRowType();
            int fieldIndex = leftAttrRef.fieldIndex;
            validateFieldIndex(leftAttrRef.inputId, fieldIndex, rowType);
            LogicalType fieldType = rowType.getTypeAt(fieldIndex);
            keyExtractors.add(new KeyExtractor(leftAttrRef.inputId, fieldIndex, fieldType));
        }
        // Sort extractors by inputId then fieldIndex for deterministic and fast key row building
        keyExtractors.sort(
                java.util.Comparator.comparingInt(KeyExtractor::getInputIdToAccess)
                        .thenComparingInt(KeyExtractor::getFieldIndexInSourceRow));
        return keyExtractors;
    }

    private RowData buildKeyRow(List<KeyExtractor> keyExtractors, RowData[] currentRows) {
        if (keyExtractors.isEmpty()) {
            return DEFAULT_KEY;
        }
        GenericRowData keyRow = new GenericRowData(keyExtractors.size());
        for (int i = 0; i < keyExtractors.size(); i++) {
            keyRow.setField(i, keyExtractors.get(i).getValue(currentRows));
        }
        return keyRow;
    }

    @Override
    public InternalTypeInfo<RowData> getKeyType(int inputId) {
        if (inputId == 0) {
            // For input 0, we have to go through all the records.
            // We use the fixed default key type, since have no rows to the left to generate
            //  a lookup key.
            return DEFAULT_KEY_TYPE;
        }

        // Determine key fields based on the *right* side attributes for this inputId.
        final List<Integer> keyFieldIndices = determineKeyFieldIndices(inputId);

        if (keyFieldIndices.isEmpty()) {
            // No equi-join fields defined for this input's state key. Use default type.
            return DEFAULT_KEY_TYPE;
        }

        // Build RowType for the key based on identified field types.
        final InternalTypeInfo<RowData> originalTypeInfo = inputTypes.get(inputId);
        final RowType originalRowType = originalTypeInfo.toRowType();
        final LogicalType[] keyTypes = new LogicalType[keyFieldIndices.size()];
        final String[] keyNames = new String[keyFieldIndices.size()];

        for (int i = 0; i < keyFieldIndices.size(); i++) {
            final int fieldIndex = keyFieldIndices.get(i);
            validateFieldIndex(inputId, fieldIndex, originalRowType); // Ensure valid index

            keyTypes[i] = originalRowType.getTypeAt(fieldIndex);
            keyNames[i] = originalRowType.getFieldNames().get(fieldIndex) + "_key"; // Suffix name
        }

        final RowType keyRowType = RowType.of(keyTypes, keyNames);
        return InternalTypeInfo.of(keyRowType);
    }

    // ==================== Helper Methods ====================

    /** Determines indices of fields in inputId that are join keys (right side of conditions). */
    private List<Integer> determineKeyFieldIndices(int inputId) {
        final Map<AttributeRef, AttributeRef> attributeMapping = joinAttributeMap.get(inputId);
        if (attributeMapping == null) {
            return Collections.emptyList();
        }
        // Need field indices from the *value* (right side) where inputId matches.
        return attributeMapping.values().stream()
                .filter(rightAttrRef -> rightAttrRef.inputId == inputId)
                .map(rightAttrRef -> rightAttrRef.fieldIndex)
                .distinct() // Ensure uniqueness
                .sorted() // Ensure consistent order
                .collect(Collectors.toList());
    }

    /** Builds a key row from a source row using specified field indices. */
    private GenericRowData buildKeyRow(
            RowData sourceRow, int inputId, List<Integer> keyFieldIndices) {
        final GenericRowData keyRow = new GenericRowData(keyFieldIndices.size());
        final InternalTypeInfo<RowData> typeInfo = inputTypes.get(inputId);
        final RowType rowType = typeInfo.toRowType();

        for (int i = 0; i < keyFieldIndices.size(); i++) {
            final int fieldIndex = keyFieldIndices.get(i);
            validateFieldIndex(inputId, fieldIndex, rowType); // Validate before access

            final LogicalType fieldType = rowType.getTypeAt(fieldIndex);
            final RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(fieldType, fieldIndex);
            final Object value = fieldGetter.getFieldOrNull(sourceRow);
            keyRow.setField(i, value);
        }
        return keyRow;
    }

    /** Performs bounds checking for field access. */
    private void validateFieldIndex(int inputId, int fieldIndex, RowType rowType) {
        if (fieldIndex >= rowType.getFieldCount() || fieldIndex < 0) {
            throw new IndexOutOfBoundsException(
                    "joinAttributeMap references field index "
                            + fieldIndex
                            + " which is out of bounds for inputId "
                            + inputId
                            + " with type "
                            + rowType);
        }
    }

    /**
     * Helper class to store pre-computed information for extracting a key part. This avoids
     * repeated lookups and object creations in {@code getKeyForStateLookup}.
     */
    private static final class KeyExtractor implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int inputIdToAccess; // Which RowData from currentRows to access
        private final int fieldIndexInSourceRow; // Field index within that RowData
        private final LogicalType fieldType; // Stored to recreate FieldGetter if needed

        private transient RowData.FieldGetter fieldGetter;

        public KeyExtractor(int inputIdToAccess, int fieldIndexInSourceRow, LogicalType fieldType) {
            this.inputIdToAccess = inputIdToAccess;
            this.fieldIndexInSourceRow = fieldIndexInSourceRow;
            this.fieldType = fieldType;
            // Initialize FieldGetter; will be re-initialized after deserialization or if null
            this.fieldGetter =
                    RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
        }

        public Object getValue(RowData[] currentRows) {
            RowData sourceRow = currentRows[inputIdToAccess];
            if (sourceRow == null) {
                return null;
            }
            // Re-initialize if transient and null (e.g., after deserialization or first use)
            if (this.fieldGetter == null) {
                this.fieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
            }
            return this.fieldGetter.getFieldOrNull(sourceRow);
        }

        public int getInputIdToAccess() {
            return inputIdToAccess;
        }

        public int getFieldIndexInSourceRow() {
            return fieldIndexInSourceRow;
        }

        private void readObject(java.io.ObjectInputStream in)
                throws java.io.IOException, ClassNotFoundException {
            in.defaultReadObject();
            // fieldGetter is transient, re-initialize it.
            // fieldType and fieldIndexInSourceRow are serialized.
            if (this.fieldType != null) {
                this.fieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
            }
        }
    }

    /**
     * Reference to a specific field (fieldIndex) within a specific input stream (inputId). Used in
     * {@code joinAttributeMap} to define equi-join relationships.
     */
    public static final class AttributeRef implements Serializable {
        private static final long serialVersionUID = 1L;

        public final int inputId;
        public final int fieldIndex;

        public AttributeRef(int inputId, int fieldIndex) {
            this.inputId = inputId;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AttributeRef that = (AttributeRef) o;
            return inputId == that.inputId && fieldIndex == that.fieldIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(inputId, fieldIndex);
        }

        @Override
        public String toString() {
            return "Input(" + inputId + ").Field(" + fieldIndex + ")";
        }
    }
}
