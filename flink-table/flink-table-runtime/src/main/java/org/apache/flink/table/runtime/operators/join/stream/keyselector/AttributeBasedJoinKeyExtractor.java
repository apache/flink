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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    private final Map<Integer, Map<AttributeRef, AttributeRef>> joinAttributeMap;
    private final List<InternalTypeInfo<RowData>> inputTypes;

    // Cache for pre-computed key extraction structures for getJoinKeyFromCurrentRows
    private final Map<Integer, List<KeyExtractor>> currentRowsFieldIndices;
    // Cache for pre-computed key field indices for getJoinKeyFromInput
    private final Map<Integer, List<Integer>> inputKeyFieldIndices;

    // Fields for common key logic
    private final Map<Integer, List<KeyExtractor>> commonKeyExtractors;
    private final Map<Integer, InternalTypeInfo<RowData>> commonKeyTypes;

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
        this.currentRowsFieldIndices = new HashMap<>();
        this.inputKeyFieldIndices = new HashMap<>();
        this.commonKeyExtractors = new HashMap<>();
        this.commonKeyTypes = new HashMap<>();

        // Eagerly initialize the caches for key extraction
        if (this.inputTypes != null) {
            for (int i = 0; i < this.inputTypes.size(); i++) {
                this.currentRowsFieldIndices.put(i, createJoinKeyFieldCurrentRowsExtractors(i));
                this.inputKeyFieldIndices.put(i, createJoinKeyFieldInputExtractors(i));
            }
        }

        initializeCommonKeyStructures();
    }

    @Override
    public RowData getJoinKeyFromInput(RowData row, int inputId) {
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

        // Retrieve pre-computed key field indices from the cache
        final List<Integer> keyFieldIndices = inputKeyFieldIndices.get(inputId);

        if (keyFieldIndices == null || keyFieldIndices.isEmpty()) {
            // Mappings exist, but none point to fields *within* this inputId (config error?), use
            // default key.
            // This case should ideally be caught by createJoinKeyFieldInputExtractors during init
            // if config is bad.
            return DEFAULT_KEY;
        }

        return buildKeyRow(row, inputId, keyFieldIndices);
    }

    @Override
    public RowData getJoinKeyFromCurrentRows(int depth, RowData[] currentRows) {
        if (depth == 0) {
            return DEFAULT_KEY;
        }
        // Retrieve pre-computed extractors from the cache
        List<KeyExtractor> keyExtractors = currentRowsFieldIndices.get(depth);

        // If no extractors are found for this depth (e.g., depth out of bounds, though
        // createJoinKeyFieldCurrentRowsExtractors should place an empty list for valid depths with
        // no
        // conditions),
        // or if the list is empty (no relevant join attributes for this depth), return default key.
        if (keyExtractors == null || keyExtractors.isEmpty()) {
            return DEFAULT_KEY;
        }
        return buildKeyRow(keyExtractors, currentRows);
    }

    private List<KeyExtractor> createJoinKeyFieldCurrentRowsExtractors(int depth) {
        if (depth == 0) {
            return Collections.emptyList();
        }
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
    public InternalTypeInfo<RowData> getJoinKeyType(int inputId) {
        if (inputId == 0) {
            // For input 0, we have to go through all the records.
            // We use the fixed default key type, since have no rows to the left to generate
            //  a lookup key.
            return DEFAULT_KEY_TYPE;
        }

        // Determine key fields based on the *right* side attributes for this inputId.
        final List<Integer> keyFieldIndices = createJoinKeyFieldInputExtractors(inputId);

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
    private List<Integer> createJoinKeyFieldInputExtractors(int inputId) {
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

    // ==================== Common Key Methods ====================

    @Override
    public RowData getCommonKey(RowData row, int inputId) {
        List<KeyExtractor> extractors = commonKeyExtractors.get(inputId);
        // If inputId is not in the map (e.g., inputTypes was empty), or list is empty
        if (extractors == null || extractors.isEmpty()) {
            return DEFAULT_KEY;
        }

        GenericRowData commonKeyRow = new GenericRowData(extractors.size());
        // The KeyExtractors in commonKeyExtractors are already set up with inputIdToAccess =
        // inputId
        RowData[] tempRows = new RowData[inputId + 1]; // Max index needed is inputId
        tempRows[inputId] = row;

        for (int i = 0; i < extractors.size(); i++) {
            commonKeyRow.setField(i, extractors.get(i).getValue(tempRows));
        }
        return commonKeyRow;
    }

    @Override
    public InternalTypeInfo<RowData> getCommonKeyType(int inputId) {
        return commonKeyTypes.getOrDefault(inputId, DEFAULT_KEY_TYPE);
    }

    private void initializeCommonKeyStructures() {
        if (inputTypes.isEmpty() || joinAttributeMap.isEmpty()) {
            for (int i = 0; i < inputTypes.size(); i++) {
                this.commonKeyExtractors.put(i, Collections.emptyList());
                this.commonKeyTypes.put(i, DEFAULT_KEY_TYPE);
            }
            return;
        }

        Map<AttributeRef, AttributeRef> parent = new HashMap<>();
        Map<AttributeRef, Integer> rank = new HashMap<>();
        Set<AttributeRef> allAttrRefs = new HashSet<>();

        for (Map<AttributeRef, AttributeRef> mapping : joinAttributeMap.values()) {
            allAttrRefs.addAll(mapping.keySet());
            allAttrRefs.addAll(mapping.values());
        }

        if (allAttrRefs.isEmpty()) {
            for (int i = 0; i < inputTypes.size(); i++) {
                this.commonKeyExtractors.put(i, Collections.emptyList());
                this.commonKeyTypes.put(i, DEFAULT_KEY_TYPE);
            }
            return;
        }

        for (AttributeRef attrRef : allAttrRefs) {
            parent.put(attrRef, attrRef);
            rank.put(attrRef, 0);
        }

        for (Map<AttributeRef, AttributeRef> mapping : joinAttributeMap.values()) {
            for (Map.Entry<AttributeRef, AttributeRef> condition : mapping.entrySet()) {
                unionAttributeSets(parent, rank, condition.getKey(), condition.getValue());
            }
        }

        Map<AttributeRef, Set<AttributeRef>> equivalenceSets = new HashMap<>();
        for (AttributeRef attrRef : allAttrRefs) {
            AttributeRef root = findAttributeSet(parent, attrRef);
            equivalenceSets.computeIfAbsent(root, k -> new HashSet<>()).add(attrRef);
        }

        List<Set<AttributeRef>> commonConceptualAttributeSets = new ArrayList<>();
        for (Set<AttributeRef> eqSet : equivalenceSets.values()) {
            boolean isCommon = true;
            if (joinAttributeMap.isEmpty()) { // Should be caught by earlier check
                isCommon = false;
            }

            for (Map<AttributeRef, AttributeRef> conditionsForStep : joinAttributeMap.values()) {
                if (conditionsForStep.isEmpty()) { // A cross-join step
                    isCommon = false;
                    break;
                }
                boolean foundInThisStep = false;
                for (Map.Entry<AttributeRef, AttributeRef> condition :
                        conditionsForStep.entrySet()) {
                    if (eqSet.contains(condition.getKey())
                            || eqSet.contains(condition.getValue())) {
                        foundInThisStep = true;
                        break;
                    }
                }
                if (!foundInThisStep) {
                    isCommon = false;
                    break;
                }
            }
            if (isCommon) {
                commonConceptualAttributeSets.add(eqSet);
            }
        }

        for (int currentInputId = 0; currentInputId < inputTypes.size(); currentInputId++) {
            List<AttributeRef> commonAttrsForThisInput = new ArrayList<>();
            for (Set<AttributeRef> eqSet : commonConceptualAttributeSets) {
                for (AttributeRef attrRef : eqSet) {
                    if (attrRef.inputId == currentInputId) {
                        commonAttrsForThisInput.add(attrRef);
                        break;
                    }
                }
            }

            commonAttrsForThisInput.sort(Comparator.comparingInt(attr -> attr.fieldIndex));

            if (commonAttrsForThisInput.isEmpty()) {
                this.commonKeyExtractors.put(currentInputId, Collections.emptyList());
                this.commonKeyTypes.put(currentInputId, DEFAULT_KEY_TYPE);
            } else {
                List<KeyExtractor> extractors = new ArrayList<>();
                LogicalType[] keyFieldTypes = new LogicalType[commonAttrsForThisInput.size()];
                String[] keyFieldNames = new String[commonAttrsForThisInput.size()];
                RowType originalRowType = inputTypes.get(currentInputId).toRowType();

                for (int i = 0; i < commonAttrsForThisInput.size(); i++) {
                    AttributeRef attr = commonAttrsForThisInput.get(i);
                    validateFieldIndex(currentInputId, attr.fieldIndex, originalRowType);
                    LogicalType fieldType = originalRowType.getTypeAt(attr.fieldIndex);
                    extractors.add(new KeyExtractor(currentInputId, attr.fieldIndex, fieldType));
                    keyFieldTypes[i] = fieldType;
                    keyFieldNames[i] =
                            originalRowType.getFieldNames().get(attr.fieldIndex) + "_common";
                }
                this.commonKeyExtractors.put(currentInputId, extractors);
                this.commonKeyTypes.put(
                        currentInputId,
                        InternalTypeInfo.of(RowType.of(keyFieldTypes, keyFieldNames)));
            }
        }
    }

    // DSU find operation (Path Compression)
    private static AttributeRef findAttributeSet(
            Map<AttributeRef, AttributeRef> parent, AttributeRef item) {
        if (!parent.get(item).equals(item)) {
            parent.put(item, findAttributeSet(parent, parent.get(item)));
        }
        return parent.get(item);
    }

    // DSU union operation (Union by Rank/Size)
    private static void unionAttributeSets(
            Map<AttributeRef, AttributeRef> parent,
            Map<AttributeRef, Integer> rank,
            AttributeRef a,
            AttributeRef b) {
        AttributeRef rootA = findAttributeSet(parent, a);
        AttributeRef rootB = findAttributeSet(parent, b);

        if (!rootA.equals(rootB)) {
            if (rank.get(rootA) < rank.get(rootB)) {
                parent.put(rootA, rootB);
            } else if (rank.get(rootA) > rank.get(rootB)) {
                parent.put(rootB, rootA);
            } else {
                parent.put(rootB, rootA);
                rank.put(rootA, rank.get(rootA) + 1);
            }
        }
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
