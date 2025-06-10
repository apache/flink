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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

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
public class AttributeBasedJoinKeyExtractor implements JoinKeyExtractor, Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap;
    private final List<RowType> inputTypes;

    // Cache for pre-computed key extraction structures
    private final Map<Integer, List<KeyExtractor>> inputIdToExtractorsMap;
    private final Map<Integer, List<Integer>> inputKeyFieldIndices;
    private final Map<Integer, List<KeyExtractor>> commonJoinKeyExtractors;
    private RowType commonJoinKeyType;

    /**
     * Creates an AttributeBasedJoinKeyExtractor.
     *
     * @param joinAttributeMap Map defining equi-join conditions. Outer key: inputId (>= 1). The
     *     value is a list of {@link ConditionAttributeRef} where each element defines an equi-join
     *     condition between a previous input (`leftInputId`, `leftFieldIndex`) and the current
     *     input (`rightInputId`, `rightFieldIndex`).
     * @param inputTypes Type information for all input streams (indexed 0 to N-1).
     */
    public AttributeBasedJoinKeyExtractor(
            final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            final List<RowType> inputTypes) {
        this.joinAttributeMap = joinAttributeMap;
        this.inputTypes = inputTypes;
        this.inputIdToExtractorsMap = new HashMap<>();
        this.inputKeyFieldIndices = new HashMap<>();
        this.commonJoinKeyExtractors = new HashMap<>();

        initializeCaches();
        initializeCommonJoinKeyStructures();
    }

    // ==================== Public Interface Methods ====================

    @Override
    public RowData getJoinKey(RowData row, int inputId) {
        if (inputId == 0) {
            return null;
        }

        final List<ConditionAttributeRef> attributeMapping = joinAttributeMap.get(inputId);
        if (attributeMapping == null || attributeMapping.isEmpty()) {
            return null;
        }

        final List<Integer> keyFieldIndices = inputKeyFieldIndices.get(inputId);
        if (keyFieldIndices == null || keyFieldIndices.isEmpty()) {
            return null;
        }

        return buildKeyRow(row, inputId, keyFieldIndices);
    }

    @Override
    public RowData getLeftSideJoinKey(int depth, RowData joinedRowData) {
        if (depth == 0) {
            return null;
        }

        List<KeyExtractor> keyExtractors = inputIdToExtractorsMap.get(depth);
        if (keyExtractors == null || keyExtractors.isEmpty()) {
            return null;
        }

        return buildKeyRow(keyExtractors, joinedRowData);
    }

    @Override
    @Nullable
    public RowType getJoinKeyType(int inputId) {
        if (inputId == 0) {
            return null;
        }

        final List<Integer> keyFieldIndices = createJoinKeyFieldInputExtractors(inputId);
        if (keyFieldIndices.isEmpty()) {
            return null;
        }

        return buildJoinKeyType(inputId, keyFieldIndices);
    }

    @Override
    public int[] getJoinKeyIndices(int inputId) {
        final List<Integer> keyFieldIndices = this.inputKeyFieldIndices.get(inputId);
        if (keyFieldIndices == null) {
            return new int[0];
        }
        return keyFieldIndices.stream().mapToInt(i -> i).toArray();
    }

    @Override
    public RowType getCommonJoinKeyType() {
        return this.commonJoinKeyType;
    }

    @Override
    public RowData getCommonJoinKey(RowData row, int inputId) {
        List<KeyExtractor> extractors = commonJoinKeyExtractors.get(inputId);
        if (extractors == null || extractors.isEmpty()) {
            return null;
        }

        return buildCommonJoinKey(row, extractors);
    }

    @Override
    public int[] getCommonJoinKeyIndices(int inputId) {
        List<KeyExtractor> extractors = commonJoinKeyExtractors.get(inputId);
        if (extractors == null || extractors.isEmpty()) {
            return new int[0];
        }

        return extractors.stream().mapToInt(KeyExtractor::getFieldIndexInSourceRow).toArray();
    }

    // ==================== Initialization Methods ====================

    private void initializeCaches() {
        if (this.inputTypes != null) {
            for (int i = 0; i < this.inputTypes.size(); i++) {
                this.inputIdToExtractorsMap.put(i, createLeftJoinKeyFieldExtractors(i));
                this.inputKeyFieldIndices.put(i, createJoinKeyFieldInputExtractors(i));
            }
        }
    }

    private List<KeyExtractor> createLeftJoinKeyFieldExtractors(int depth) {
        if (depth == 0) {
            return Collections.emptyList();
        }

        List<ConditionAttributeRef> attributeMapping = joinAttributeMap.get(depth);
        if (attributeMapping == null || attributeMapping.isEmpty()) {
            return Collections.emptyList();
        }

        List<KeyExtractor> keyExtractors = new ArrayList<>();
        for (ConditionAttributeRef entry : attributeMapping) {
            AttributeRef leftAttrRef = getLeftAttributeRef(depth, entry);
            keyExtractors.add(createKeyExtractor(leftAttrRef));
        }

        keyExtractors.sort(
                Comparator.comparingInt(KeyExtractor::getInputIdToAccess)
                        .thenComparingInt(KeyExtractor::getFieldIndexInSourceRow));
        return keyExtractors;
    }

    private static AttributeRef getLeftAttributeRef(int depth, ConditionAttributeRef entry) {
        AttributeRef leftAttrRef = new AttributeRef(entry.leftInputId, entry.leftFieldIndex);
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
        return leftAttrRef;
    }

    private KeyExtractor createKeyExtractor(AttributeRef attrRef) {
        RowType rowType = inputTypes.get(attrRef.inputId);
        validateFieldIndex(attrRef.inputId, attrRef.fieldIndex, rowType);
        LogicalType fieldType = rowType.getTypeAt(attrRef.fieldIndex);

        // Calculate absolute field index by summing up field counts of previous inputs
        int absoluteFieldIndex = attrRef.fieldIndex;
        for (int i = 0; i < attrRef.inputId; i++) {
            absoluteFieldIndex += inputTypes.get(i).getFieldCount();
        }

        return new KeyExtractor(attrRef.inputId, attrRef.fieldIndex, absoluteFieldIndex, fieldType);
    }

    private List<Integer> createJoinKeyFieldInputExtractors(int inputId) {
        final List<ConditionAttributeRef> attributeMapping = joinAttributeMap.get(inputId);
        if (attributeMapping == null) {
            return Collections.emptyList();
        }

        return attributeMapping.stream()
                .filter(rightAttrRef -> rightAttrRef.rightInputId == inputId)
                .map(rightAttrRef -> rightAttrRef.rightFieldIndex)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    // ==================== Key Building Methods ====================

    private RowData buildKeyRow(List<KeyExtractor> keyExtractors, RowData joinedRowData) {
        if (keyExtractors.isEmpty()) {
            return null;
        }

        GenericRowData keyRow = new GenericRowData(keyExtractors.size());
        for (int i = 0; i < keyExtractors.size(); i++) {
            keyRow.setField(i, keyExtractors.get(i).getLeftSideKey(joinedRowData));
        }
        return keyRow;
    }

    private GenericRowData buildKeyRow(
            RowData sourceRow, int inputId, List<Integer> keyFieldIndices) {
        final GenericRowData keyRow = new GenericRowData(keyFieldIndices.size());
        final RowType rowType = inputTypes.get(inputId);

        for (int i = 0; i < keyFieldIndices.size(); i++) {
            final int fieldIndex = keyFieldIndices.get(i);
            validateFieldIndex(inputId, fieldIndex, rowType);

            final LogicalType fieldType = rowType.getTypeAt(fieldIndex);
            final RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(fieldType, fieldIndex);
            final Object value = fieldGetter.getFieldOrNull(sourceRow);
            keyRow.setField(i, value);
        }
        return keyRow;
    }

    private RowData buildCommonJoinKey(RowData row, List<KeyExtractor> extractors) {
        GenericRowData commonJoinKeyRow = new GenericRowData(extractors.size());

        for (int i = 0; i < extractors.size(); i++) {
            commonJoinKeyRow.setField(i, extractors.get(i).getRightSideKey(row));
        }
        return commonJoinKeyRow;
    }

    private RowType buildJoinKeyType(int inputId, List<Integer> keyFieldIndices) {
        final RowType originalRowType = inputTypes.get(inputId);
        final LogicalType[] keyTypes = new LogicalType[keyFieldIndices.size()];
        final String[] keyNames = new String[keyFieldIndices.size()];

        for (int i = 0; i < keyFieldIndices.size(); i++) {
            final int fieldIndex = keyFieldIndices.get(i);
            validateFieldIndex(inputId, fieldIndex, originalRowType);

            keyTypes[i] = originalRowType.getTypeAt(fieldIndex);
            keyNames[i] = originalRowType.getFieldNames().get(fieldIndex) + "_key";
        }

        return RowType.of(keyTypes, keyNames);
    }

    // ==================== Common Key Methods ====================

    private void initializeCommonJoinKeyStructures() {
        this.commonJoinKeyType = null;

        if (this.inputTypes != null) {
            for (int i = 0; i < this.inputTypes.size(); i++) {
                this.commonJoinKeyExtractors.put(i, Collections.emptyList());
            }
        }

        assert inputTypes != null;
        if (inputTypes.isEmpty() || joinAttributeMap.isEmpty()) {
            return;
        }

        Map<AttributeRef, AttributeRef> parent = new HashMap<>();
        Map<AttributeRef, Integer> rank = new HashMap<>();
        Set<AttributeRef> allAttrRefs = collectAllAttributeRefs();

        if (allAttrRefs.isEmpty()) {
            return;
        }

        initializeDisjointSets(parent, rank, allAttrRefs);
        processJoinConditions(parent, rank);
        Map<AttributeRef, Set<AttributeRef>> equivalenceSets =
                buildEquivalenceSets(parent, allAttrRefs);
        List<Set<AttributeRef>> commonConceptualAttributeSets =
                findCommonConceptualAttributeSets(equivalenceSets);

        if (commonConceptualAttributeSets.isEmpty()) {
            return;
        }

        processCommonAttributes(commonConceptualAttributeSets);
    }

    private Set<AttributeRef> collectAllAttributeRefs() {
        Set<AttributeRef> allAttrRefs = new HashSet<>();
        for (List<ConditionAttributeRef> mapping : joinAttributeMap.values()) {
            for (ConditionAttributeRef attrRef : mapping) {
                allAttrRefs.add(new AttributeRef(attrRef.leftInputId, attrRef.leftFieldIndex));
                allAttrRefs.add(new AttributeRef(attrRef.rightInputId, attrRef.rightFieldIndex));
            }
        }
        return allAttrRefs;
    }

    private void initializeDisjointSets(
            Map<AttributeRef, AttributeRef> parent,
            Map<AttributeRef, Integer> rank,
            Set<AttributeRef> allAttrRefs) {
        for (AttributeRef attrRef : allAttrRefs) {
            parent.put(attrRef, attrRef);
            rank.put(attrRef, 0);
        }
    }

    private void processJoinConditions(
            Map<AttributeRef, AttributeRef> parent, Map<AttributeRef, Integer> rank) {
        for (List<ConditionAttributeRef> mapping : joinAttributeMap.values()) {
            for (ConditionAttributeRef condition : mapping) {
                unionAttributeSets(
                        parent,
                        rank,
                        new AttributeRef(condition.leftInputId, condition.leftFieldIndex),
                        new AttributeRef(condition.rightInputId, condition.rightFieldIndex));
            }
        }
    }

    private Map<AttributeRef, Set<AttributeRef>> buildEquivalenceSets(
            Map<AttributeRef, AttributeRef> parent, Set<AttributeRef> allAttrRefs) {
        Map<AttributeRef, Set<AttributeRef>> equivalenceSets = new HashMap<>();
        for (AttributeRef attrRef : allAttrRefs) {
            AttributeRef root = findAttributeSet(parent, attrRef);
            equivalenceSets.computeIfAbsent(root, k -> new HashSet<>()).add(attrRef);
        }
        return equivalenceSets;
    }

    private List<Set<AttributeRef>> findCommonConceptualAttributeSets(
            Map<AttributeRef, Set<AttributeRef>> equivalenceSets) {
        List<Set<AttributeRef>> commonConceptualAttributeSets = new ArrayList<>();
        for (Set<AttributeRef> eqSet : equivalenceSets.values()) {
            if (isCommonConceptualAttributeSet(eqSet)) {
                commonConceptualAttributeSets.add(eqSet);
            }
        }
        return commonConceptualAttributeSets;
    }

    private boolean isCommonConceptualAttributeSet(Set<AttributeRef> eqSet) {
        if (joinAttributeMap.isEmpty()) {
            return false;
        }

        for (List<ConditionAttributeRef> conditionsForStep : joinAttributeMap.values()) {
            if (conditionsForStep.isEmpty()) {
                return false;
            }

            boolean foundInThisStep = false;
            for (ConditionAttributeRef condition : conditionsForStep) {
                if (eqSet.contains(
                                new AttributeRef(condition.leftInputId, condition.leftFieldIndex))
                        || eqSet.contains(
                                new AttributeRef(
                                        condition.rightInputId, condition.rightFieldIndex))) {
                    foundInThisStep = true;
                    break;
                }
            }
            if (!foundInThisStep) {
                return false;
            }
        }
        return true;
    }

    private void processCommonAttributes(List<Set<AttributeRef>> commonConceptualAttributeSets) {
        for (int currentInputId = 0; currentInputId < inputTypes.size(); currentInputId++) {
            List<AttributeRef> commonAttrsForThisInput =
                    findCommonAttributesForInput(currentInputId, commonConceptualAttributeSets);

            if (commonAttrsForThisInput.isEmpty()) {
                throw new IllegalStateException(
                        "No common attributes found for inputId "
                                + currentInputId
                                + ". This indicates a misconfiguration in joinAttributeMap.");
            }

            processInputCommonAttributes(currentInputId, commonAttrsForThisInput);
        }
    }

    private List<AttributeRef> findCommonAttributesForInput(
            int currentInputId, List<Set<AttributeRef>> commonConceptualAttributeSets) {
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
        return commonAttrsForThisInput;
    }

    private void processInputCommonAttributes(
            int currentInputId, List<AttributeRef> commonAttrsForThisInput) {
        List<KeyExtractor> extractors = new ArrayList<>();
        LogicalType[] keyFieldTypes = new LogicalType[commonAttrsForThisInput.size()];
        String[] keyFieldNames = new String[commonAttrsForThisInput.size()];
        RowType originalRowType = inputTypes.get(currentInputId);

        for (int i = 0; i < commonAttrsForThisInput.size(); i++) {
            AttributeRef attr = commonAttrsForThisInput.get(i);
            validateFieldIndex(currentInputId, attr.fieldIndex, originalRowType);
            LogicalType fieldType = originalRowType.getTypeAt(attr.fieldIndex);
            extractors.add(
                    new KeyExtractor(currentInputId, attr.fieldIndex, attr.fieldIndex, fieldType));
            keyFieldTypes[i] = fieldType;
            keyFieldNames[i] = originalRowType.getFieldNames().get(attr.fieldIndex) + "_common";
        }

        this.commonJoinKeyExtractors.put(currentInputId, extractors);

        if (currentInputId == 0 && !extractors.isEmpty()) {
            this.commonJoinKeyType = RowType.of(keyFieldTypes, keyFieldNames);
        }
    }

    // ==================== Helper Methods ====================

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

    private static AttributeRef findAttributeSet(
            Map<AttributeRef, AttributeRef> parent, AttributeRef item) {
        if (!parent.get(item).equals(item)) {
            parent.put(item, findAttributeSet(parent, parent.get(item)));
        }
        return parent.get(item);
    }

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

    // ==================== Inner Classes ====================

    /** Helper class to store pre-computed information for extracting a key part. */
    // TODO we actually need int[] for the indices
    // because we can have multiple common join keys as fields
    // this whole file will be refactored in a next ticket
    private static final class KeyExtractor implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int inputIdToAccess;
        private final int fieldIndexInSourceRow;
        private final int absoluteFieldIndex;
        private final LogicalType fieldType;
        private transient RowData.FieldGetter fieldGetter;

        public KeyExtractor(
                int inputIdToAccess,
                int fieldIndexInSourceRow,
                int absoluteFieldIndex,
                LogicalType fieldType) {
            this.inputIdToAccess = inputIdToAccess;
            this.fieldIndexInSourceRow = fieldIndexInSourceRow;
            this.absoluteFieldIndex = absoluteFieldIndex;
            this.fieldType = fieldType;
            this.fieldGetter =
                    RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
        }

        public Object getRightSideKey(RowData joinedRowData) {
            if (joinedRowData == null) {
                return null;
            }
            if (this.fieldGetter == null) {
                this.fieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
            }
            return this.fieldGetter.getFieldOrNull(joinedRowData);
        }

        public Object getLeftSideKey(RowData joinedRowData) {
            if (joinedRowData == null) {
                return null;
            }
            if (this.fieldGetter == null) {
                this.fieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.absoluteFieldIndex);
            }
            return this.fieldGetter.getFieldOrNull(joinedRowData);
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
            if (this.fieldType != null) {
                this.fieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
            }
        }
    }

    /** Reference to a specific field (fieldIndex) within a specific input stream (inputId). */
    public static final class AttributeRef implements Serializable {
        public int inputId;
        public int fieldIndex;

        public AttributeRef() {
            // Default constructor for deserialization
        }

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
            return "InputId:" + inputId + ";FieldIndex:" + fieldIndex + ";";
        }
    }

    /** Reference to a specific field (fieldIndex) within a specific input stream (inputId). */
    public static final class ConditionAttributeRef implements Serializable {
        public int leftInputId;
        public int leftFieldIndex;
        public int rightInputId;
        public int rightFieldIndex;

        public ConditionAttributeRef() {
            // Default constructor for deserialization
        }

        public ConditionAttributeRef(
                int leftInputId, int leftFieldIndex, int rightInputId, int rightFieldIndex) {
            this.leftInputId = leftInputId;
            this.leftFieldIndex = leftFieldIndex;
            this.rightInputId = rightInputId;
            this.rightFieldIndex = rightFieldIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConditionAttributeRef that = (ConditionAttributeRef) o;
            return leftInputId == that.leftInputId
                    && leftFieldIndex == that.leftFieldIndex
                    && rightInputId == that.rightInputId
                    && rightFieldIndex == that.rightFieldIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(leftInputId, leftFieldIndex, rightInputId, rightFieldIndex);
        }

        @Override
        public String toString() {
            return "LeftInputId:"
                    + leftInputId
                    + ";LeftFieldIndex:"
                    + leftFieldIndex
                    + ";RightInputId:"
                    + rightInputId
                    + ";RightFieldIndex:"
                    + rightFieldIndex
                    + ";";
        }
    }
}
