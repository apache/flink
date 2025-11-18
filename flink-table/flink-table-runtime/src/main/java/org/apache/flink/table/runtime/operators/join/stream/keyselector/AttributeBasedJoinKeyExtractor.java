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
import org.apache.flink.table.utils.NoCommonJoinKeyException;

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

/**
 * A {@link JoinKeyExtractor} that derives keys from {@link AttributeRef} mappings in {@code
 * joinAttributeMap}. It describes how attributes from different inputs are equated via equi-join
 * conditions. Input 0 is the base; each subsequent input joins to one of the preceding inputs.
 *
 * <p>Example used throughout the comments: t1.id1 = t2.user_id2 and t3.user_id3 = t2.user_id2. All
 * three attributes (t1.id1, t2.user_id2, t3.user_id3) represent the same conceptual key.
 *
 * <p>The {@code joinAttributeMap} for this example would be structured as follows:
 *
 * <ul>
 *   <li>Key `1` (for t2): A list containing one element `ConditionAttributeRef(leftInputId=0,
 *       leftFieldIndex=id1_idx, rightInputId=1, rightFieldIndex=user_id2_idx)`.
 *   <li>Key `2` (for t3): A list containing one element `ConditionAttributeRef(leftInputId=1,
 *       leftFieldIndex=user_id2_idx, rightInputId=2, rightFieldIndex=user_id3_idx)`.
 * </ul>
 */
public class AttributeBasedJoinKeyExtractor implements JoinKeyExtractor, Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap;
    private final List<RowType> inputTypes;

    // Cache for pre-computed key extraction structures.
    // leftKeyExtractorsMap: extractors that read the left side (joined row so far)
    //   using the same attribute order as in joinAttributeMap.
    private final Map<Integer, List<KeyExtractor>> leftKeyExtractorsMap;
    // rightKeyExtractorsMap: extractors to extract the right-side key from each input.
    private final Map<Integer, List<KeyExtractor>> rightKeyExtractorsMap;

    // Data structures for the "common join key" shared by all inputs.
    // Input 0 provides the canonical order and defines commonJoinKeyType.
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
        this.leftKeyExtractorsMap = new HashMap<>();
        this.rightKeyExtractorsMap = new HashMap<>();
        this.commonJoinKeyExtractors = new HashMap<>();

        initializeCaches();
        initializeCommonJoinKeyStructures();
        validateKeyStructures();
    }

    // ==================== Public Interface Methods ====================

    @Override
    public RowData getJoinKey(final RowData row, final int inputId) {
        if (inputId == 0) {
            return null;
        }

        final List<ConditionAttributeRef> attributeMapping = joinAttributeMap.get(inputId);
        if (attributeMapping == null || attributeMapping.isEmpty()) {
            return null;
        }

        final List<KeyExtractor> keyExtractors = rightKeyExtractorsMap.get(inputId);
        if (keyExtractors == null || keyExtractors.isEmpty()) {
            return null;
        }

        return buildKeyRowFromSourceRow(row, keyExtractors);
    }

    @Override
    public RowData getLeftSideJoinKey(final int depth, final RowData joinedRowData) {
        if (depth == 0) {
            return null;
        }

        final List<KeyExtractor> keyExtractors = leftKeyExtractorsMap.get(depth);
        if (keyExtractors == null || keyExtractors.isEmpty()) {
            return null;
        }

        return buildKeyRowFromJoinedRow(keyExtractors, joinedRowData);
    }

    @Override
    @Nullable
    public RowType getJoinKeyType(final int inputId) {
        if (inputId == 0) {
            return null;
        }

        final List<KeyExtractor> keyExtractors = this.rightKeyExtractorsMap.get(inputId);

        if (keyExtractors == null || keyExtractors.isEmpty()) {
            return null;
        }

        return buildJoinKeyType(inputId, keyExtractors);
    }

    @Override
    public int[] getJoinKeyIndices(final int inputId) {
        final List<KeyExtractor> keyFieldIndices = this.rightKeyExtractorsMap.get(inputId);
        if (keyFieldIndices == null) {
            return new int[0];
        }
        return keyFieldIndices.stream().mapToInt(KeyExtractor::getFieldIndexInSourceRow).toArray();
    }

    @Override
    public RowType getCommonJoinKeyType() {
        // We return an empty RowType if no common join key is defined.
        return Objects.requireNonNullElseGet(this.commonJoinKeyType, RowType::of);
    }

    @Override
    public @Nullable RowData getCommonJoinKey(final RowData row, final int inputId) {
        final List<KeyExtractor> extractors = commonJoinKeyExtractors.get(inputId);
        if (extractors == null || extractors.isEmpty()) {
            return null;
        }

        return buildKeyRowFromSourceRow(row, extractors);
    }

    @Override
    public int[] getCommonJoinKeyIndices(final int inputId) {
        final List<KeyExtractor> extractors = commonJoinKeyExtractors.get(inputId);
        if (extractors == null || extractors.isEmpty()) {
            return new int[0];
        }

        return extractors.stream().mapToInt(KeyExtractor::getFieldIndexInSourceRow).toArray();
    }

    // ==================== Initialization Methods ====================

    private void initializeCaches() {
        if (this.inputTypes != null) {
            for (int i = 0; i < this.inputTypes.size(); i++) {
                this.leftKeyExtractorsMap.put(i, createLeftJoinKeyFieldExtractors(i));
                this.rightKeyExtractorsMap.put(i, createRightJoinKeyExtractors(i));
            }
        }
    }

    private List<KeyExtractor> createLeftJoinKeyFieldExtractors(final int inputId) {
        if (inputId == 0) {
            return Collections.emptyList();
        }

        final List<ConditionAttributeRef> attributeMapping = joinAttributeMap.get(inputId);
        if (attributeMapping == null || attributeMapping.isEmpty()) {
            return Collections.emptyList();
        }

        final List<KeyExtractor> keyExtractors = new ArrayList<>();
        for (final ConditionAttributeRef entry : attributeMapping) {
            final AttributeRef leftAttrRef = getLeftAttributeRef(inputId, entry);
            keyExtractors.add(createKeyExtractor(leftAttrRef));
        }

        return keyExtractors;
    }

    private List<KeyExtractor> createRightJoinKeyExtractors(final int inputId) {
        final List<ConditionAttributeRef> attributeMapping = joinAttributeMap.get(inputId);
        if (attributeMapping == null) {
            return Collections.emptyList();
        }

        final List<KeyExtractor> keyExtractors = new ArrayList<>();
        for (final ConditionAttributeRef entry : attributeMapping) {
            final AttributeRef rightAttrRef = getRightAttributeRef(inputId, entry);
            keyExtractors.add(createKeyExtractor(rightAttrRef));
        }

        return keyExtractors;
    }

    private static AttributeRef getLeftAttributeRef(
            final int inputId, final ConditionAttributeRef entry) {
        final AttributeRef leftAttrRef = new AttributeRef(entry.leftInputId, entry.leftFieldIndex);
        if (leftAttrRef.inputId >= inputId) {
            throw new IllegalStateException(
                    "Invalid joinAttributeMap configuration for inputId "
                            + inputId
                            + ". Left attribute "
                            + leftAttrRef
                            + " does not reference a previous input (< "
                            + inputId
                            + ").");
        }
        return leftAttrRef;
    }

    private static AttributeRef getRightAttributeRef(
            final int inputId, final ConditionAttributeRef entry) {
        final AttributeRef rightAttrRef =
                new AttributeRef(entry.rightInputId, entry.rightFieldIndex);
        // For a given join step (e.g., input 2 joining to a previous input), the "right"
        // attribute in the join condition belongs to the current input (input 2).
        if (rightAttrRef.inputId != inputId) {
            throw new IllegalStateException(
                    "Invalid joinAttributeMap configuration for inputId "
                            + inputId
                            + ". Right attribute "
                            + rightAttrRef
                            + " must reference the current input ("
                            + inputId
                            + ").");
        }
        return rightAttrRef;
    }

    private KeyExtractor createKeyExtractor(final AttributeRef attrRef) {
        final RowType rowType = inputTypes.get(attrRef.inputId);
        validateFieldIndex(attrRef.inputId, attrRef.fieldIndex, rowType);
        final LogicalType fieldType = rowType.getTypeAt(attrRef.fieldIndex);

        // Absolute field index within the concatenated joined row =
        // current field index + sum(field counts of all previous inputs).
        int absoluteFieldIndex = attrRef.fieldIndex;
        for (int i = 0; i < attrRef.inputId; i++) {
            absoluteFieldIndex += inputTypes.get(i).getFieldCount();
        }

        return new KeyExtractor(attrRef.inputId, attrRef.fieldIndex, absoluteFieldIndex, fieldType);
    }

    // ==================== Key Building Methods ====================

    private RowData buildKeyRowFromJoinedRow(
            final List<KeyExtractor> keyExtractors, final RowData joinedRowData) {
        if (keyExtractors.isEmpty()) {
            return null;
        }

        final GenericRowData keyRow = new GenericRowData(keyExtractors.size());
        for (int i = 0; i < keyExtractors.size(); i++) {
            keyRow.setField(i, keyExtractors.get(i).getLeftSideKey(joinedRowData));
        }
        return keyRow;
    }

    private GenericRowData buildKeyRowFromSourceRow(
            final RowData sourceRow, final List<KeyExtractor> keyExtractors) {
        if (keyExtractors.isEmpty()) {
            return null;
        }

        final GenericRowData keyRow = new GenericRowData(keyExtractors.size());
        for (int i = 0; i < keyExtractors.size(); i++) {
            keyRow.setField(i, keyExtractors.get(i).getRightSideKey(sourceRow));
        }
        return keyRow;
    }

    private RowType buildJoinKeyType(final int inputId, final List<KeyExtractor> keyExtractors) {
        final RowType originalRowType = inputTypes.get(inputId);
        final LogicalType[] keyTypes = new LogicalType[keyExtractors.size()];
        final String[] keyNames = new String[keyExtractors.size()];

        for (int i = 0; i < keyExtractors.size(); i++) {
            final KeyExtractor extractor = keyExtractors.get(i);
            final int fieldIndex = extractor.getFieldIndexInSourceRow();
            validateFieldIndex(inputId, fieldIndex, originalRowType);

            keyTypes[i] = extractor.fieldType;
            keyNames[i] = originalRowType.getFieldNames().get(fieldIndex) + "_key";
        }

        return RowType.of(keyTypes, keyNames);
    }

    // ==================== Common Key Methods ====================

    /**
     * Builds the data structures that describe the common join key shared by all inputs.
     *
     * <p>Algorithm:
     *
     * <ol>
     *   <li>Collect all attributes referenced by any equality condition.
     *   <li>Run union-find: for each equi-join condition, union the two attributes so that directly
     *       or transitively equal attributes end up in the same set.
     *   <li>Form equivalence sets by grouping attributes that share the same union-find root.
     *   <li>Keep only those sets that touch every join step (see {@link
     *       #isCommonConceptualAttributeSet(Set)}). Each kept set is one conceptual key common to
     *       all inputs.
     *   <li>For each input, pick that input's attributes from the kept sets (in a consistent order)
     *       and create per-input key extractors. For input 0, also build the {@link RowType} of the
     *       common key.
     * </ol>
     *
     * <p><b>Example (same throughout):</b>
     *
     * <ol>
     *   <li>Join conditions: t1.id1 = t2.user_id2 and t3.user_id3 = t2.user_id2.
     *   <li>Union-find groups attributes into { (t1,id1), (t2,user_id2), (t3,user_id3) }.
     *   <li>The group touches both steps (t2↔t1 and t3↔t2), so it is the common key.
     *   <li>Create extractors for t1.id1, t2.user_id2, and t3.user_id3.
     *   <li>Define a one-field commonJoinKeyType based on t1.id1.
     * </ol>
     */
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

        // Maps an attribute to the representative of its equivalence set.
        final Map<AttributeRef, AttributeRef> attributeToRoot = new HashMap<>();
        // Used for the union-by-rank optimization to keep the union-find tree shallow.
        final Map<AttributeRef, Integer> rootRank = new HashMap<>();
        final Set<AttributeRef> allAttrRefs = collectAllAttributeRefs();

        if (allAttrRefs.isEmpty()) {
            return;
        }

        initializeDisjointSets(attributeToRoot, rootRank, allAttrRefs);
        findAttributeRoots(attributeToRoot, rootRank);
        final Map<AttributeRef, Set<AttributeRef>> equivalenceSets =
                buildEquivalenceSets(attributeToRoot, allAttrRefs);

        // From all equivalence sets, keep only those that touch every join step.
        final List<Set<AttributeRef>> commonConceptualAttributeSets =
                findCommonConceptualAttributeSets(equivalenceSets);
        processCommonAttributes(commonConceptualAttributeSets, attributeToRoot);
    }

    /**
     * Returns every attribute reference (input id + field index) present in the configured equality
     * conditions.
     *
     * <p>Example: with t1.id1 = t2.user_id2 and t3.user_id3 = t2.user_id2, this returns { (t1,id1),
     * (t2,user_id2), (t3,user_id3) }.
     */
    private Set<AttributeRef> collectAllAttributeRefs() {
        final Set<AttributeRef> allAttrRefs = new HashSet<>();
        for (final Map.Entry<Integer, List<ConditionAttributeRef>> entry :
                joinAttributeMap.entrySet()) {
            // Skip joinAttributeMap for key 0 where there are no join conditions to the left
            if (entry.getKey() == 0) {
                continue;
            }
            for (final ConditionAttributeRef attrRef : entry.getValue()) {
                allAttrRefs.add(new AttributeRef(attrRef.leftInputId, attrRef.leftFieldIndex));
                allAttrRefs.add(new AttributeRef(attrRef.rightInputId, attrRef.rightFieldIndex));
            }
        }
        return allAttrRefs;
    }

    /**
     * Initializes union-find: every attribute is the root of its own set with rank 0.
     *
     * <p>Example: attributes {(t1,id1), (t2,user_id2), (t3,user_id3)} start with attributeToRoot[a]
     * = a.
     */
    private void initializeDisjointSets(
            final Map<AttributeRef, AttributeRef> attributeToRoot,
            final Map<AttributeRef, Integer> rootRank,
            final Set<AttributeRef> allAttrRefs) {
        for (final AttributeRef attrRef : allAttrRefs) {
            attributeToRoot.put(attrRef, attrRef);
            rootRank.put(attrRef, 0);
        }
    }

    /**
     * Applies all equi-join conditions to union-find by uniting the corresponding attribute sets.
     *
     * <p>Example: unite (t2,user_id2) with (t1,id1), then (t3,user_id3) with (t2,user_id2),
     * yielding one set { (t1,id1), (t2,user_id2), (t3,user_id3) }.
     */
    private void findAttributeRoots(
            final Map<AttributeRef, AttributeRef> attributeToRoot,
            final Map<AttributeRef, Integer> rootRank) {
        for (final Map.Entry<Integer, List<ConditionAttributeRef>> entry :
                joinAttributeMap.entrySet()) {
            // Skip joinAttributeMap for key 0 where there are no join conditions to the left
            if (entry.getKey() == 0) {
                continue;
            }
            for (final ConditionAttributeRef condition : entry.getValue()) {
                unionAttributeSets(
                        attributeToRoot,
                        rootRank,
                        new AttributeRef(condition.leftInputId, condition.leftFieldIndex),
                        new AttributeRef(condition.rightInputId, condition.rightFieldIndex));
            }
        }
    }

    /** Converts the union-find forest into a map from root → full equivalence set. */
    private Map<AttributeRef, Set<AttributeRef>> buildEquivalenceSets(
            final Map<AttributeRef, AttributeRef> attributeToRoot,
            final Set<AttributeRef> allAttrRefs) {
        final Map<AttributeRef, Set<AttributeRef>> equivalenceSets = new HashMap<>();
        for (final AttributeRef attrRef : allAttrRefs) {
            final AttributeRef root = findAttributeSet(attributeToRoot, attrRef);
            equivalenceSets.computeIfAbsent(root, k -> new HashSet<>()).add(attrRef);
        }
        return equivalenceSets;
    }

    /**
     * From all equivalence sets, keep only those that intersect every join step. A set is "common"
     * only if each step has at least one attribute in the set.
     *
     * <p>Example: with steps (t2.user_id2 = t1.id1) and (t3.user_id3 = t2.user_id2), { (t1,id1),
     * (t2,user_id2), (t3,user_id3) } is kept; a set touching only one step is discarded.
     */
    private List<Set<AttributeRef>> findCommonConceptualAttributeSets(
            final Map<AttributeRef, Set<AttributeRef>> equivalenceSets) {
        final List<Set<AttributeRef>> commonConceptualAttributeSets = new ArrayList<>();
        for (final Set<AttributeRef> eqSet : equivalenceSets.values()) {
            if (isCommonConceptualAttributeSet(eqSet)) {
                commonConceptualAttributeSets.add(eqSet);
            }
        }
        return commonConceptualAttributeSets;
    }

    /**
     * Returns true if the given equivalence set intersects every configured join step. For each
     * step, we check whether the set contains any left or right attribute of that step.
     *
     * <p>Example: with steps (t2.user_id2 = t1.id1) and (t3.user_id3 = t2.user_id2): - { (t1,id1),
     * (t2,user_id2), (t3,user_id3) } → true - { (t1,other), (t2,other) } → false
     */
    private boolean isCommonConceptualAttributeSet(final Set<AttributeRef> eqSet) {
        if (joinAttributeMap.isEmpty()) {
            return false;
        }

        for (final List<ConditionAttributeRef> conditionsForStep : joinAttributeMap.values()) {
            if (conditionsForStep.isEmpty()) {
                return false;
            }

            boolean foundInThisStep = false;
            for (final ConditionAttributeRef condition : conditionsForStep) {
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

    /**
     * For each input, select its attributes that belong to the common sets and initialize
     * extractors. If any input contributes none, the multi-join is invalid (exception).
     *
     * <p>Example: from { (t1,id1), (t2,user_id2), (t3,user_id3) }, input 0 uses id1, input 1 uses
     * user_id2, input 2 uses user_id3. If some input had none, we would throw an exception.
     */
    private void processCommonAttributes(
            final List<Set<AttributeRef>> commonConceptualAttributeSets,
            final Map<AttributeRef, AttributeRef> attributeToRoot) {
        for (int currentInputId = 0; currentInputId < inputTypes.size(); currentInputId++) {
            final List<AttributeRef> commonAttrsForThisInput =
                    findCommonAttributesForInput(
                            currentInputId, commonConceptualAttributeSets, attributeToRoot);

            if (commonAttrsForThisInput.isEmpty()) {
                // This indicates that there is no common join key among all inputs.
                // In this case, we cannot use a multi-join, so throw an exception.
                throw new NoCommonJoinKeyException(
                        "All inputs in a multi-way join must share a common join key. Input #"
                                + currentInputId
                                + " does not share a join key with the other inputs. Please ensure all join"
                                + " conditions connect all inputs with a common key. Support for multiple"
                                + " independent join key groups is tracked under FLINK-37890.");
            }

            processInputCommonAttributes(currentInputId, commonAttrsForThisInput);
        }
    }

    /**
     * For a given input, pick at most one attribute per common set (first match), then sort by
     * field index. This defines the field order of the common key for that input.
     *
     * <p>Example: if input 1 participates in two common sets with fields user_id and account_id,
     * this returns [account_id, user_id] if account_id's index < user_id's index.
     */
    private List<AttributeRef> findCommonAttributesForInput(
            final int currentInputId,
            final List<Set<AttributeRef>> commonConceptualAttributeSets,
            final Map<AttributeRef, AttributeRef> attributeToRoot) {
        final List<AttributeRef> commonAttrsForThisInput = new ArrayList<>();
        for (final Set<AttributeRef> eqSet : commonConceptualAttributeSets) {
            for (final AttributeRef attrRef : eqSet) {
                if (attrRef.inputId == currentInputId) {
                    commonAttrsForThisInput.add(attrRef);
                    break;
                }
            }
        }

        // Important: ensure a consistent conceptual attribute ordering derived from roots.
        // The common key fields must have a canonical order across all inputs. This is
        // achieved by sorting based on the properties of the union-find root attribute
        // for each conceptual key set. We sort first by the root's field index and then
        // by the current attribute's field index as a tie-breaker for stability. This
        // ensures that input 0's attribute order defines the canonical order.
        commonAttrsForThisInput.sort(
                Comparator.<AttributeRef>comparingInt(
                                a -> {
                                    AttributeRef root = attributeToRoot.get(a);
                                    return root != null ? root.fieldIndex : -1;
                                })
                        // This is for stable ordering when two roots happen to have the same
                        // fieldIndex
                        .thenComparingInt(a -> a.fieldIndex));

        return commonAttrsForThisInput;
    }

    /**
     * Creates extractor objects and key type metadata for an input. Input 0 defines {@code
     * commonJoinKeyType} shared across all inputs.
     *
     * <p>Example: for input 1 with common attributes [user_id], we create an extractor for that
     * field and name it "user_id_common" with its logical type.
     */
    private void processInputCommonAttributes(
            final int currentInputId, final List<AttributeRef> commonAttrsForThisInput) {
        final List<KeyExtractor> extractors = new ArrayList<>();
        final String[] keyFieldNames = new String[commonAttrsForThisInput.size()];
        final RowType originalRowType = inputTypes.get(currentInputId);

        for (int i = 0; i < commonAttrsForThisInput.size(); i++) {
            final AttributeRef attr = commonAttrsForThisInput.get(i);
            extractors.add(createKeyExtractor(attr));
            keyFieldNames[i] = originalRowType.getFieldNames().get(attr.fieldIndex) + "_common";
        }

        this.commonJoinKeyExtractors.put(currentInputId, extractors);

        final LogicalType[] keyFieldTypes =
                extractors.stream().map(e -> e.fieldType).toArray(LogicalType[]::new);
        if (currentInputId == 0 && !extractors.isEmpty()) {
            this.commonJoinKeyType = RowType.of(keyFieldTypes, keyFieldNames);
        }
    }

    // ==================== Helper Methods ====================

    private void validateFieldIndex(
            final int inputId, final int fieldIndex, final RowType rowType) {
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
            final Map<AttributeRef, AttributeRef> attributeToRoot, final AttributeRef item) {
        if (!attributeToRoot.get(item).equals(item)) {
            attributeToRoot.put(item, findAttributeSet(attributeToRoot, attributeToRoot.get(item)));
        }
        return attributeToRoot.get(item);
    }

    private static void unionAttributeSets(
            final Map<AttributeRef, AttributeRef> attributeToRoot,
            final Map<AttributeRef, Integer> rootRank,
            final AttributeRef a,
            final AttributeRef b) {
        final AttributeRef rootA = findAttributeSet(attributeToRoot, a);
        final AttributeRef rootB = findAttributeSet(attributeToRoot, b);

        // Standard union-by-rank implementation to merge sets. The resulting root
        // depends on the rank of the sets, not on attribute properties like inputId.
        // A subsequent sorting step establishes a canonical ordering for the common key fields.
        // Following this logic, the root will be the common join key attributes from input 0
        if (!rootA.equals(rootB)) {
            if (rootRank.get(rootA) < rootRank.get(rootB)) {
                attributeToRoot.put(rootA, rootB);
            } else if (rootRank.get(rootA) > rootRank.get(rootB)) {
                attributeToRoot.put(rootB, rootA);
            } else {
                attributeToRoot.put(rootB, rootA);
                rootRank.put(rootA, rootRank.get(rootA) + 1);
            }
        }
    }

    // ==================== Validation Methods ====================

    /**
     * Validates internal key structures for consistency:
     *
     * <ol>
     *   <li>For every input id, the number of left-side extractors equals the number of right-side
     *       key-field indices.
     *   <li>If a common join key is defined, then for every input id the number of common key
     *       extractors equals the number of fields in {@code commonJoinKeyType}.
     *   <li>If a common join key is defined, each extractor's logical type matches the
     *       corresponding field type in {@code commonJoinKeyType}.
     * </ol>
     *
     * <p>Throws {@link IllegalStateException} if any inconsistency is found.
     */
    public void validateKeyStructures() {
        final int numInputs = inputTypes == null ? 0 : inputTypes.size();

        for (int inputId = 0; inputId < numInputs; inputId++) {
            if (inputId == 0) {
                if (!leftKeyExtractorsMap.get(inputId).isEmpty()) {
                    throw new IllegalStateException(
                            "Input 0 should not have left key extractors, but found left extractors "
                                    + leftKeyExtractorsMap.get(inputId)
                                    + ".");
                }

                // We skip validating the extracted keys type equality for input 0
                // because it has no left-side extractors.
                continue;
            }

            final List<KeyExtractor> leftExtractors = leftKeyExtractorsMap.get(inputId);
            final List<KeyExtractor> rightExtractors = rightKeyExtractorsMap.get(inputId);

            final int extractorsLength =
                    validateExtractorsLength(leftExtractors, rightExtractors, inputId);

            for (int j = 0; j < extractorsLength; j++) {
                final KeyExtractor rightExtractor = rightExtractors.get(j);
                final KeyExtractor leftExtractor = leftExtractors.get(j);

                if (leftExtractor == null || rightExtractor == null) {
                    throw new IllegalStateException(
                            "Null extractor found when validating key structures for field "
                                    + j
                                    + " on input "
                                    + inputId
                                    + ": left extractor "
                                    + leftExtractor
                                    + ", right extractor "
                                    + rightExtractor
                                    + ".");
                }

                final LogicalType leftType = leftExtractor.fieldType;
                final LogicalType rightType = rightExtractor.fieldType;
                if (!Objects.equals(leftType.getTypeRoot(), rightType.getTypeRoot())) {
                    throw new IllegalStateException(
                            "Type mismatch for join key field "
                                    + j
                                    + " on input "
                                    + inputId
                                    + ": left type "
                                    + leftType.getTypeRoot()
                                    + " vs right type "
                                    + rightType.getTypeRoot()
                                    + ".");
                }
            }
        }

        if (this.commonJoinKeyType != null) {
            final int expectedCommonFields = this.commonJoinKeyType.getFieldCount();

            for (int inputId = 0; inputId < numInputs; inputId++) {
                final List<KeyExtractor> commonExtractors = commonJoinKeyExtractors.get(inputId);
                final int actual = commonExtractors == null ? 0 : commonExtractors.size();

                if (actual != expectedCommonFields) {
                    throw new IllegalStateException(
                            "Mismatch in common key counts for input "
                                    + inputId
                                    + ": extractors ("
                                    + actual
                                    + ") vs commonJoinKeyType fields ("
                                    + expectedCommonFields
                                    + ").");
                }

                for (int i = 0; i < expectedCommonFields; i++) {
                    final LogicalType extractorType = commonExtractors.get(i).fieldType;
                    final LogicalType expectedType = this.commonJoinKeyType.getTypeAt(i);

                    if (!Objects.equals(extractorType.getTypeRoot(), expectedType.getTypeRoot())) {
                        throw new IllegalStateException(
                                "Type mismatch for common key field "
                                        + i
                                        + " on input "
                                        + inputId
                                        + ": extractor type "
                                        + extractorType.getTypeRoot()
                                        + " vs commonJoinKeyType "
                                        + expectedType.getTypeRoot()
                                        + ".");
                    }
                }
            }
        }
    }

    private static int validateExtractorsLength(
            final List<KeyExtractor> leftExtractors,
            final List<KeyExtractor> rightExtractors,
            final int inputId) {
        final int leftSize = leftExtractors == null ? 0 : leftExtractors.size();
        final int rightSize = rightExtractors == null ? 0 : rightExtractors.size();

        if (leftSize != rightSize) {
            throw new IllegalStateException(
                    "Mismatch in key counts for input "
                            + inputId
                            + ": left extractors ("
                            + leftSize
                            + ") vs right extractors ("
                            + rightSize
                            + ").");
        }
        return leftSize;
    }

    // ==================== Inner Classes ====================

    /**
     * Helper class to store pre-computed information for extracting a key part.
     *
     * <p>This class uses two separate {@link RowData.FieldGetter} instances because a key extractor
     * may need to extract a field from the left (already joined) side, which is a wide,
     * concatenated row, or the right (current input) side, which is a single source row. The field
     * indices for these two cases are different.
     */
    private static final class KeyExtractor implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int inputIdToAccess;
        private final int fieldIndexInSourceRow;
        private final int absoluteFieldIndex;
        private final LogicalType fieldType;
        private transient RowData.FieldGetter leftFieldGetter;
        private transient RowData.FieldGetter rightFieldGetter;

        public KeyExtractor(
                final int inputIdToAccess,
                final int fieldIndexInSourceRow,
                final int absoluteFieldIndex,
                final LogicalType fieldType) {
            this.inputIdToAccess = inputIdToAccess;
            this.fieldIndexInSourceRow = fieldIndexInSourceRow;
            this.absoluteFieldIndex = absoluteFieldIndex;
            this.fieldType = fieldType;
            this.rightFieldGetter =
                    RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
            this.leftFieldGetter =
                    RowData.createFieldGetter(this.fieldType, this.absoluteFieldIndex);
        }

        /**
         * Returns the key value from a single input row (right/current side of the current join
         * step).
         *
         * <p>Example: for a step joining t1 (input 0) and t2 (input 1) t1.id1 = t2.user_id2, the
         * extractor configured for t2 (input 1) returns t2.user_id2 value from the provided t2 row.
         */
        public Object getRightSideKey(final RowData row) {
            if (row == null) {
                return null;
            }
            if (this.rightFieldGetter == null) {
                this.rightFieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
            }
            return this.rightFieldGetter.getFieldOrNull(row);
        }

        /**
         * Returns the key value from the already-joined row (left side of the current join step).
         *
         * <p>Example: for a step joining t1 (input 0) and t2 (input 1) t1.id1 = t2.user_id2, the
         * extractor configured for t2 (input 1) returns t1.id1 value from the provided t1 row.
         */
        public Object getLeftSideKey(final RowData joinedRowData) {
            if (joinedRowData == null) {
                return null;
            }
            if (this.leftFieldGetter == null) {
                this.leftFieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.absoluteFieldIndex);
            }
            return this.leftFieldGetter.getFieldOrNull(joinedRowData);
        }

        public int getInputIdToAccess() {
            return inputIdToAccess;
        }

        public int getFieldIndexInSourceRow() {
            return fieldIndexInSourceRow;
        }

        private void readObject(final java.io.ObjectInputStream in)
                throws java.io.IOException, ClassNotFoundException {
            in.defaultReadObject();
            if (this.fieldType != null) {
                this.rightFieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.fieldIndexInSourceRow);
                this.leftFieldGetter =
                        RowData.createFieldGetter(this.fieldType, this.absoluteFieldIndex);
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

        public AttributeRef(final int inputId, final int fieldIndex) {
            this.inputId = inputId;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final AttributeRef that = (AttributeRef) o;
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
                final int leftInputId,
                final int leftFieldIndex,
                final int rightInputId,
                final int rightFieldIndex) {
            this.leftInputId = leftInputId;
            this.leftFieldIndex = leftFieldIndex;
            this.rightInputId = rightInputId;
            this.rightFieldIndex = rightFieldIndex;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ConditionAttributeRef that = (ConditionAttributeRef) o;
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
