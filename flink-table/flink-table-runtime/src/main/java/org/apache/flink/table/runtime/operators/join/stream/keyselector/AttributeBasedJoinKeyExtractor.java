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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
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
    }

    @Override
    public RowData getKeyForStateStorage(RowData row, int inputId) {
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
    public RowData getKeyForStateLookup(int depth, RowData[] currentRows) {
        if (depth == 0) {
            // Input 0 lookup always uses the fixed default key.
            return DEFAULT_KEY;
        }

        // For depths > 0, lookup key derived from *previous* rows (indices < depth)
        // using the *left* side of equi-join conditions for the *current* depth.
        final Map<AttributeRef, AttributeRef> attributeMapping = joinAttributeMap.get(depth);
        if (attributeMapping == null || attributeMapping.isEmpty()) {
            // No equi-join conditions link previous inputs to this depth (e.g. cross join).
            // Use default key.
            return DEFAULT_KEY;
        }

        // TreeMap ensures deterministic key structure: left inputId -> left fieldIndex
        final Map<Integer, Map<Integer, Object>> sortedKeyComponents = new TreeMap<>();

        // Iterate through join attributes for the current depth.
        // Key (leftAttrRef) points to previous input (< depth).
        // Value (rightAttrRef) points to current input (== depth).
        for (Map.Entry<AttributeRef, AttributeRef> entry : attributeMapping.entrySet()) {
            final AttributeRef leftAttrRef = entry.getKey();

            if (leftAttrRef.inputId >= depth) {
                // Configuration error: Left side must reference an input index < depth.
                throw new IllegalStateException(
                        "Invalid joinAttributeMap configuration for depth "
                                + depth
                                + ". Left attribute "
                                + leftAttrRef
                                + " does not reference a previous input (< "
                                + depth
                                + ").");
            }

            final RowData leftRow = currentRows[leftAttrRef.inputId];
            // Extract value. Handles null rows/fields internally (returns null value).
            final Object value = extractFieldValue(leftRow, leftAttrRef);

            // Store value, ensuring deterministic order by inputId, then fieldIndex.
            sortedKeyComponents
                    .computeIfAbsent(leftAttrRef.inputId, k -> new TreeMap<>())
                    .put(leftAttrRef.fieldIndex, value);
        }

        if (sortedKeyComponents.isEmpty()) {
            // Should not happen if attributeMapping was valid, but handle defensively.
            // Implies no valid attributes found linking previous inputs. Use default key.
            return DEFAULT_KEY;
        }

        // Assemble final key row from sorted components.
        final List<Object> keyValues = new ArrayList<>();
        for (Map.Entry<Integer, Map<Integer, Object>> inputEntry : sortedKeyComponents.entrySet()) {
            // Add values ordered by fieldIndex within each inputId
            keyValues.addAll(inputEntry.getValue().values());
        }

        final GenericRowData keyRow = new GenericRowData(keyValues.size());
        for (int i = 0; i < keyValues.size(); i++) {
            keyRow.setField(i, keyValues.get(i));
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

    /** Extracts a field value using an AttributeRef. Returns null if row or field is null. */
    private Object extractFieldValue(RowData row, AttributeRef attrRef) {
        // Handles null row input gracefully by returning null.
        if (row == null) {
            return null;
        }
        final InternalTypeInfo<RowData> typeInfo = inputTypes.get(attrRef.inputId);
        final RowType rowType = typeInfo.toRowType();
        final int fieldIndex = attrRef.fieldIndex;

        validateFieldIndex(attrRef.inputId, fieldIndex, rowType); // Validate before access

        final LogicalType fieldType = rowType.getTypeAt(fieldIndex);
        final RowData.FieldGetter fieldGetter = RowData.createFieldGetter(fieldType, fieldIndex);
        // getFieldOrNull handles null fields within the row.
        return fieldGetter.getFieldOrNull(row);
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
