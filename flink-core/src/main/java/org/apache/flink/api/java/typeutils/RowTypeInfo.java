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
package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowComparator;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link TypeInformation} for {@link Row}.
 *
 * <p>Note: The implementations of {@link #hashCode()} and {@link #equals(Object)} do not check
 * field names because those don't matter during serialization and runtime. This might change in
 * future versions. See FLINK-14438 for more information.
 */
@PublicEvolving
public class RowTypeInfo extends TupleTypeInfoBase<Row> {

    private static final long serialVersionUID = 9158518989896601963L;

    private static final String REGEX_INT_FIELD = "[0-9]+";
    private static final String REGEX_STR_FIELD = "[\\p{L}_\\$][\\p{L}\\p{Digit}_\\$]*";
    private static final String REGEX_FIELD = REGEX_STR_FIELD + "|" + REGEX_INT_FIELD;
    private static final String REGEX_NESTED_FIELDS = "(" + REGEX_FIELD + ")(\\.(.+))?";
    private static final String REGEX_NESTED_FIELDS_WILDCARD =
            REGEX_NESTED_FIELDS
                    + "|\\"
                    + ExpressionKeys.SELECT_ALL_CHAR
                    + "|\\"
                    + ExpressionKeys.SELECT_ALL_CHAR_SCALA;

    private static final Pattern PATTERN_NESTED_FIELDS = Pattern.compile(REGEX_NESTED_FIELDS);
    private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD =
            Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);
    private static final Pattern PATTERN_INT_FIELD = Pattern.compile(REGEX_INT_FIELD);

    // --------------------------------------------------------------------------------------------

    protected final String[] fieldNames;
    /** Temporary variable for directly passing orders to comparators. */
    private boolean[] comparatorOrders = null;

    public RowTypeInfo(TypeInformation<?>... types) {
        super(Row.class, types);

        this.fieldNames = new String[types.length];

        for (int i = 0; i < types.length; i++) {
            fieldNames[i] = "f" + i;
        }
    }

    public RowTypeInfo(TypeInformation<?>[] types, String[] fieldNames) {
        super(Row.class, types);
        checkNotNull(fieldNames, "FieldNames should not be null.");
        checkArgument(
                types.length == fieldNames.length, "Number of field types and names is different.");
        checkArgument(!hasDuplicateFieldNames(fieldNames), "Field names are not unique.");

        this.fieldNames = Arrays.copyOf(fieldNames, fieldNames.length);
    }

    @Override
    public void getFlatFields(
            String fieldExpression, int offset, List<FlatFieldDescriptor> result) {
        Matcher matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression);

        if (!matcher.matches()) {
            throw new InvalidFieldReferenceException(
                    "Invalid tuple field reference \"" + fieldExpression + "\".");
        }

        String field = matcher.group(0);

        if ((field.equals(ExpressionKeys.SELECT_ALL_CHAR))
                || (field.equals(ExpressionKeys.SELECT_ALL_CHAR_SCALA))) {
            // handle select all
            int keyPosition = 0;
            for (TypeInformation<?> fType : types) {
                if (fType instanceof CompositeType) {
                    CompositeType<?> cType = (CompositeType<?>) fType;
                    cType.getFlatFields(
                            ExpressionKeys.SELECT_ALL_CHAR, offset + keyPosition, result);
                    keyPosition += cType.getTotalFields() - 1;
                } else {
                    result.add(new FlatFieldDescriptor(offset + keyPosition, fType));
                }
                keyPosition++;
            }
        } else {
            field = matcher.group(1);

            Matcher intFieldMatcher = PATTERN_INT_FIELD.matcher(field);
            int fieldIndex;
            if (intFieldMatcher.matches()) {
                // field expression is an integer
                fieldIndex = Integer.valueOf(field);
            } else {
                fieldIndex = this.getFieldIndex(field);
            }
            // fetch the field type will throw exception if the index is illegal
            TypeInformation<?> fieldType = this.getTypeAt(fieldIndex);
            // compute the offset,
            for (int i = 0; i < fieldIndex; i++) {
                offset += this.getTypeAt(i).getTotalFields();
            }

            String tail = matcher.group(3);

            if (tail == null) {
                // expression hasn't nested field
                if (fieldType instanceof CompositeType) {
                    ((CompositeType) fieldType).getFlatFields("*", offset, result);
                } else {
                    result.add(new FlatFieldDescriptor(offset, fieldType));
                }
            } else {
                // expression has nested field
                if (fieldType instanceof CompositeType) {
                    ((CompositeType) fieldType).getFlatFields(tail, offset, result);
                } else {
                    throw new InvalidFieldReferenceException(
                            "Nested field expression \""
                                    + tail
                                    + "\" not possible on atomic type "
                                    + fieldType
                                    + ".");
                }
            }
        }
    }

    @Override
    public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
        Matcher matcher = PATTERN_NESTED_FIELDS.matcher(fieldExpression);
        if (!matcher.matches()) {
            if (fieldExpression.equals(ExpressionKeys.SELECT_ALL_CHAR)
                    || fieldExpression.equals(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
                throw new InvalidFieldReferenceException(
                        "Wildcard expressions are not allowed here.");
            } else {
                throw new InvalidFieldReferenceException(
                        "Invalid format of Row field expression \"" + fieldExpression + "\".");
            }
        }

        String field = matcher.group(1);

        Matcher intFieldMatcher = PATTERN_INT_FIELD.matcher(field);
        int fieldIndex;
        if (intFieldMatcher.matches()) {
            // field expression is an integer
            fieldIndex = Integer.valueOf(field);
        } else {
            fieldIndex = this.getFieldIndex(field);
        }
        // fetch the field type will throw exception if the index is illegal
        TypeInformation<X> fieldType = this.getTypeAt(fieldIndex);

        String tail = matcher.group(3);
        if (tail == null) {
            // found the type
            return fieldType;
        } else {
            if (fieldType instanceof CompositeType) {
                return ((CompositeType<?>) fieldType).getTypeAt(tail);
            } else {
                throw new InvalidFieldReferenceException(
                        "Nested field expression \""
                                + tail
                                + "\" not possible on atomic type "
                                + fieldType
                                + ".");
            }
        }
    }

    @Override
    public TypeComparator<Row> createComparator(
            int[] logicalKeyFields,
            boolean[] orders,
            int logicalFieldOffset,
            ExecutionConfig config) {

        comparatorOrders = orders;
        TypeComparator<Row> comparator =
                super.createComparator(logicalKeyFields, orders, logicalFieldOffset, config);
        comparatorOrders = null;
        return comparator;
    }

    @Override
    protected TypeComparatorBuilder<Row> createTypeComparatorBuilder() {
        if (comparatorOrders == null) {
            throw new IllegalStateException("Cannot create comparator builder without orders.");
        }
        return new RowTypeComparatorBuilder(comparatorOrders);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public int getFieldIndex(String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public TypeSerializer<Row> createSerializer(ExecutionConfig config) {
        int len = getArity();
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer[len];
        for (int i = 0; i < len; i++) {
            fieldSerializers[i] = types[i].createSerializer(config);
        }
        final LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            positionByName.put(fieldNames[i], i);
        }
        return new RowSerializer(fieldSerializers, positionByName);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof RowTypeInfo;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode();
    }

    /**
     * The equals method does only check for field types. Field names do not matter during runtime
     * so we can consider rows with the same field types as equal. Use {@link
     * RowTypeInfo#schemaEquals(Object)} for checking schema-equivalence.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowTypeInfo) {
            final RowTypeInfo other = (RowTypeInfo) obj;
            return other.canEqual(this) && super.equals(other);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder("Row");
        if (types.length > 0) {
            bld.append('(').append(fieldNames[0]).append(": ").append(types[0]);

            for (int i = 1; i < types.length; i++) {
                bld.append(", ").append(fieldNames[i]).append(": ").append(types[i]);
            }

            bld.append(')');
        }
        return bld.toString();
    }

    /**
     * Creates a serializer for the old {@link Row} format before Flink 1.11.
     *
     * <p>The serialization format has changed from 1.10 to 1.11 and added {@link Row#getKind()}.
     */
    @Deprecated
    public TypeSerializer<Row> createLegacySerializer(ExecutionConfig config) {
        int len = getArity();
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer[len];
        for (int i = 0; i < len; i++) {
            fieldSerializers[i] = types[i].createSerializer(config);
        }
        return new RowSerializer(fieldSerializers, null, true);
    }

    /** Tests whether an other object describes the same, schema-equivalent row information. */
    public boolean schemaEquals(Object obj) {
        return equals(obj) && Arrays.equals(fieldNames, ((RowTypeInfo) obj).fieldNames);
    }

    private boolean hasDuplicateFieldNames(String[] fieldNames) {
        HashSet<String> names = new HashSet<>();
        for (String field : fieldNames) {
            if (!names.add(field)) {
                return true;
            }
        }
        return false;
    }

    private class RowTypeComparatorBuilder implements TypeComparatorBuilder<Row> {

        private final ArrayList<TypeComparator> fieldComparators = new ArrayList<TypeComparator>();
        private final ArrayList<Integer> logicalKeyFields = new ArrayList<Integer>();
        private final boolean[] comparatorOrders;

        public RowTypeComparatorBuilder(boolean[] comparatorOrders) {
            this.comparatorOrders = comparatorOrders;
        }

        @Override
        public void initializeTypeComparatorBuilder(int size) {
            fieldComparators.ensureCapacity(size);
            logicalKeyFields.ensureCapacity(size);
        }

        @Override
        public void addComparatorField(int fieldId, TypeComparator<?> comparator) {
            fieldComparators.add(comparator);
            logicalKeyFields.add(fieldId);
        }

        @Override
        public TypeComparator<Row> createTypeComparator(ExecutionConfig config) {
            checkState(
                    fieldComparators.size() > 0,
                    "No field comparators were defined for the TupleTypeComparatorBuilder.");

            checkState(
                    logicalKeyFields.size() > 0,
                    "No key fields were defined for the TupleTypeComparatorBuilder.");

            checkState(
                    fieldComparators.size() == logicalKeyFields.size(),
                    "The number of field comparators and key fields is not equal.");

            final int maxKey = Collections.max(logicalKeyFields);

            checkState(maxKey >= 0, "The maximum key field must be greater or equal than 0.");

            TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1];

            for (int i = 0; i <= maxKey; i++) {
                fieldSerializers[i] = types[i].createSerializer(config);
            }

            int[] keyPositions = new int[logicalKeyFields.size()];
            for (int i = 0; i < keyPositions.length; i++) {
                keyPositions[i] = logicalKeyFields.get(i);
            }

            TypeComparator[] comparators = new TypeComparator[fieldComparators.size()];
            for (int i = 0; i < fieldComparators.size(); i++) {
                comparators[i] = fieldComparators.get(i);
            }

            //noinspection unchecked
            return new RowComparator(
                    getArity(),
                    keyPositions,
                    comparators,
                    (TypeSerializer<Object>[]) fieldSerializers,
                    comparatorOrders);
        }
    }

    /**
     * Creates a {@link RowTypeInfo} with projected fields.
     *
     * @param rowType The original RowTypeInfo whose fields are projected
     * @param fieldMapping The field mapping of the projection
     * @return A RowTypeInfo with projected fields.
     */
    public static RowTypeInfo projectFields(RowTypeInfo rowType, int[] fieldMapping) {
        TypeInformation[] fieldTypes = new TypeInformation[fieldMapping.length];
        String[] fieldNames = new String[fieldMapping.length];
        for (int i = 0; i < fieldMapping.length; i++) {
            fieldTypes[i] = rowType.getTypeAt(fieldMapping[i]);
            fieldNames[i] = rowType.getFieldNames()[fieldMapping[i]];
        }
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
