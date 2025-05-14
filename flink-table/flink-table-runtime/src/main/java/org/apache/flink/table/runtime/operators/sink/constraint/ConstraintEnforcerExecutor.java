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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NestedEnforcer;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.api.config.ExecutionConfigOptions.TypeLengthEnforcer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Logic extracted from {@link ConstraintEnforcer} in order to use it outside of {@link
 * TableStreamOperator}.
 */
@Internal
public class ConstraintEnforcerExecutor implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Constraint[] constraints;

    private ConstraintEnforcerExecutor(Constraint[] constraints) {
        this.constraints = constraints;
    }

    public Constraint[] getConstraints() {
        return constraints;
    }

    public static Optional<ConstraintEnforcerExecutor> create(
            final RowType physicalType,
            final NotNullEnforcer notNullEnforcer,
            final TypeLengthEnforcer typeLengthEnforcer,
            final NestedEnforcer nestedConstraints) {
        final Constraint[] topLevelConstraints =
                createConstraints(
                        physicalType, notNullEnforcer, typeLengthEnforcer, nestedConstraints);

        return create(topLevelConstraints);
    }

    private static Constraint[] createConstraints(
            final RowType physicalType,
            final NotNullEnforcer notNullEnforcer,
            final TypeLengthEnforcer typeLengthEnforcer,
            final NestedEnforcer nestedEnforcer) {
        final String[] fieldNames = physicalType.getFieldNames().toArray(new String[0]);
        final List<Constraint> constraints = new ArrayList<>();

        // Build NOT NULL enforcer
        final int[] notNullFieldIndices = getNotNullFieldIndices(physicalType);
        if (notNullFieldIndices.length > 0) {
            final String[] notNullFieldNames =
                    Arrays.stream(notNullFieldIndices)
                            .mapToObj(idx -> fieldNames[idx])
                            .toArray(String[]::new);

            constraints.add(
                    new NotNullConstraint(
                            NotNullEnforcementStrategy.of(notNullEnforcer),
                            notNullFieldIndices,
                            notNullFieldNames));
        }

        if (typeLengthEnforcer != TypeLengthEnforcer.IGNORE) {
            // Build CHAR/VARCHAR length enforcer
            addLengthEnforcers(physicalType, typeLengthEnforcer, fieldNames, constraints);
        }

        // Build nested row constraints
        if (nestedEnforcer != NestedEnforcer.IGNORE) {
            addNestedConstraints(
                    physicalType,
                    notNullEnforcer,
                    typeLengthEnforcer,
                    nestedEnforcer,
                    fieldNames,
                    constraints);
        }

        return constraints.toArray(Constraint[]::new);
    }

    private static void addNestedConstraints(
            final RowType physicalType,
            final NotNullEnforcer notNullEnforcer,
            final TypeLengthEnforcer typeLengthEnforcer,
            final NestedEnforcer nestedEnforcer,
            final String[] fieldNames,
            final List<Constraint> constraints) {
        final List<NestedRowInfo> nestedRowInfo = getNestedRowInfos(physicalType);
        if (!nestedRowInfo.isEmpty()) {
            final String[] nestedRowFieldNames =
                    nestedRowInfo.stream()
                            .map(r -> fieldNames[r.getFieldIdx()])
                            .toArray(String[]::new);

            final int[] nestedRowFieldIndices =
                    nestedRowInfo.stream().mapToInt(NestedRowInfo::getFieldIdx).toArray();

            final int[] nestedRowFieldArities =
                    nestedRowInfo.stream().mapToInt(NestedRowInfo::getArity).toArray();

            final Constraint[][] nestedConstraints =
                    nestedRowInfo.stream()
                            .map(
                                    r ->
                                            createConstraints(
                                                    r.getFieldType(),
                                                    notNullEnforcer,
                                                    typeLengthEnforcer,
                                                    nestedEnforcer))
                            .toArray(Constraint[][]::new);

            constraints.add(
                    new NestedRowConstraint(
                            nestedRowFieldIndices,
                            nestedRowFieldArities,
                            nestedRowFieldNames,
                            nestedConstraints));
        }

        if (nestedEnforcer == NestedEnforcer.ROWS) {
            return;
        }

        if (typeLengthEnforcer == TypeLengthEnforcer.TRIM_PAD) {
            throw new ValidationException(
                    "Trimming and/or padding is not supported if constraint checking is enabled on"
                            + " types nested in collections.");
        }

        // add nested array constraints
        final List<NestedArrayInfo> nestedArrayInfos = getNestedArrayInfos(physicalType);
        if (!nestedArrayInfos.isEmpty()) {
            final String[] nestedRowFieldNames =
                    nestedArrayInfos.stream()
                            .map(r -> fieldNames[r.getFieldIdx()])
                            .toArray(String[]::new);

            final int[] nestedRowFieldIndices =
                    nestedArrayInfos.stream().mapToInt(NestedArrayInfo::getFieldIdx).toArray();

            final Constraint[][] elementConstraints =
                    nestedArrayInfos.stream()
                            .map(
                                    r1 ->
                                            createConstraints(
                                                    new RowType(
                                                            List.of(
                                                                    new RowType.RowField(
                                                                            "element",
                                                                            r1.getElementType()))),
                                                    notNullEnforcer,
                                                    typeLengthEnforcer,
                                                    nestedEnforcer))
                            .toArray(Constraint[][]::new);

            final ArrayData.ElementGetter[] elementGetters =
                    nestedArrayInfos.stream()
                            .map(r -> ArrayData.createElementGetter(r.getElementType()))
                            .toArray(ArrayData.ElementGetter[]::new);

            constraints.add(
                    new NestedArrayConstraint(
                            nestedRowFieldIndices,
                            nestedRowFieldNames,
                            elementConstraints,
                            elementGetters));
        }

        // add nested map constraints
        final List<NestedMapInfo> nestedMapInfos = getNestedMapInfos(physicalType);
        if (!nestedMapInfos.isEmpty()) {
            final String[] nestedRowFieldNames =
                    nestedMapInfos.stream()
                            .map(r -> fieldNames[r.getFieldIdx()])
                            .toArray(String[]::new);

            final int[] nestedRowFieldIndices =
                    nestedMapInfos.stream().mapToInt(NestedMapInfo::getFieldIdx).toArray();

            final Constraint[][] elementConstraints =
                    nestedMapInfos.stream()
                            .map(
                                    r1 ->
                                            createConstraints(
                                                    new RowType(
                                                            List.of(
                                                                    new RowType.RowField(
                                                                            "key", r1.getKeyType()),
                                                                    new RowType.RowField(
                                                                            "value",
                                                                            r1.getValueType()))),
                                                    notNullEnforcer,
                                                    typeLengthEnforcer,
                                                    nestedEnforcer))
                            .toArray(Constraint[][]::new);

            final ArrayData.ElementGetter[] keyGetters =
                    nestedMapInfos.stream()
                            .map(r -> ArrayData.createElementGetter(r.getKeyType()))
                            .toArray(ArrayData.ElementGetter[]::new);

            final ArrayData.ElementGetter[] valueGetters =
                    nestedMapInfos.stream()
                            .map(r -> ArrayData.createElementGetter(r.getValueType()))
                            .toArray(ArrayData.ElementGetter[]::new);

            constraints.add(
                    new NestedMapConstraint(
                            nestedRowFieldIndices,
                            nestedRowFieldNames,
                            elementConstraints,
                            keyGetters,
                            valueGetters));
        }
    }

    private static void addLengthEnforcers(
            final RowType physicalType,
            final TypeLengthEnforcer typeLengthEnforcer,
            final String[] fieldNames,
            final List<Constraint> constraints) {
        final List<FieldInfo> charFieldInfo =
                getFieldInfoForLengthEnforcer(physicalType, LengthEnforcerKind.CHAR);
        if (!charFieldInfo.isEmpty()) {
            final String[] charFieldNames =
                    charFieldInfo.stream()
                            .map(cfi -> fieldNames[cfi.fieldIdx()])
                            .toArray(String[]::new);

            final int[] charFieldIndices =
                    charFieldInfo.stream().mapToInt(FieldInfo::fieldIdx).toArray();

            final int[] charFieldLengths =
                    charFieldInfo.stream().mapToInt(FieldInfo::getLength).toArray();

            constraints.add(
                    new CharLengthConstraint(
                            TypeLengthEnforcementStrategy.of(typeLengthEnforcer),
                            charFieldIndices,
                            charFieldLengths,
                            charFieldNames,
                            buildCouldPad(charFieldInfo)));
        }

        // Build BINARY/VARBINARY length enforcer
        final List<FieldInfo> binaryFieldInfo =
                getFieldInfoForLengthEnforcer(physicalType, LengthEnforcerKind.BINARY);
        if (!binaryFieldInfo.isEmpty()) {
            final String[] binaryFieldNames =
                    binaryFieldInfo.stream()
                            .map(cfi -> fieldNames[cfi.fieldIdx()])
                            .toArray(String[]::new);

            final int[] binaryFieldIndices =
                    binaryFieldInfo.stream().mapToInt(FieldInfo::fieldIdx).toArray();

            final int[] binaryFieldLengths =
                    binaryFieldInfo.stream().mapToInt(FieldInfo::getLength).toArray();

            constraints.add(
                    new BinaryLengthConstraint(
                            TypeLengthEnforcementStrategy.of(typeLengthEnforcer),
                            binaryFieldIndices,
                            binaryFieldLengths,
                            binaryFieldNames,
                            buildCouldPad(binaryFieldInfo)));
        }
    }

    private static Optional<ConstraintEnforcerExecutor> create(final Constraint[] constraints) {
        if (constraints.length == 0) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintEnforcerExecutor(constraints));
    }

    private static int[] getNotNullFieldIndices(final RowType physicalType) {
        return IntStream.range(0, physicalType.getFieldCount())
                .filter(pos -> !physicalType.getTypeAt(pos).isNullable())
                .toArray();
    }

    private static List<NestedRowInfo> getNestedRowInfos(final RowType physicalType) {
        return IntStream.range(0, physicalType.getFieldCount())
                .boxed()
                .flatMap(
                        pos -> {
                            final LogicalType nestedType = physicalType.getTypeAt(pos);
                            if (nestedType.is(LogicalTypeRoot.ROW)) {
                                final RowType rowType = (RowType) nestedType;
                                return Stream.of(
                                        new NestedRowInfo(pos, (rowType).getFieldCount(), rowType));
                            } else if (nestedType.is(LogicalTypeRoot.STRUCTURED_TYPE)) {
                                // for constraint extraction, convert the STRUCTURED_TYPE to a ROW
                                final StructuredType structuredType = (StructuredType) nestedType;
                                final List<StructuredType.StructuredAttribute> attributes =
                                        structuredType.getAttributes();
                                final List<RowType.RowField> fields =
                                        attributes.stream()
                                                .map(
                                                        attr ->
                                                                new RowType.RowField(
                                                                        attr.getName(),
                                                                        attr.getType()))
                                                .collect(Collectors.toList());
                                return Stream.of(
                                        new NestedRowInfo(
                                                pos,
                                                attributes.size(),
                                                new RowType(structuredType.isNullable(), fields)));
                            } else {
                                return Stream.empty();
                            }
                        })
                .collect(Collectors.toList());
    }

    private static List<NestedArrayInfo> getNestedArrayInfos(final RowType physicalType) {
        return IntStream.range(0, physicalType.getFieldCount())
                .boxed()
                .flatMap(
                        pos -> {
                            final LogicalType nestedType = physicalType.getTypeAt(pos);
                            if (nestedType.is(LogicalTypeRoot.ARRAY)) {
                                final ArrayType arrayType = (ArrayType) nestedType;
                                final LogicalType elementType = arrayType.getElementType();

                                return Stream.of(new NestedArrayInfo(pos, elementType));
                            } else {
                                return Stream.empty();
                            }
                        })
                .collect(Collectors.toList());
    }

    private static List<NestedMapInfo> getNestedMapInfos(final RowType physicalType) {
        return IntStream.range(0, physicalType.getFieldCount())
                .boxed()
                .flatMap(
                        pos -> {
                            final LogicalType nestedType = physicalType.getTypeAt(pos);
                            if (nestedType.is(LogicalTypeRoot.MAP)) {
                                final MapType mapType = (MapType) nestedType;

                                return Stream.of(
                                        new NestedMapInfo(
                                                pos, mapType.getKeyType(), mapType.getValueType()));
                            } else {
                                return Stream.empty();
                            }
                        })
                .collect(Collectors.toList());
    }

    /**
     * Returns a List of {@link FieldInfo}, each containing the info needed to determine whether a
     * string or binary value needs trimming and/or padding.
     */
    private static List<FieldInfo> getFieldInfoForLengthEnforcer(
            final RowType physicalType, final LengthEnforcerKind enforcerType) {
        LogicalTypeRoot staticType = null;
        LogicalTypeRoot variableType = null;
        int maxLength = 0;
        switch (enforcerType) {
            case CHAR:
                staticType = LogicalTypeRoot.CHAR;
                variableType = LogicalTypeRoot.VARCHAR;
                maxLength = CharType.MAX_LENGTH;
                break;
            case BINARY:
                staticType = LogicalTypeRoot.BINARY;
                variableType = LogicalTypeRoot.VARBINARY;
                maxLength = BinaryType.MAX_LENGTH;
        }
        final List<FieldInfo> fieldsAndLengths = new ArrayList<>();
        for (int i = 0; i < physicalType.getFieldCount(); i++) {
            LogicalType type = physicalType.getTypeAt(i);
            boolean isStatic = type.is(staticType);
            // Should trim and possibly pad
            if ((isStatic && (LogicalTypeChecks.getLength(type) < maxLength))
                    || (type.is(variableType) && (LogicalTypeChecks.getLength(type) < maxLength))) {
                fieldsAndLengths.add(new FieldInfo(i, LogicalTypeChecks.getLength(type), isStatic));
            } else if (isStatic) { // Should pad
                fieldsAndLengths.add(new FieldInfo(i, -1, isStatic));
            }
        }
        return fieldsAndLengths;
    }

    private static BitSet buildCouldPad(final List<FieldInfo> charFieldInfo) {
        BitSet couldPad = new BitSet(charFieldInfo.size());
        for (int i = 0; i < charFieldInfo.size(); i++) {
            if (charFieldInfo.get(i).couldPad()) {
                couldPad.set(i);
            }
        }
        return couldPad;
    }

    public @Nullable RowData enforce(final RowData row) {
        RowData enforcedRow = row;
        for (Constraint constraint : constraints) {
            enforcedRow = constraint.enforce(enforcedRow);
            if (enforcedRow == null) {
                return null; // drop row
            }
        }
        return enforcedRow;
    }

    private static class NestedRowInfo {
        private final int fieldIdx;
        private final int arity;
        private final RowType fieldType;

        public NestedRowInfo(int fieldIdx, int arity, RowType fieldType) {
            this.fieldIdx = fieldIdx;
            this.arity = arity;
            this.fieldType = fieldType;
        }

        public int getFieldIdx() {
            return fieldIdx;
        }

        public int getArity() {
            return arity;
        }

        public RowType getFieldType() {
            return fieldType;
        }
    }

    private static class NestedArrayInfo {
        private final int fieldIdx;
        private final LogicalType elementType;

        private NestedArrayInfo(final int fieldIdx, final LogicalType elementType) {
            this.fieldIdx = fieldIdx;
            this.elementType = elementType;
        }

        public int getFieldIdx() {
            return fieldIdx;
        }

        public LogicalType getElementType() {
            return elementType;
        }
    }

    private static class NestedMapInfo {
        private final int fieldIdx;
        private final LogicalType valueType;
        private final LogicalType keyType;

        private NestedMapInfo(final int fieldIdx, LogicalType keyType, LogicalType valueType) {
            this.fieldIdx = fieldIdx;
            this.valueType = valueType;
            this.keyType = keyType;
        }

        public int getFieldIdx() {
            return fieldIdx;
        }

        public LogicalType getValueType() {
            return valueType;
        }

        public LogicalType getKeyType() {
            return keyType;
        }
    }

    /**
     * Helper POJO to keep info about CHAR/VARCHAR/BINARY/VARBINARY fields, used to determine if
     * trimming or padding is needed.
     */
    @Internal
    private static class FieldInfo {
        private final int fieldIdx;
        private final int length;
        private final boolean couldPad;

        public FieldInfo(int fieldIdx, int length, boolean couldPad) {
            this.fieldIdx = fieldIdx;
            this.length = length;
            this.couldPad = couldPad;
        }

        public int fieldIdx() {
            return fieldIdx;
        }

        public boolean couldPad() {
            return couldPad;
        }

        public int getLength() {
            return length;
        }
    }

    enum LengthEnforcerKind {
        CHAR,
        BINARY
    }
}
