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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;

/**
 * {@link Projection} represents a list of (possibly nested) indexes that can be used to project
 * data types. A row projection includes both reducing the accessible fields and reordering them.
 */
@PublicEvolving
public abstract class Projection {

    // sealed class
    private Projection() {}

    /**
     * Projects a (possibly nested) row data type by returning a new data type that only includes
     * fields of the given index paths.
     *
     * <p>When extracting nested fields, the name of the resulting fields is the full path of the
     * field separated by {@code _}. For example, the field {@code b} inside the row field {@code a}
     * of the root {@link DataType} is named {@code a_b} in the result {@link DataType}. In case of
     * naming conflicts the postfix notation '_$%d' is used, where {@code %d} is an arbitrary
     * number, in order to generate a unique field name. For example if the root {@link DataType}
     * includes both a field {@code a_b} and a nested row {@code a} with field {@code b}, the result
     * {@link DataType} will contain one field named {@code a_b} and the other named {@code a_b_1}.
     */
    public abstract DataType project(DataType dataType);

    /** Same as {@link #project(DataType)}, but accepting and returning {@link LogicalType}. */
    public LogicalType project(LogicalType logicalType) {
        return this.project(DataTypes.of(logicalType)).getLogicalType();
    }

    /** @return {@code true} whether this projection is nested or not. */
    public abstract boolean isNested();

    /**
     * Perform a difference of this {@link Projection} with another {@link Projection}. The result
     * of this operation is a new {@link Projection} retaining the same ordering of this instance
     * but with the indexes from {@code other} removed. For example:
     *
     * <pre>
     * <code>
     * [4, 1, 0, 3, 2] - [4, 2] = [1, 0, 2]
     * </code>
     * </pre>
     *
     * <p>Note how the index {@code 3} in the minuend becomes {@code 2} because it's rescaled to
     * project correctly a {@link RowData} or arity 3.
     *
     * @param other the subtrahend
     * @throws IllegalArgumentException when {@code other} is nested.
     */
    public abstract Projection difference(Projection other);

    /**
     * Complement this projection. The returned projection is an ordered projection of fields from 0
     * to {@code fieldsNumber} except the indexes in this {@link Projection}. For example:
     *
     * <pre>
     * <code>
     * [4, 2].complement(5) = [0, 1, 3]
     * </code>
     * </pre>
     *
     * @param fieldsNumber the size of the universe
     * @throws IllegalStateException if this projection is nested.
     */
    public abstract Projection complement(int fieldsNumber);

    /** Like {@link #complement(int)}, using the {@code dataType} fields count. */
    public Projection complement(DataType dataType) {
        return complement(DataType.getFieldCount(dataType));
    }

    /**
     * Convert this instance to a projection of top level indexes. The array represents the mapping
     * of the fields of the original {@link DataType}. For example, {@code [0, 2, 1]} specifies to
     * include in the following order the 1st field, the 3rd field and the 2nd field of the row.
     *
     * @throws IllegalStateException if this projection is nested.
     */
    public abstract int[] toTopLevelIndexes();

    /**
     * Convert this instance to a nested projection index paths. The array represents the mapping of
     * the fields of the original {@link DataType}, including nested rows. For example, {@code [[0,
     * 2, 1], ...]} specifies to include the 2nd field of the 3rd field of the 1st field in the
     * top-level row.
     */
    public abstract int[][] toNestedIndexes();

    /**
     * Create an empty {@link Projection}, that is a projection that projects no fields, returning
     * an empty {@link DataType}.
     */
    public static Projection empty() {
        return EmptyProjection.INSTANCE;
    }

    /**
     * Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toTopLevelIndexes()
     */
    public static Projection of(int[] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new TopLevelProjection(indexes);
    }

    /**
     * Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toNestedIndexes()
     */
    public static Projection of(int[][] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new NestedProjection(indexes);
    }

    /**
     * Create a {@link Projection} of the provided {@code dataType} using the provided {@code
     * projectedFields}.
     */
    public static Projection fromFieldNames(DataType dataType, List<String> projectedFields) {
        List<String> dataTypeFieldNames = DataType.getFieldNames(dataType);
        return new TopLevelProjection(
                projectedFields.stream().mapToInt(dataTypeFieldNames::indexOf).toArray());
    }

    /** Create a {@link Projection} of all the fields in the provided {@code dataType}. */
    public static Projection all(DataType dataType) {
        return new TopLevelProjection(
                IntStream.range(0, DataType.getFieldCount(dataType)).toArray());
    }

    /** Create a {@link Projection} of a field range. */
    public static Projection range(int startInclusive, int endExclusive) {
        return new TopLevelProjection(IntStream.range(startInclusive, endExclusive).toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Projection)) {
            return false;
        }
        Projection other = (Projection) o;
        if (!this.isNested() && !other.isNested()) {
            return Arrays.equals(this.toTopLevelIndexes(), other.toTopLevelIndexes());
        }
        return Arrays.deepEquals(this.toNestedIndexes(), other.toNestedIndexes());
    }

    @Override
    public int hashCode() {
        if (isNested()) {
            return Arrays.deepHashCode(toNestedIndexes());
        }
        return Arrays.hashCode(toTopLevelIndexes());
    }

    @Override
    public String toString() {
        if (isNested()) {
            return "Nested projection = " + Arrays.deepToString(toNestedIndexes());
        }
        return "Top level projection = " + Arrays.toString(toTopLevelIndexes());
    }

    private static class EmptyProjection extends Projection {

        static final EmptyProjection INSTANCE = new EmptyProjection();

        private EmptyProjection() {}

        @Override
        public DataType project(DataType dataType) {
            return new NestedProjection(toNestedIndexes()).project(dataType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection projection) {
            return this;
        }

        @Override
        public Projection complement(int fieldsNumber) {
            return new TopLevelProjection(IntStream.range(0, fieldsNumber).toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            return new int[0];
        }

        @Override
        public int[][] toNestedIndexes() {
            return new int[0][];
        }
    }

    private static class NestedProjection extends Projection {

        final int[][] projection;
        final boolean nested;

        NestedProjection(int[][] projection) {
            this.projection = projection;
            this.nested = Arrays.stream(projection).anyMatch(arr -> arr.length > 1);
        }

        @Override
        public DataType project(DataType dataType) {
            final List<RowType.RowField> updatedFields = new ArrayList<>();
            final List<DataType> updatedChildren = new ArrayList<>();
            Set<String> nameDomain = new HashSet<>();
            int duplicateCount = 0;
            for (int[] indexPath : this.projection) {
                DataType fieldType = dataType.getChildren().get(indexPath[0]);
                LogicalType fieldLogicalType = fieldType.getLogicalType();
                StringBuilder builder =
                        new StringBuilder(
                                ((RowType) dataType.getLogicalType())
                                        .getFieldNames()
                                        .get(indexPath[0]));
                for (int index = 1; index < indexPath.length; index++) {
                    Preconditions.checkArgument(
                            fieldLogicalType.is(ROW), "Row data type expected.");
                    RowType rowtype = ((RowType) fieldLogicalType);
                    builder.append("_").append(rowtype.getFieldNames().get(indexPath[index]));
                    fieldLogicalType = rowtype.getFields().get(indexPath[index]).getType();
                    fieldType = fieldType.getChildren().get(indexPath[index]);
                }
                String path = builder.toString();
                while (nameDomain.contains(path)) {
                    path = builder.append("_$").append(duplicateCount++).toString();
                }
                updatedFields.add(new RowType.RowField(path, fieldLogicalType));
                updatedChildren.add(fieldType);
                nameDomain.add(path);
            }
            return new FieldsDataType(
                    new RowType(dataType.getLogicalType().isNullable(), updatedFields),
                    dataType.getConversionClass(),
                    updatedChildren);
        }

        @Override
        public boolean isNested() {
            return nested;
        }

        @Override
        public Projection difference(Projection other) {
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between nested projection and nested projection");
            }
            if (other instanceof EmptyProjection) {
                return this;
            }
            if (!this.isNested()) {
                return new TopLevelProjection(toTopLevelIndexes()).difference(other);
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<int[]> resultProjection =
                    Arrays.stream(projection).collect(Collectors.toCollection(ArrayList::new));

            ListIterator<int[]> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int[] indexArr = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                int searchResult = Arrays.binarySearch(indexesToExclude, indexArr[0]);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1);
                    if (offset != 0) {
                        indexArr[0] = indexArr[0] - offset;
                    }
                }
            }

            return new NestedProjection(resultProjection.toArray(new int[0][]));
        }

        @Override
        public Projection complement(int fieldsNumber) {
            if (isNested()) {
                throw new IllegalStateException("Cannot perform complement of a nested projection");
            }
            return new TopLevelProjection(toTopLevelIndexes()).complement(fieldsNumber);
        }

        @Override
        public int[] toTopLevelIndexes() {
            if (isNested()) {
                throw new IllegalStateException(
                        "Cannot convert a nested projection to a top level projection");
            }
            return Arrays.stream(projection).mapToInt(arr -> arr[0]).toArray();
        }

        @Override
        public int[][] toNestedIndexes() {
            return projection;
        }
    }

    private static class TopLevelProjection extends Projection {

        final int[] projection;

        TopLevelProjection(int[] projection) {
            this.projection = projection;
        }

        @Override
        public DataType project(DataType dataType) {
            return new NestedProjection(toNestedIndexes()).project(dataType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection other) {
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between top level projection and nested projection");
            }
            if (other instanceof EmptyProjection) {
                return this;
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<Integer> resultProjection =
                    Arrays.stream(projection)
                            .boxed()
                            .collect(Collectors.toCollection(ArrayList::new));

            ListIterator<Integer> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int index = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                int searchResult = Arrays.binarySearch(indexesToExclude, index);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1);
                    if (offset != 0) {
                        resultProjectionIterator.set(index - offset);
                    }
                }
            }

            return new TopLevelProjection(resultProjection.stream().mapToInt(i -> i).toArray());
        }

        @Override
        public Projection complement(int fieldsNumber) {
            int[] indexesToExclude = Arrays.copyOf(projection, projection.length);
            Arrays.sort(indexesToExclude);

            return new TopLevelProjection(
                    IntStream.range(0, fieldsNumber)
                            .filter(i -> Arrays.binarySearch(indexesToExclude, i) < 0)
                            .toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            return projection;
        }

        @Override
        public int[][] toNestedIndexes() {
            return Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        }
    }
}
