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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link SortSpec} describes how the data will be sorted. */
public class SortSpec {

    public static final String FIELD_NAME_FIELDS = "fields";

    /** SortSpec does not require sort. */
    public static final SortSpec ANY = new SortSpec(new SortFieldSpec[0]);

    @JsonProperty(FIELD_NAME_FIELDS)
    private final SortFieldSpec[] fieldSpecs;

    @JsonCreator
    public SortSpec(@JsonProperty(FIELD_NAME_FIELDS) SortFieldSpec[] fieldSpecs) {
        this.fieldSpecs = checkNotNull(fieldSpecs);
    }

    /** Creates a sub SortSpec starting from startIndex sort field. */
    public SortSpec createSubSortSpec(int startIndex) {
        Preconditions.checkArgument(startIndex >= 0 && startIndex < fieldSpecs.length);
        return new SortSpec(
                Arrays.stream(fieldSpecs, startIndex, fieldSpecs.length)
                        .toArray(SortFieldSpec[]::new));
    }

    /** Gets field index of all fields in input. */
    @JsonIgnore
    public int[] getFieldIndices() {
        return Arrays.stream(fieldSpecs).mapToInt(SortFieldSpec::getFieldIndex).toArray();
    }

    /** Gets flags of all fields for whether to sort in ascending order or not. */
    @JsonIgnore
    public boolean[] getAscendingOrders() {
        boolean[] orders = new boolean[fieldSpecs.length];
        IntStream.range(0, fieldSpecs.length)
                .forEach(i -> orders[i] = fieldSpecs[i].isAscendingOrder);
        return orders;
    }

    /** Gets flags of all fields for whether to put null at last or not. */
    @JsonIgnore
    public boolean[] getNullsIsLast() {
        boolean[] nullIsLasts = new boolean[fieldSpecs.length];
        IntStream.range(0, fieldSpecs.length)
                .forEach(i -> nullIsLasts[i] = fieldSpecs[i].nullIsLast);
        return nullIsLasts;
    }

    /** Gets all {@link SortFieldSpec} in the SortSpec. */
    @JsonIgnore
    public SortFieldSpec[] getFieldSpecs() {
        return fieldSpecs;
    }

    /** Gets {@link SortFieldSpec} of field at given index. */
    public SortFieldSpec getFieldSpec(int index) {
        return fieldSpecs[index];
    }

    /** Gets num of field in the spec. */
    @JsonIgnore
    public int getFieldSize() {
        return fieldSpecs.length;
    }

    /** Gets fields types of sort fields accoording to input type. */
    public LogicalType[] getFieldTypes(RowType input) {
        return Stream.of(fieldSpecs)
                .map(field -> input.getTypeAt(field.fieldIndex))
                .toArray(LogicalType[]::new);
    }

    public static SortSpecBuilder builder() {
        return new SortSpecBuilder();
    }

    /** SortSpec builder. */
    public static class SortSpecBuilder {

        private final List<SortFieldSpec> fieldSpecs = new LinkedList<>();

        public SortSpecBuilder addField(
                int fieldIndex, boolean isAscendingOrder, boolean nullIsLast) {
            fieldSpecs.add(new SortFieldSpec(fieldIndex, isAscendingOrder, nullIsLast));
            return this;
        }

        public SortSpec build() {
            return new SortSpec(fieldSpecs.toArray(new SortFieldSpec[0]));
        }
    }

    /** Sort info for a Field. */
    public static class SortFieldSpec {
        public static final String FIELD_NAME_INDEX = "index";
        public static final String FIELD_NAME_IS_ASCENDING = "isAscending";
        public static final String FIELD_NAME_NULL_IS_LAST = "nullIsLast";

        /** 0-based index of field being sorted. */
        @JsonProperty(FIELD_NAME_INDEX)
        private final int fieldIndex;
        /** in ascending order or not. */
        @JsonProperty(FIELD_NAME_IS_ASCENDING)
        private final boolean isAscendingOrder;
        /** put null at last or not. */
        @JsonProperty(FIELD_NAME_NULL_IS_LAST)
        private final boolean nullIsLast;

        @JsonCreator
        public SortFieldSpec(
                @JsonProperty(FIELD_NAME_INDEX) int fieldIndex,
                @JsonProperty(FIELD_NAME_IS_ASCENDING) boolean isAscendingOrder,
                @JsonProperty(FIELD_NAME_NULL_IS_LAST) boolean nullIsLast) {
            this.fieldIndex = fieldIndex;
            this.isAscendingOrder = isAscendingOrder;
            this.nullIsLast = nullIsLast;
        }

        @JsonIgnore
        public int getFieldIndex() {
            return fieldIndex;
        }

        @JsonIgnore
        public boolean getIsAscendingOrder() {
            return isAscendingOrder;
        }

        @JsonIgnore
        public boolean getNullIsLast() {
            return nullIsLast;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SortFieldSpec that = (SortFieldSpec) o;
            return fieldIndex == that.fieldIndex
                    && isAscendingOrder == that.isAscendingOrder
                    && nullIsLast == that.nullIsLast;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldIndex, isAscendingOrder, nullIsLast);
        }

        @Override
        public String toString() {
            return "SortField{"
                    + "fieldIndex="
                    + fieldIndex
                    + ", isAscendingOrder="
                    + isAscendingOrder
                    + ", nullIsLast="
                    + nullIsLast
                    + '}';
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SortSpec sortSpec = (SortSpec) o;
        return Arrays.equals(fieldSpecs, sortSpec.fieldSpecs);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSpecs);
    }

    @Override
    public String toString() {
        return "Sort{" + "fields=" + Arrays.toString(fieldSpecs) + '}';
    }
}
