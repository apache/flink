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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** SortSpec describes how the data will be sorted. */
public class SortSpec {

    /** SortSpec does not require sort. */
    public static final SortSpec ANY = new SortSpec(new SortFieldSpec[0]);

    private final SortFieldSpec[] fieldSpecs;

    public SortSpec(SortFieldSpec[] fieldSpecs) {
        this.fieldSpecs = fieldSpecs;
    }

    /** Creates a sub SortSpec starting from startIndex sort field. */
    public SortSpec createSubSortSpec(int startIndex) {
        Preconditions.checkArgument(startIndex >= 0 && startIndex < fieldSpecs.length);
        return new SortSpec(
                Arrays.stream(fieldSpecs, startIndex, fieldSpecs.length)
                        .toArray(SortFieldSpec[]::new));
    }

    /** Gets field index of all fields in input. */
    public int[] getFieldIndices() {
        return Arrays.stream(fieldSpecs).mapToInt(SortFieldSpec::getFieldIndex).toArray();
    }

    /** Gets flags of all fields for whether to sort in ascending order or not. */
    public boolean[] getAscendingOrders() {
        boolean[] orders = new boolean[fieldSpecs.length];
        IntStream.range(0, fieldSpecs.length)
                .forEach(i -> orders[i] = fieldSpecs[i].isAscendingOrder);
        return orders;
    }

    /** Gets flags of all fields for whether to put null at last or not. */
    public boolean[] getNullsIsLast() {
        boolean[] nullIsLasts = new boolean[fieldSpecs.length];
        IntStream.range(0, fieldSpecs.length)
                .forEach(i -> nullIsLasts[i] = fieldSpecs[i].nullIsLast);
        return nullIsLasts;
    }

    /** Gets {@link SortFieldSpec} of field at given index. */
    public SortFieldSpec getFieldSpec(int index) {
        return fieldSpecs[index];
    }

    /** Gets num of field in the spec. */
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
            return new SortSpec(fieldSpecs.stream().toArray(SortFieldSpec[]::new));
        }
    }

    /** Sort info for a Field. */
    public static class SortFieldSpec {
        /** 0-based index of field being sorted. */
        private final int fieldIndex;
        /** in ascending order or not. */
        private final boolean isAscendingOrder;
        /** put null at last or not. */
        private final boolean nullIsLast;

        public SortFieldSpec(int fieldIndex, boolean isAscendingOrder, boolean nullIsLast) {
            this.fieldIndex = fieldIndex;
            this.isAscendingOrder = isAscendingOrder;
            this.nullIsLast = nullIsLast;
        }

        public int getFieldIndex() {
            return fieldIndex;
        }

        public boolean getIsAscendingOrder() {
            return isAscendingOrder;
        }

        public boolean getNullIsLast() {
            return nullIsLast;
        }
    }
}
