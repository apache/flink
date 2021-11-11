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

package org.apache.flink.table.types.projection;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.stream.IntStream;

/**
 * {@link Projection} represents a list of (possibly nested) indexes that can be used to project
 * data types.
 */
@Internal
public interface Projection {

    /** Project the provided {@link DataType} using this {@link Projection}. */
    DataType project(DataType dataType);

    /** @return {@code true} whether this projection is nested or not. */
    boolean isNested();

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
    Projection difference(Projection other);

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
    Projection complement(int fieldsNumber);

    /**
     * Convert this instance to a projection of top level indexes.
     *
     * @throws IllegalStateException if this projection is nested.
     */
    int[] toTopLevelIndexes();

    /** Convert this instance to a nested projection. */
    int[][] toNestedIndexes();

    /** Create an empty {@link Projection}. */
    static Projection empty() {
        return EmptyProjection.INSTANCE;
    }

    /** Create a {@link Projection} of the provided {@code indexes}. */
    static Projection of(int[] indexes) {
        return new TopLevelProjection(indexes);
    }

    /** Create a {@link Projection} of the provided {@code indexes}. */
    static Projection of(int[][] indexes) {
        return new NestedProjection(indexes);
    }

    /**
     * Create a {@link Projection} of the provided {@code dataType} using the provided {@code
     * projectedFields}.
     */
    static Projection fromFieldNames(DataType dataType, List<String> projectedFields) {
        List<String> dataTypeFieldNames = DataType.getFieldNames(dataType);
        return new TopLevelProjection(
                projectedFields.stream().mapToInt(dataTypeFieldNames::indexOf).toArray());
    }

    /** Create a {@link Projection} of all the fields in the provided {@code dataType}. */
    static Projection all(DataType dataType) {
        return new TopLevelProjection(
                IntStream.range(0, DataType.getFieldCount(dataType)).toArray());
    }
}
