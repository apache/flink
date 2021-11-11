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

import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

class NestedProjection extends AbstractProjection {

    final int[][] projection;
    final boolean nested;

    NestedProjection(int[][] projection) {
        this.projection = projection;
        this.nested = Arrays.stream(projection).anyMatch(arr -> arr.length > 1);
    }

    @Override
    public DataType project(DataType dataType) {
        return DataType.projectFields(dataType, projection);
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
