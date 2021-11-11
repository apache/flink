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
import java.util.stream.IntStream;

class TopLevelProjection extends AbstractProjection {

    final int[] projection;

    TopLevelProjection(int[] projection) {
        this.projection = projection;
    }

    @Override
    public DataType project(DataType dataType) {
        return DataType.projectFields(dataType, this.projection);
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
                Arrays.stream(projection).boxed().collect(Collectors.toCollection(ArrayList::new));

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
