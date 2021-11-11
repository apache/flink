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

import java.util.stream.IntStream;

class EmptyProjection extends AbstractProjection {

    static final EmptyProjection INSTANCE = new EmptyProjection();

    private EmptyProjection() {}

    @Override
    public DataType project(DataType dataType) {
        return dataType;
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
