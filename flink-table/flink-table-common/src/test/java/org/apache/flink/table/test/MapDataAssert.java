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

package org.apache.flink.table.test;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.assertj.core.api.AbstractAssert;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Assertions for {@link MapData}. */
@Experimental
public class MapDataAssert extends AbstractAssert<MapDataAssert, MapData> {

    public MapDataAssert(MapData mapData) {
        super(mapData, MapDataAssert.class);
    }

    public MapDataAssert hasSize(int size) {
        isNotNull();
        assertThat(this.actual.size()).isEqualTo(size);
        return this;
    }

    public MapDataAssert asGeneric(DataType dataType) {
        return asGeneric(dataType.getLogicalType());
    }

    public MapDataAssert asGeneric(LogicalType logicalType) {
        GenericMapData actual = InternalDataUtils.toGenericMap(this.actual, logicalType);
        return new MapDataAssert(actual)
                .usingComparator(
                        (x, y) -> {
                            // Avoid converting actual again
                            x = x == actual ? x : InternalDataUtils.toGenericMap(x, logicalType);
                            y = y == actual ? y : InternalDataUtils.toGenericMap(y, logicalType);
                            if (Objects.equals(x, y)) {
                                return 0;
                            }
                            return Objects.hashCode(x) < Objects.hashCode(y) ? -1 : 1;
                        });
    }
}
