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

package org.apache.flink.table.client.gateway.result;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class BaseMaterializedResultTest {

    static Function<Row, BinaryRowData> createInternalBinaryRowDataConverter(DataType dataType) {
        DataStructureConverter<Object, Object> converter =
                DataStructureConverters.getConverter(dataType);
        RowDataSerializer serializer = new RowDataSerializer((RowType) dataType.getLogicalType());

        return row -> serializer.toBinaryRow((RowData) converter.toInternalOrNull(row)).copy();
    }

    static void assertRowEquals(
            List<Row> expected,
            List<RowData> actual,
            DataStructureConverter<RowData, Row> converter) {
        assertThat(actual.stream().map(converter::toExternalOrNull).collect(Collectors.toList()))
                .isEqualTo(expected);
    }
}
