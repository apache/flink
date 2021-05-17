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

package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Tests for {@link RowArrowSourceFunction}. */
public class RowArrowSourceFunctionTest extends ArrowSourceFunctionTestBase<Row> {

    private static List<LogicalType> fieldTypes = new ArrayList<>();
    private static RowType rowType;
    private static DataType dataType;
    private static BufferAllocator allocator;

    public RowArrowSourceFunctionTest() {
        super(
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator),
                new RowSerializer(new TypeSerializer[] {StringSerializer.INSTANCE}),
                Comparator.comparing(o -> (String) (o.getField(0))));
    }

    @BeforeClass
    public static void init() {
        fieldTypes.add(new VarCharType());
        List<RowType.RowField> rowFields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            rowFields.add(new RowType.RowField("f" + i, fieldTypes.get(i)));
        }
        rowType = new RowType(rowFields);
        dataType = TypeConversions.fromLogicalToDataType(rowType);
        allocator = ArrowUtils.getRootAllocator().newChildAllocator("stdout", 0, Long.MAX_VALUE);
    }

    @Override
    public Tuple2<List<Row>, Integer> getTestData() {
        return Tuple2.of(
                Arrays.asList(
                        Row.of("aaa"), Row.of("bbb"), Row.of("ccc"), Row.of("ddd"), Row.of("eee")),
                3);
    }

    @Override
    public ArrowWriter<Row> createArrowWriter() {
        return ArrowUtils.createRowArrowWriter(root, rowType);
    }

    @Override
    public AbstractArrowSourceFunction<Row> createArrowSourceFunction(byte[][] arrowData) {
        return new RowArrowSourceFunction(dataType, arrowData);
    }
}
