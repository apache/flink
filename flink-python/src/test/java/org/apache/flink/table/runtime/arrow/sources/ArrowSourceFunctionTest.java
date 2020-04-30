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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Tests for {@link ArrowSourceFunction}.
 */
public class ArrowSourceFunctionTest extends ArrowSourceFunctionTestBase<RowData> {

	private static List<LogicalType> fieldTypes = new ArrayList<>();
	private static RowType rowType;
	private static DataType dataType;
	private static RowDataSerializer serializer;
	private static BufferAllocator allocator;

	public ArrowSourceFunctionTest() {
		super(VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator),
			serializer,
			Comparator.comparing(o -> o.getString(0)),
			new DeeplyEqualsChecker()
				.withCustomCheck(
					(o1, o2) -> o1 instanceof RowData && o2 instanceof RowData,
					(o1, o2, checker) -> deepEqualsBaseRow(
						(RowData) o1,
						(RowData) o2,
						(RowDataSerializer) serializer.duplicate(),
						(RowDataSerializer) serializer.duplicate())));
	}

	private static boolean deepEqualsBaseRow(
		RowData should, RowData is,
		RowDataSerializer serializer1, RowDataSerializer serializer2) {
		if (should.getArity() != is.getArity()) {
			return false;
		}
		BinaryRowData row1 = serializer1.toBinaryRow(should);
		BinaryRowData row2 = serializer2.toBinaryRow(is);

		return Objects.equals(row1, row2);
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
		serializer = new RowDataSerializer(
			new ExecutionConfig(), fieldTypes.toArray(new LogicalType[0]));
		allocator = ArrowUtils.getRootAllocator().newChildAllocator("stdout", 0, Long.MAX_VALUE);
	}

	@Override
	public Tuple2<List<RowData>, Integer> getTestData() {
		return Tuple2.of(
			Arrays.asList(
				GenericRowData.of(BinaryStringData.fromString("aaa")),
				GenericRowData.of(BinaryStringData.fromString("bbb")),
				GenericRowData.of(BinaryStringData.fromString("ccc")),
				GenericRowData.of(BinaryStringData.fromString("ddd")),
				GenericRowData.of(BinaryStringData.fromString("eee"))),
			3);
	}

	@Override
	public ArrowWriter<RowData> createArrowWriter() {
		return ArrowUtils.createRowDataArrowWriter(root, rowType);
	}

	@Override
	public AbstractArrowSourceFunction<RowData> createArrowSourceFunction(byte[][] arrowData) {
		return new ArrowSourceFunction(dataType, arrowData);
	}
}
