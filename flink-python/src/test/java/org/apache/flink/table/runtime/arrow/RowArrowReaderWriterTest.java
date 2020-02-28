/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link ArrowReader} and {@link ArrowWriter} of Row.
 */
public class RowArrowReaderWriterTest extends ArrowReaderWriterTestBase<Row> {
	private static RowType rowType;
	private static BufferAllocator allocator;

	@BeforeClass
	public static void init() {
		List<LogicalType> fieldTypes = new ArrayList<>();
		fieldTypes.add(new TinyIntType());
		fieldTypes.add(new SmallIntType());
		fieldTypes.add(new IntType());
		fieldTypes.add(new BigIntType());

		List<RowType.RowField> rowFields = new ArrayList<>();
		for (int i = 0; i < fieldTypes.size(); i++) {
			rowFields.add(new RowType.RowField("f" + i, fieldTypes.get(i)));
		}
		rowType = new RowType(rowFields);
		allocator = ArrowUtils.ROOT_ALLOCATOR.newChildAllocator("stdout", 0, Long.MAX_VALUE);
	}

	@Override
	public ArrowReader<Row> createArrowReader(InputStream inputStream) throws IOException {
		ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
		reader.loadNextBatch();
		return ArrowUtils.createRowArrowReader(reader.getVectorSchemaRoot());
	}

	@Override
	public Tuple2<ArrowWriter<Row>, ArrowStreamWriter> createArrowWriter(OutputStream outputStream) throws IOException {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		ArrowWriter<Row> arrowWriter = ArrowUtils.createRowArrowWriter(root);
		ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(root, null, outputStream);
		arrowStreamWriter.start();
		return Tuple2.of(arrowWriter, arrowStreamWriter);
	}

	@Override
	public Row[] getTestData() {
		Row row1 = Row.of((byte) 1, (short) 2, 3, 4L);
		Row row2 = Row.of((byte) 1, (short) 2, 3, 4L);
		Row row3 = Row.of(null, (short) 2, 3, 4L);
		Row row4 = Row.of((byte) 1, null, 3, 4L);
		Row row5 = Row.of(null, null, null, null);
		Row row6 = Row.of(null, null, null, null);
		return new Row[]{row1, row2, row3, row4, row5, row6};
	}
}
