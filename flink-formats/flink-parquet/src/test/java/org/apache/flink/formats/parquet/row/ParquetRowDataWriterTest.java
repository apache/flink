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

package org.apache.flink.formats.parquet.row;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * Test for {@link ParquetRowDataBuilder} and {@link ParquetRowDataWriter}.
 */
public class ParquetRowDataWriterTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static final RowType ROW_TYPE = RowType.of(
			new VarCharType(VarCharType.MAX_LENGTH),
			new VarBinaryType(VarBinaryType.MAX_LENGTH),
			new BooleanType(),
			new TinyIntType(),
			new SmallIntType(),
			new IntType(),
			new BigIntType(),
			new FloatType(),
			new DoubleType(),
			new TimestampType(9),
			new DecimalType(5, 0),
			new DecimalType(15, 0),
			new DecimalType(20, 0));

	@SuppressWarnings("unchecked")
	private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
			DataFormatConverters.getConverterForDataType(
					TypeConversions.fromLogicalToDataType(ROW_TYPE));

	@Test
	public void testTypes() throws IOException {
		Configuration conf = new Configuration();
		innerTest(conf, true);
		innerTest(conf, false);
	}

	@Test
	public void testCompression() throws IOException {
		Configuration conf = new Configuration();
		conf.set(ParquetOutputFormat.COMPRESSION, "GZIP");
		innerTest(conf, true);
		innerTest(conf, false);
	}

	private void innerTest(
			Configuration conf,
			boolean utcTimestamp) throws IOException {
		Path path = new Path(TEMPORARY_FOLDER.newFolder().getPath(), UUID.randomUUID().toString());
		int number = 1000;
		List<Row> rows = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			Integer v = i;
			rows.add(Row.of(
					String.valueOf(v),
					String.valueOf(v).getBytes(StandardCharsets.UTF_8),
					v % 2 == 0,
					v.byteValue(),
					v.shortValue(),
					v,
					v.longValue(),
					v.floatValue(),
					v.doubleValue(),
					toDateTime(v),
					BigDecimal.valueOf(v),
					BigDecimal.valueOf(v),
					BigDecimal.valueOf(v)));
		}

		ParquetWriterFactory<RowData> factory = ParquetRowDataBuilder.createWriterFactory(
				ROW_TYPE, conf, utcTimestamp);
		BulkWriter<RowData> writer = factory.create(path.getFileSystem().create(
				path, FileSystem.WriteMode.OVERWRITE));
		for (int i = 0; i < number; i++) {
			writer.addElement(CONVERTER.toInternal(rows.get(i)));
		}
		writer.flush();
		writer.finish();

		// verify
		ParquetColumnarRowSplitReader reader = ParquetSplitReaderUtil.genPartColumnarRowReader(
				utcTimestamp,
				true,
				conf,
				ROW_TYPE.getFieldNames().toArray(new String[0]),
				ROW_TYPE.getChildren().stream()
						.map(TypeConversions::fromLogicalToDataType)
						.toArray(DataType[]::new),
				new HashMap<>(),
				IntStream.range(0, ROW_TYPE.getFieldCount()).toArray(),
				50,
				path,
				0,
				Long.MAX_VALUE);
		int cnt = 0;
		while (!reader.reachedEnd()) {
			Row row = CONVERTER.toExternal(reader.nextRecord());
			Assert.assertEquals(rows.get(cnt), row);
			cnt++;
		}
		Assert.assertEquals(number, cnt);
	}

	private LocalDateTime toDateTime(Integer v) {
		v = (v > 0 ? v : -v) % 1000;
		return LocalDateTime.now().plusNanos(v).plusSeconds(v);
	}
}
