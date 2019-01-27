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

package org.apache.flink.table.sources.parquet;

import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions;
import org.apache.flink.types.Row;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.table.sources.parquet.ParquetTestUtil.checkWriteParquet;
import static org.apache.hadoop.hdfs.server.common.Storage.deleteDir;

/**
 * Tests for {@link ParquetInputFormat}
 * and {@link VectorizedBatchParquetInputFormat}.
 */
public class VectorizedBatchParquetInputFormatTest {

	private String path = System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID();

	@Test
	public void testReadOneSplitFile() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.STRING, DataTypes.DOUBLE,
			DataTypes.LONG, DataTypes.FLOAT, DataTypes.BYTE,
			DataTypes.TIMESTAMP};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"};
		int blockSize = 1024;
		boolean enableDictionary = false;
		int generatorSize = 4096;
		int split = 1;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, true);
				r.update(1, (short) random.nextInt());
				r.update(2, random.nextInt() + "testReadOneSplitFile");
				r.update(3, random.nextDouble());
				r.update(4, random.nextLong());
				r.update(5, random.nextFloat());
				r.update(6, (byte) 1);
				r.update(7, BuildInScalarFunctions.toLong(
						new Timestamp(System.currentTimeMillis())));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		checkWriteParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			enableDictionary,
			CompressionCodecName.UNCOMPRESSED,
			split,
			rowIterator,
			expertRows);
	}

	@Test
	public void testReadMultiSplitFile() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.STRING, DataTypes.DOUBLE,
			DataTypes.LONG, DataTypes.FLOAT, DataTypes.BYTE,
			DataTypes.TIMESTAMP};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"};
		int blockSize = 1024;
		boolean enableDictionary = false;
		int generatorSize = 5096;
		int split = 4;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, true);
				r.update(1, (short) random.nextInt());
				r.update(2, random.nextInt() + "testReadMultiSplitFile");
				r.update(3, random.nextDouble());
				r.update(4, random.nextLong());
				r.update(5, random.nextFloat());
				r.update(6, (byte) 1);
				r.update(7, BuildInScalarFunctions.toLong(
						new Timestamp(System.currentTimeMillis())));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		checkWriteParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			enableDictionary,
			CompressionCodecName.UNCOMPRESSED,
			split,
			rowIterator,
			expertRows);
	}

	@Test
	public void testReadNullValue() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.STRING, DataTypes.DOUBLE,
			DataTypes.LONG, DataTypes.FLOAT, DataTypes.BYTE,
			DataTypes.TIMESTAMP};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"};
		int blockSize = 1024;
		boolean enableDictionary = false;
		int generatorSize = 5096;
		int split = 4;
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, null);
				r.update(1, null);
				r.update(2, null);
				r.update(3, null);
				r.update(4, null);
				r.update(5, null);
				r.update(6, null);
				r.update(7, null);
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		checkWriteParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			enableDictionary,
			CompressionCodecName.UNCOMPRESSED,
			split,
			rowIterator,
			expertRows);
	}

	@Test
	public void testRequiredRead() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.STRING, DataTypes.DOUBLE,
			DataTypes.LONG, DataTypes.FLOAT, DataTypes.BYTE,
			DataTypes.TIMESTAMP};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"};
		int blockSize = 1024;
		boolean enableDictionary = false;
		int generatorSize = 4096;
		int split = 1;
		final Random random = new Random(generatorSize);

		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, true);
				r.update(1, (short) random.nextInt());
				r.update(2, random.nextInt() + "testRequiredRead");
				r.update(3, random.nextDouble());
				r.update(4, random.nextLong());
				r.update(5, random.nextFloat());
				r.update(6, (byte) 1);
				r.update(7, BuildInScalarFunctions.toLong(
						new Timestamp(System.currentTimeMillis())));
				return r;
			}

			@Override
			public void remove() {

			}
		};

		List<Row> actualRows = new ArrayList<>();

		ParquetTestUtil.writeParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			enableDictionary,
			CompressionCodecName.UNCOMPRESSED,
			rowIterator);

		//read

		InternalType[] requiredFieldTypes = new InternalType[]{
			DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.STRING, DataTypes.DOUBLE,
			DataTypes.LONG, DataTypes.FLOAT, DataTypes.BYTE};
		String[] requiredFieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7"};

		VectorizedBatchParquetInputFormat inputFormat = new VectorizedBatchParquetInputFormat(
			new org.apache.flink.core.fs.Path(path),
			requiredFieldTypes,
			requiredFieldNames);
		ParquetTestUtil.readParquet(
			inputFormat,
			split,
			new ParquetTestUtil.ConvertVectorBatch2Row(actualRows));

		//verify
		assertEquals(actualRows.size(), generatorSize);
		for (Row r : actualRows) {
			assertEquals(r.getArity(), 7);
		}
	}

	@Test
	public void testFilterRead() throws Exception {
		InternalType[] fieldTypes = new InternalType[]{
			DataTypes.BOOLEAN, DataTypes.SHORT};
		final String[] fieldNames = new String[]{"f1", "f2"};
		int blockSize = 1024;
		boolean enableDictionary = false;
		int generatorSize = 1024;
		int split = 2;
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			int index = 0;

			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, true);
				r.update(1, (short) index++);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		List<Row> actualRows = new ArrayList<>();

		ParquetTestUtil.writeParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			enableDictionary,
			CompressionCodecName.UNCOMPRESSED,
			rowIterator);

		//read based filter
		VectorizedBatchParquetInputFormat inputFormat = new VectorizedBatchParquetInputFormat(
			new org.apache.flink.core.fs.Path(path),
			fieldTypes,
			fieldNames);
		FilterPredicate filter = FilterApi.lt(FilterApi.intColumn("f2"), 10);
		inputFormat.setFilterPredicate(filter);

		ParquetTestUtil.readParquet(
			inputFormat,
			split,
			new ParquetTestUtil.ConvertVectorBatch2Row(actualRows));
		//verify
		assertTrue(generatorSize > actualRows.size());
	}

	@After
	public void after() throws IOException {
		File file = new File(path);
		if (file.exists()) {
			deleteDir(file);
		}
	}
}
