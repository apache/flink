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

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static org.apache.hadoop.hdfs.server.common.Storage.deleteDir;

/**
 * Tests for {@link ParquetInputFormat}
 * and {@link VectorizedRowParquetInputFormatTest}.
 */
public class VectorizedRowParquetInputFormatTest {

	private String path;

	@Before
	public void setUp() {
		path = System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID();
	}

	@Test
	public void testReadRow() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.STRING,
				DecimalType.of(5, 1), DecimalType.of(15, 9),  DecimalType.of(20, 4)};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6"};
		int blockSize = 1026;
		int generatorSize = 1026;
		int split = 2;
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			private Random random = new Random();

			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, random.nextBoolean());
				r.update(1, (short) random.nextInt(100));
				r.update(2, "testReadRow" + random.nextInt());
				r.update(3, newRandomDecimal(random, 5, 1));
				r.update(4, newRandomDecimal(random, 15, 9));
				r.update(5, newRandomDecimal(random, 20, 4));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};
		doReadRow(fieldTypes, fieldNames, blockSize, false, split, expertRows, rowIterator);
	}

	@Test
	public void testReadRowWithBiggerBlockSize() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
				DataTypes.INT, DataTypes.DOUBLE,
				DataTypes.LONG, DataTypes.FLOAT,
				DecimalType.of(5, 1), DecimalType.of(15, 9),  DecimalType.of(20, 4)};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7"};
		int blockSize = 40960;
		int generatorSize = 4099;
		int split = 1;
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			private Random random = new Random();

			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, random.nextInt());
				r.update(1, random.nextDouble());
				r.update(2, random.nextLong());
				r.update(3, random.nextFloat());
				r.update(4, newRandomDecimal(random, 5, 1));
				r.update(5, newRandomDecimal(random, 15, 9));
				r.update(6, newRandomDecimal(random, 20, 4));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};
		doReadRow(fieldTypes, fieldNames, blockSize, false, split, expertRows, rowIterator);
	}

	@Test
	public void testReadRowWithDictionary() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
				DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.STRING,
				DecimalType.of(5, 1), DecimalType.of(15, 9),  DecimalType.of(20, 4)};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6"};
		int blockSize = 1026;
		int generatorSize = 1026;
		int split = 2;
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			private Random random = new Random();

			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, random.nextBoolean());
				r.update(1, (short) random.nextInt(1024));
				r.update(2, "testReadRow" + random.nextInt());
				r.update(3, newRandomDecimal(random, 5, 1));
				r.update(4, newRandomDecimal(random, 15, 9));
				r.update(5, newRandomDecimal(random, 20, 4));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};
		doReadRow(fieldTypes, fieldNames, blockSize, true, split, expertRows, rowIterator);
	}

	@Test
	public void testReadRowWithDictionaryWithBiggerBlockSize() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
				DataTypes.INT, DataTypes.DOUBLE,
				DataTypes.LONG, DataTypes.FLOAT,
				DecimalType.of(5, 1), DecimalType.of(15, 9),  DecimalType.of(20, 4)};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7"};
		int blockSize = 40960;
		int generatorSize = 4099;
		int split = 1;
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			private Random random = new Random();

			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, random.nextInt(1024));
				r.update(1, random.nextDouble());
				r.update(2, random.nextLong());
				r.update(3, random.nextFloat());
				r.update(4, newRandomDecimal(random, 5, 1));
				r.update(5, newRandomDecimal(random, 15, 9));
				r.update(6, newRandomDecimal(random, 20, 4));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};
		doReadRow(fieldTypes, fieldNames, blockSize, true, split, expertRows, rowIterator);
	}

	private void doReadRow(InternalType[] fieldTypes, String[] fieldNames, int blockSize,
			boolean enableDictionary, int split, List<GenericRow> expertRows,
			Iterator<GenericRow> rowIterator) throws IOException {

		ParquetTestUtil.writeParquet(
				path,
				fieldTypes,
				fieldNames,
				blockSize,
				enableDictionary,
				CompressionCodecName.UNCOMPRESSED,
				rowIterator);

		//read based filter
		VectorizedGenericRowInputParquetFormat inputFormat = new VectorizedGenericRowInputParquetFormat(
				new org.apache.flink.core.fs.Path(path), fieldTypes, fieldNames);

		inputFormat.setNestedFileEnumeration(true);
		FileInputSplit[] splits = inputFormat.createInputSplits(split);
		List<GenericRow> actualRows = new ArrayList<>();
		for (int i = 0; i < split; i++) {
			inputFormat.open(splits[i]);
			while (!inputFormat.reachedEnd()) {
				GenericRow ret = inputFormat.nextRecord(null);
				actualRows.add(ret);
			}
			inputFormat.close();

		}
		for (int i = 0; i < expertRows.size(); i++) {
			assertEquals("i: " + i, expertRows.get(i), actualRows.get(i));
		}
	}

	@After
	public void after() throws IOException {
		File file = new File(path);
		if (file.exists()) {
			deleteDir(file);
		}
	}

	private Decimal newRandomDecimal(Random r, int precision, int scale) {
		BigInteger n = BigInteger.TEN.pow(precision);
		return Decimal.fromBigDecimal(new BigDecimal(newRandomBigInteger(n, r), scale), precision, scale);
	}

	private static BigInteger newRandomBigInteger(BigInteger n, Random rnd) {
		BigInteger r;
		do {
			r = new BigInteger(n.bitLength(), rnd);
		} while (r.compareTo(n) >= 0);

		return r;
	}
}
