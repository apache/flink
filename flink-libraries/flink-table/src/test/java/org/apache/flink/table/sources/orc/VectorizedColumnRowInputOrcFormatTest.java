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

package org.apache.flink.table.sources.orc;

import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions;

import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Test for {@link org.apache.flink.table.sources.orc.OrcInputFormat}
 * and {@link VectorizedColumnRowInputOrcFormat}.
 */
public class VectorizedColumnRowInputOrcFormatTest {
	private String path = System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID();

	@Test
	public void testTypesWithoutNulls() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			BooleanType.INSTANCE,
			ByteType.INSTANCE,
			ShortType.INSTANCE,
			IntType.INSTANCE,
			LongType.INSTANCE,
			FloatType.INSTANCE,
			DoubleType.INSTANCE,
			StringType.INSTANCE,
			DecimalType.SYSTEM_DEFAULT,
			DateType.DATE,
			TimestampType.TIMESTAMP};
		final String[] fieldNames = new String[]{
			"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11"};

		int generatorSize = 4096;
		int split = 1;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new OrcTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, true);
				r.update(1, (byte) random.nextInt());
				r.update(2, (short) random.nextInt());
				r.update(3, random.nextInt());
				r.update(4, random.nextLong());
				r.update(5, random.nextFloat());
				r.update(6, random.nextDouble());
				r.update(7, random.nextInt() + "testTypesWithoutNulls");
				r.update(8, Decimal.castFrom(random.nextLong(), 38, 18));
				r.update(9, BuildInScalarFunctions.toInt(new Date(System.currentTimeMillis())));
				r.update(10, BuildInScalarFunctions.toLong(new Timestamp(System.currentTimeMillis())));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		VectorizedColumnRowInputOrcFormat inputFormat =
			new VectorizedColumnRowInputOrcFormat(new org.apache.flink.core.fs.Path(path), fieldTypes, fieldNames);

		OrcTestUtil.checkOrcVectorizedBaseFormat(fieldTypes, fieldNames, fieldTypes, fieldNames,
			path, split, rowIterator, inputFormat, expertRows);
	}

	@Test
	public void testTypesWithoutNullsAndWithRepeating() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			BooleanType.INSTANCE,
			ByteType.INSTANCE,
			ShortType.INSTANCE,
			IntType.INSTANCE,
			LongType.INSTANCE,
			FloatType.INSTANCE,
			DoubleType.INSTANCE,
			StringType.INSTANCE};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"};

		int generatorSize = 4096;
		int split = 1;
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new OrcTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, true);
				r.update(1, (byte) 1);
				r.update(2, (short) 2);
				r.update(3, 3);
				r.update(4, 4L);
				r.update(5, 5.0F);
				r.update(6, 6.0D);
				r.update(7, "testTypesWithoutNulls");
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		VectorizedColumnRowInputOrcFormat inputFormat =
			new VectorizedColumnRowInputOrcFormat(new org.apache.flink.core.fs.Path(path), fieldTypes, fieldNames);

		OrcTestUtil.checkOrcVectorizedBaseFormat(fieldTypes, fieldNames, fieldTypes, fieldNames,
			path, split, rowIterator, inputFormat, expertRows);
	}

	@Test
	public void testTypesWithNulls() throws IOException {
		InternalType[] fieldTypes = new InternalType[]{
			BooleanType.INSTANCE,
			ByteType.INSTANCE,
			ShortType.INSTANCE,
			IntType.INSTANCE,
			LongType.INSTANCE,
			FloatType.INSTANCE,
			DoubleType.INSTANCE,
			StringType.INSTANCE};
		final String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"};

		int generatorSize = 4096;
		int split = 1;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new OrcTestUtil.GeneratorRow(generatorSize) {

			private int index = 0;

			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				r.update(0, (index % 1000 == 0) ? null : true);
				r.update(1, (index % 1000 == 0) ? null : (byte) random.nextInt());
				r.update(2, (index % 1000 == 0) ? null : (short) random.nextInt());
				r.update(3, (index % 1000 == 0) ? null : random.nextInt());
				r.update(4, (index % 1000 == 0) ? null : random.nextLong());
				r.update(5, (index % 1000 == 0) ? null : random.nextFloat());
				r.update(6, (index % 1000 == 0) ? null : random.nextDouble());
				r.update(7, (index % 1000 == 0) ? null : random.nextInt() + "testTypesWithoutNulls");
				expertRows.add(r);

				++index;

				return r;
			}

			@Override
			public void remove() {

			}
		};

		VectorizedColumnRowInputOrcFormat inputFormat =
			new VectorizedColumnRowInputOrcFormat(new org.apache.flink.core.fs.Path(path), fieldTypes, fieldNames);

		OrcTestUtil.checkOrcVectorizedBaseFormat(fieldTypes, fieldNames, fieldTypes, fieldNames,
			path, split, rowIterator, inputFormat, expertRows);
	}
}
