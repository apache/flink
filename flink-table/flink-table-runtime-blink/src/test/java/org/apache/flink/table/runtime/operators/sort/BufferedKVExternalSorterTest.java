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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * UT for BufferedKVExternalSorter.
 */
@RunWith(Parameterized.class)
public class BufferedKVExternalSorterTest {
	private static final int PAGE_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

	private IOManager ioManager;
	private BinaryRowSerializer keySerializer;
	private BinaryRowSerializer valueSerializer;
	private NormalizedKeyComputer computer;
	private RecordComparator comparator;

	private int spillNumber;
	private int recordNumberPerFile;

	private Configuration conf;

	public BufferedKVExternalSorterTest(
			int spillNumber, int recordNumberPerFile, boolean spillCompress) {
		ioManager = new IOManagerAsync();
		conf = new Configuration();
		conf.setInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES, 5);
		if (!spillCompress) {
			conf.setBoolean(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED, false);
		}
		this.spillNumber = spillNumber;
		this.recordNumberPerFile = recordNumberPerFile;
	}

	@Parameterized.Parameters
	public static List<Object[]> getDataSize() {
		List<Object[]> paras = new ArrayList<>();
		paras.add(new Object[]{3, 1000, true});
		paras.add(new Object[]{3, 1000, false});
		paras.add(new Object[]{10, 1000, true});
		paras.add(new Object[]{10, 1000, false});
		paras.add(new Object[]{10, 10000, true});
		paras.add(new Object[]{10, 10000, false});
		return paras;
	}

	@Before
	public void beforeTest() throws InstantiationException, IllegalAccessException {
		this.ioManager = new IOManagerAsync();

		this.keySerializer = new BinaryRowSerializer(2);
		this.valueSerializer = new BinaryRowSerializer(2);

		this.computer = IntNormalizedKeyComputer.INSTANCE;
		this.comparator = IntRecordComparator.INSTANCE;
	}

	@After
	public void afterTest() throws Exception {
		this.ioManager.close();
	}

	@Test
	public void test() throws Exception {
		BufferedKVExternalSorter sorter =
				new BufferedKVExternalSorter(
						ioManager, keySerializer, valueSerializer, computer, comparator,
						PAGE_SIZE, conf);
		TestMemorySegmentPool pool = new TestMemorySegmentPool(PAGE_SIZE);
		List<Integer> expected = new ArrayList<>();
		for (int i = 0; i < spillNumber; i++) {
			ArrayList<MemorySegment> segments = new ArrayList<>();
			SimpleCollectingOutputView out =
					new SimpleCollectingOutputView(segments, pool, PAGE_SIZE);
			writeKVToBuffer(
					keySerializer, valueSerializer,
					out,
					expected,
					recordNumberPerFile);
			sorter.sortAndSpill(segments, recordNumberPerFile, pool);
		}
		Collections.sort(expected);
		MutableObjectIterator<Tuple2<BinaryRow, BinaryRow>> iterator = sorter.getKVIterator();
		Tuple2<BinaryRow, BinaryRow> kv =
				new Tuple2<>(keySerializer.createInstance(), valueSerializer.createInstance());
		int count = 0;
		while ((kv = iterator.next(kv)) != null) {
			Assert.assertEquals((int) expected.get(count), kv.f0.getInt(0));
			Assert.assertEquals(expected.get(count) * -3 + 177, kv.f1.getInt(0));
			count++;
		}
		Assert.assertEquals(expected.size(), count);
		sorter.close();
	}

	private void writeKVToBuffer(
			BinaryRowSerializer keySerializer,
			BinaryRowSerializer valueSerializer,
			SimpleCollectingOutputView out,
			List<Integer> expecteds,
			int length) throws IOException {
		Random random = new Random();
		int stringLength = 30;
		for (int i = 0; i < length; i++) {
			BinaryRow key = randomRow(random, stringLength);
			BinaryRow val = key.copy();
			val.setInt(0, val.getInt(0) * -3 + 177);
			expecteds.add(key.getInt(0));
			keySerializer.serializeToPages(key, out);
			valueSerializer.serializeToPages(val, out);
		}
	}

	public static BinaryRow randomRow(Random random, int stringLength) {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeInt(0, random.nextInt());
		writer.writeString(1, BinaryString.fromString(RandomStringUtils.random(stringLength)));
		writer.complete();
		return row;
	}
}
