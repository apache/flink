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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

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

import static org.apache.flink.table.runtime.sort.InMemorySortTest.randomRow;

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
			int spillNumber, int recordNumberPerFile,
			boolean useBufferedIO, boolean spillCompress) {
		ioManager = useBufferedIO ? new IOManagerAsync(1024 * 1024, 1024 * 1024) : new IOManagerAsync();
		conf = new Configuration();
		if (!spillCompress) {
			conf.setBoolean(TableConfigOptions.SQL_EXEC_SPILL_COMPRESSION_ENABLED, false);
		}
		this.spillNumber = spillNumber;
		this.recordNumberPerFile = recordNumberPerFile;
	}

	@Parameterized.Parameters
	public static List<Object[]> getDataSize() {
		List<Object[]> paras = new ArrayList<>();
		paras.add(new Object[]{3, 1000, true, true});
		paras.add(new Object[]{3, 1000, false, false});
		paras.add(new Object[]{10, 1000, true, true});
		paras.add(new Object[]{10, 1000, false, false});
		paras.add(new Object[]{10, 10000, true, true});
		paras.add(new Object[]{10, 10000, false, false});
		return paras;
	}

	@Before
	public void beforeTest() throws InstantiationException, IllegalAccessException {
		this.ioManager = new IOManagerAsync();

		this.keySerializer =
				new BinaryRowSerializer(new TypeInformation[]{Types.INT, Types.STRING});
		this.valueSerializer =
				new BinaryRowSerializer(new TypeInformation[]{Types.INT, Types.STRING});

		Tuple2<NormalizedKeyComputer, RecordComparator> base =
				InMemorySortTest.getIntStringSortBase(true, "BufferedKVExternalSorterTest");
		this.computer = base.f0;
		this.comparator = base.f1;
	}

	@After
	public void afterTest() {
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
	}

	@Test
	public void test() throws Exception {
		BufferedKVExternalSorter sorter =
				new BufferedKVExternalSorter(
						ioManager, keySerializer, valueSerializer, computer, comparator,
						PAGE_SIZE, conf);
		InMemorySortTest.TestMemorySegmentPool pool =
				new InMemorySortTest.TestMemorySegmentPool(PAGE_SIZE);
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
}
