/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.ExecutionConfigOptions;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.typeutils.BinaryRowSerializer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.runtime.hashtable.LongHashPartition.INVALID_ADDRESS;

/**
 * Test for {@link LongHashPartition}.
 */
@RunWith(Parameterized.class)
public class LongHashPartitionTest {

	private static final int PAGE_SIZE = 32 * 1024;

	private IOManager ioManager;
	private BinaryRowSerializer buildSideSerializer;
	private BinaryRowSerializer probeSideSerializer;
	private MemoryManager memManager = new MemoryManager(50 * PAGE_SIZE, 1);

	private boolean useCompress;
	private Configuration conf;

	private LongHashPartition partition;

	public LongHashPartitionTest(boolean useCompress) {
		this.useCompress = useCompress;
	}

	@Parameterized.Parameters(name = "useCompress-{0}")
	public static List<Boolean> getVarSeg() {
		return Arrays.asList(true, false);
	}

	@Before
	public void init() {
		TypeInformation[] types = new TypeInformation[]{Types.LONG, Types.STRING};
		this.buildSideSerializer = new BinaryRowSerializer(types.length);
		this.probeSideSerializer = new BinaryRowSerializer(types.length);
		this.ioManager = new IOManagerAsync();

		conf = new Configuration();
		conf.setBoolean(ExecutionConfigOptions.SQL_EXEC_SPILL_COMPRESSION_ENABLED, useCompress);

		LongHybridHashTable table = new MyHashTable(50 * PAGE_SIZE);
		this.partition = new LongHashPartition(table, 1,
			new BinaryRowSerializer(types.length), 15, 15, 0);
	}

	@Test
	public void testAddressAndLen() {
		int length = 5;
		long addrAndLen = LongHashPartition.toAddrAndLen(INVALID_ADDRESS, length);
		Assert.assertEquals(INVALID_ADDRESS, LongHashPartition.toAddress(addrAndLen));
		Assert.assertEquals(length, LongHashPartition.toLength(addrAndLen));
	}

	@Test
	public void testDenseLong() throws IOException {
		partition.append(0, row(0, "hehe0"));
		partition.append(0, row(0, "haha0"));
		partition.append(5, row(5, "hehe5"));
		partition.append(5, row(5, "haha5"));
		partition.append(3, row(3, "hehe3"));
		partition.append(3, row(3, "haha3"));
		partition.append(128, row(128, "hehe128"));

		partition.finalizeBuildPhase(null, null);

		assertPartition(0, Arrays.asList("haha0", "hehe0"));
		assertPartition(5, Arrays.asList("haha5", "hehe5"));
		assertPartition(3, Arrays.asList("haha3", "hehe3"));
		assertPartition(128, Collections.singletonList("hehe128"));
	}

	@Test
	public void testNotDenseLong() throws IOException {
		partition.append(5, row(5, "hehe5"));
		partition.append(5, row(5, "haha5"));
		partition.append(3222, row(3222, "hehe3222"));
		partition.append(3, row(3, "haha3"));
		partition.append(1284444, row(1284444, "hehe1284444"));

		partition.finalizeBuildPhase(null, null);

		assertPartition(5, Arrays.asList("haha5", "hehe5"));
		assertPartition(3, Collections.singletonList("haha3"));
		assertPartition(3222, Collections.singletonList("hehe3222"));
		assertPartition(1284444, Collections.singletonList("hehe1284444"));
	}

	@Test
	public void testMany() throws IOException {
		for (int i = 0; i < 2500; i++) {
			partition.append(i, row(i, "hehe" + i));
		}

		partition.append(10000000L, row(10000000L, "hehe" + 10000000L));

		partition.finalizeBuildPhase(null, null);

		for (int i = 0; i < 2500; i++) {
			assertPartition(i, Collections.singletonList("hehe" + i));
		}
		assertPartition(10000000L, Collections.singletonList("hehe" + 10000000L));
		Assert.assertFalse(partition.get(1010002020202L).advanceNext());
	}

	private void assertPartition(long k, List<String> values) {
		RowIterator<BinaryRow> iterator = partition.get(k);
		List<String> ret = new ArrayList<>();
		while (iterator.advanceNext()) {
			ret.add(iterator.getRow().getString(1).toString());
		}
		Assert.assertEquals(values, ret);
	}

	private BinaryRow row(long k, String v) {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeLong(0, k);
		writer.writeString(1, BinaryString.fromString(v));
		writer.complete();
		return row;
	}

	private class MyHashTable extends LongHybridHashTable {
		MyHashTable(long memorySize) {
			super(conf, LongHashPartitionTest.this, buildSideSerializer, probeSideSerializer, memManager, memorySize,
				memorySize, 0, LongHashPartitionTest.this.ioManager,
				24, 200000);
		}

		@Override
		public long getBuildLongKey(BaseRow row) {
			return row.getInt(0);
		}

		@Override
		public long getProbeLongKey(BaseRow row) {
			return row.getInt(0);
		}

		@Override
		public BinaryRow probeToBinary(BaseRow row) {
			return (BinaryRow) row;
		}
	}
}
