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

package org.apache.flink.table.runtime.join.batch.hashtable.longtable;

import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.runtime.sort.InMemorySortTest;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.typeutils.BinaryRowSerializer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.runtime.join.batch.hashtable.longtable.LongHashPartition.INVALID_ADDRESS;

/**
 * Test for {@link LongHashPartition}.
 */
public class LongHashPartitionTest {

	private LongHashPartition table;

	@Before
	public void init() {
		LongHashContext context = new TestLongHashContext();
		this.table = new LongHashPartition(context, 1,
				new BinaryRowSerializer(DataTypes.LONG, DataTypes.STRING), 15, 15, 0);
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

		table.append(0, row(0, "hehe0"));
		table.append(0, row(0, "haha0"));
		table.append(5, row(5, "hehe5"));
		table.append(5, row(5, "haha5"));
		table.append(3, row(3, "hehe3"));
		table.append(3, row(3, "haha3"));
		table.append(128, row(128, "hehe128"));

		table.finalizeBuildPhase(null, null);

		assertTable(0, Arrays.asList("haha0", "hehe0"));
		assertTable(5, Arrays.asList("haha5", "hehe5"));
		assertTable(3, Arrays.asList("haha3", "hehe3"));
		assertTable(128, Collections.singletonList("hehe128"));
	}

	@Test
	public void testNotDenseLong() throws IOException {

		table.append(5, row(5, "hehe5"));
		table.append(5, row(5, "haha5"));
		table.append(3222, row(3222, "hehe3222"));
		table.append(3, row(3, "haha3"));
		table.append(1284444, row(1284444, "hehe1284444"));

		table.finalizeBuildPhase(null, null);

		assertTable(5, Arrays.asList("haha5", "hehe5"));
		assertTable(3, Collections.singletonList("haha3"));
		assertTable(3222, Collections.singletonList("hehe3222"));
		assertTable(1284444, Collections.singletonList("hehe1284444"));
	}

	@Test
	public void testMany() throws IOException {

		for (int i = 0; i < 2500; i++) {
			table.append(i, row(i, "hehe" + i));
		}

		table.append(10000000L, row(10000000L, "hehe" + 10000000L));

		table.finalizeBuildPhase(null, null);

		for (int i = 0; i < 2500; i++) {
			assertTable(i, Collections.singletonList("hehe" + i));
		}
		assertTable(10000000L, Collections.singletonList("hehe" + 10000000L));
		Assert.assertFalse(table.get(1010002020202L).advanceNext());
	}

	private void assertTable(long k, List<String> values) {
		RowIterator<BinaryRow> iterator = table.get(k);
		List<String> ret = new ArrayList<>();
		while (iterator.advanceNext()) {
			ret.add(iterator.getRow().getString(1));
		}
		Assert.assertEquals(values, ret);
	}

	private BinaryRow row(long k, String v) {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeLong(0, k);
		writer.writeString(1, v);
		writer.complete();
		return row;
	}

	private static class TestLongHashContext
			extends InMemorySortTest.TestMemorySegmentPool implements LongHashContext {

		private TestLongHashContext() {
			super(32 * 1024);
		}

		@Override
		public MemorySegment getNextBuffer() {
			return nextSegment();
		}

		@Override
		public int spillPartition() throws IOException {
			throw new RuntimeException();
		}

		@Override
		public boolean compressionEnable() {
			return false;
		}

		@Override
		public BlockCompressionFactory compressionCodecFactory() {
			return null;
		}

		@Override
		public int compressionBlockSize() {
			return 0;
		}
	}
}
