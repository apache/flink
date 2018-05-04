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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Tests to guard {@link ShrinkableBloomFilterNode}.
 */
public class ShrinkableBloomFilterNodeTest {

	@Test
	public void testBasicFunctionalityForBloomFilterUnit() {
		ShrinkableBloomFilterNode.BloomFilterUnit unit = new ShrinkableBloomFilterNode.BloomFilterUnit(10000, 0.02);

		Assert.assertEquals(10000, unit.capacity());
		Assert.assertEquals(0, unit.size());
		Assert.assertFalse(unit.full());

		for (int i = 0; i < 10000; ++i) {
			Assert.assertFalse(unit.contains(String.valueOf(i).getBytes()));
		}

		for (int i = 0; i < 10000; ++i) {
			unit.add(String.valueOf(i).getBytes());
		}

		for (int i = 0; i < 10000; ++i) {
			Assert.assertTrue(unit.contains(String.valueOf(i).getBytes()));
		}
	}

	@Test
	public void testMergeBloomFilterUnit() {

		ShrinkableBloomFilterNode.BloomFilterUnit unit1 = new ShrinkableBloomFilterNode.BloomFilterUnit(10000, 0.02);
		ShrinkableBloomFilterNode.BloomFilterUnit unit2 = new ShrinkableBloomFilterNode.BloomFilterUnit(10000, 0.02);
		ShrinkableBloomFilterNode.BloomFilterUnit unit3 = new ShrinkableBloomFilterNode.BloomFilterUnit(20000, 0.02);

		for (int i = 0; i < 5000; ++i) {
			unit1.add(String.valueOf(i).getBytes());
		}

		for (int i = 5000; i < 10000; ++i) {
			unit2.add(String.valueOf(i).getBytes());
		}

		// before merging with unit2
		for (int i = 0; i < 5000; ++i) {
			Assert.assertTrue(unit1.contains(String.valueOf(i).getBytes()));
		}

		List<byte[]> records5000To10000 = new ArrayList<>();
		for (int i = 5000; i < 10000; ++i) {
			records5000To10000.add(String.valueOf(i).getBytes());
		}

		verifyBloomFilterWithFpp(unit1, records5000To10000, false, 0.02);

		// after merging with unit1
		unit1.merge(unit2);
		for (int i = 0; i < 10000; ++i) {
			Assert.assertTrue(unit1.contains(String.valueOf(i).getBytes()));
		}

		try {
			unit2.merge(unit3);
			Assert.fail(); // failed because the size of unit2 and unit3 is different.
		} catch (Exception expected) {

		}
	}

	@Test
	public void testSnapshotAndRestoreForBloomFilterUnit() throws Exception {
		ShrinkableBloomFilterNode.BloomFilterUnit unit = new ShrinkableBloomFilterNode.BloomFilterUnit(10000, 0.02);

		for (int i = 0; i < 5000; ++i) {
			unit.add(String.valueOf(i).getBytes());
		}

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);
		DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
		unit.snapshot(outputViewStreamWrapper);

		byte[] snapshottedBytes = outputStream.toByteArray();

		ByteArrayInputStream inputStream = new ByteArrayInputStream(snapshottedBytes);
		DataInputViewStreamWrapper inputViewStreamWrapper = new DataInputViewStreamWrapper(inputStream);

		ShrinkableBloomFilterNode.BloomFilterUnit restoredUnit = new ShrinkableBloomFilterNode.BloomFilterUnit();
		restoredUnit.restore(inputViewStreamWrapper);

		Assert.assertEquals(unit, restoredUnit);
	}

	@Test
	public void testBasicFunctionalityForShrinkableBloomFilterNode() {

	}

	@Test
	public void testSnapshotAndRestoreForShrinkableBloomFilterNode() {

	}

	@Test
	public void testShrinking() {

	}

	private boolean verifyBloomFilterWithFpp(ShrinkableBloomFilterNode.BloomFilterUnit unit, Collection<byte[]> records, boolean target, double expectedFpp) {
		int count = 0;
		for (byte[] record : records) {
			if (unit.contains(record) != target) {
				++count;
			}
		}
		return ((double) count / records.size()) <= expectedFpp;
	}
}
