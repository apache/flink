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

package org.apache.flink.runtime.operators.util;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.testutils.recordutils.RecordComparatorFactory;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.KeyFieldOutOfBoundsException;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OutputEmitterTest {


	@Test
	public void testPartitionHash() {
		// Test for IntValue
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.PARTITION_HASH, intComp);

		int numChans = 100;
		int numRecs = 50000;
		int[] hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);

			int chan = oe1.selectChannel(rec, hit.length);
			hit[chan]++;
		}

		int cnt = 0;
		for (int aHit : hit) {
			assertTrue(aHit > 0);
			cnt += aHit;
		}
		assertTrue(cnt == numRecs);

		// Test for StringValue
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> stringComp = new RecordComparatorFactory(new int[] {0}, new Class[] {StringValue.class}).createComparator();
		final ChannelSelector<Record> oe2 = new OutputEmitter<>(ShipStrategyType.PARTITION_HASH, stringComp);

		numChans = 100;
		numRecs = 10000;

		hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);

			int chan = oe2.selectChannel(rec, hit.length);
			hit[chan]++;
		}

		cnt = 0;
		for (int aHit : hit) {
			assertTrue(aHit > 0);
			cnt += aHit;
		}
		assertTrue(cnt == numRecs);

		// test hash corner cases
		final TestIntComparator testIntComp = new TestIntComparator();
		final ChannelSelector<Integer> oe3 = new OutputEmitter<>(ShipStrategyType.PARTITION_HASH, testIntComp);

		numChans = 100;

		// MinVal hash
		int chans = oe3.selectChannel(Integer.MIN_VALUE, numChans);
		assertTrue(chans >= 0 && chans <= numChans-1);

		// -1 hash
		chans = oe3.selectChannel(-1, hit.length);
		assertTrue(chans >= 0 && chans <= numChans-1);

		// 0 hash
		chans = oe3.selectChannel(0, hit.length);
		assertTrue(chans >= 0 && chans <= numChans-1);

		// 1 hash
		chans = oe3.selectChannel(1, hit.length);
		assertTrue(chans >= 0 && chans <= numChans-1);

		// MaxVal hash
		chans = oe3.selectChannel(Integer.MAX_VALUE, hit.length);
		assertTrue(chans >= 0 && chans <= numChans-1);
	}

	@Test
	public void testForward() {
		// Test for IntValue
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.FORWARD, intComp);

		int numChannels = 100;
		int numRecords = 50000 + numChannels / 2;

		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);

			int chan = oe1.selectChannel(rec, hit.length);
			hit[chan]++;
		}

		assertTrue(hit[0] == numRecords);
		for (int i = 1; i < hit.length; i++) {
			assertTrue(hit[i] == 0);
		}

		// Test for StringValue
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> stringComp = new RecordComparatorFactory(new int[] {0}, new Class[] {StringValue.class}).createComparator();
		final ChannelSelector<Record> oe2 = new OutputEmitter<>(ShipStrategyType.FORWARD, stringComp);

		numChannels = 100;
		numRecords = 10000 + numChannels / 2;

		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);

			int chan = oe2.selectChannel(rec, hit.length);
			hit[chan]++;
		}

		assertTrue(hit[0] == numRecords);
		for (int i = 1; i < hit.length; i++) {
			assertTrue(hit[i] == 0);
		}
	}

	@Test
	public void testForcedRebalance() {
		// Test for IntValue
		int numChannels = 100;
		int toTaskIndex = numChannels * 6/7;
		int fromTaskIndex = toTaskIndex + numChannels;
		int extraRecords = numChannels / 3;
		int numRecords = 50000 + extraRecords;

		final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.PARTITION_FORCED_REBALANCE, fromTaskIndex);

		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);

			int chan = oe1.selectChannel(rec, hit.length);
			hit[chan]++;
		}

		int cnt = 0;
		for (int i = 0; i < hit.length; i++) {
			if (toTaskIndex <= i || i < toTaskIndex+extraRecords-numChannels) {
				assertTrue(hit[i] == (numRecords/numChannels)+1);
			} else {
				assertTrue(hit[i] == numRecords/numChannels);
			}
			cnt += hit[i];
		}
		assertTrue(cnt == numRecords);

		// Test for StringValue
		numChannels = 100;
		toTaskIndex = numChannels / 5;
		fromTaskIndex = toTaskIndex + 2 * numChannels;
		extraRecords = numChannels * 2/9;
		numRecords = 10000 + extraRecords;

		final ChannelSelector<Record> oe2 = new OutputEmitter<>(ShipStrategyType.PARTITION_FORCED_REBALANCE, fromTaskIndex);

		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);

			int chan = oe2.selectChannel(rec, hit.length);
			hit[chan]++;
		}

		cnt = 0;
		for (int i = 0; i < hit.length; i++) {
			if (toTaskIndex <= i && i < toTaskIndex+extraRecords) {
				assertTrue(hit[i] == (numRecords/numChannels)+1);
			} else {
				assertTrue(hit[i] == numRecords/numChannels);
			}
			cnt += hit[i];
		}
		assertTrue(cnt == numRecords);
	}

	@Test
	public void testBroadcast() {
		try {
			// Test for IntValue
			@SuppressWarnings({"unchecked", "rawtypes"})
			final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[]{0}, new Class[]{IntValue.class}).createComparator();
			final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.BROADCAST, intComp);

			int numChannels = 100;
			int numRecords = 50000;

			int[] hit = new int[numChannels];

			for (int i = 0; i < numRecords; i++) {
				IntValue k = new IntValue(i);
				Record rec = new Record(k);

				int chan = oe1.selectChannel(rec, hit.length);
				hit[chan]++;
			}

			for (int aHit : hit) {
				assertTrue(aHit + "", aHit == numRecords);
			}

			// Test for StringValue
			@SuppressWarnings({"unchecked", "rawtypes"})
			final TypeComparator<Record> stringComp = new RecordComparatorFactory(new int[]{0}, new Class[]{StringValue.class}).createComparator();
			final ChannelSelector<Record> oe2 = new OutputEmitter<>(ShipStrategyType.BROADCAST, stringComp);

			numChannels = 100;
			numRecords = 5000;

			hit = new int[numChannels];

			for (int i = 0; i < numRecords; i++) {
				StringValue k = new StringValue(i + "");
				Record rec = new Record(k);

				int chan = oe2.selectChannel(rec, hit.length);
				hit[chan]++;
			}

			for (int aHit : hit) {
				assertTrue(aHit + "", aHit == numRecords);
			}
		} catch (UnsupportedOperationException ex) {
			return;
		}

		fail("Broadcast selector does not support select channels.");
	}

	@Test
	public void testMultiKeys() {
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> multiComp = new RecordComparatorFactory(new int[] {0,1,3}, new Class[] {IntValue.class, StringValue.class, DoubleValue.class}).createComparator();
		final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.PARTITION_HASH, multiComp);

		int numChannels = 100;
		int numRecords = 5000;

		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			Record rec = new Record(4);
			rec.setField(0, new IntValue(i));
			rec.setField(1, new StringValue("AB"+i+"CD"+i));
			rec.setField(3, new DoubleValue(i*3.141d));

			int chan = oe1.selectChannel(rec, hit.length);
			hit[chan]++;
		}

		int cnt = 0;
		for (int aHit : hit) {
			assertTrue(aHit > 0);
			cnt += aHit;
		}
		assertTrue(cnt == numRecords);

	}

	@Test
	public void testMissingKey() {
		// Test for IntValue
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {1}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.PARTITION_HASH, intComp);

		Record rec = new Record(0);
		rec.setField(0, new IntValue(1));

		try {
			oe1.selectChannel(rec, 100);
		} catch (KeyFieldOutOfBoundsException re) {
			Assert.assertEquals(1, re.getFieldNumber());
			return;
		}
		Assert.fail("Expected a KeyFieldOutOfBoundsException.");
	}

	@Test
	public void testNullKey() {
		// Test for IntValue
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.PARTITION_HASH, intComp);

		Record rec = new Record(2);
		rec.setField(1, new IntValue(1));

		try {
			oe1.selectChannel(rec, 100);
		} catch (NullKeyFieldException re) {
			Assert.assertEquals(0, re.getFieldNumber());
			return;
		}
		Assert.fail("Expected a NullKeyFieldException.");
	}

	@Test
	public void testWrongKeyClass() {

		// Test for IntValue
		@SuppressWarnings({"unchecked", "rawtypes"})
		final TypeComparator<Record> doubleComp = new RecordComparatorFactory(new int[] {0}, new Class[] {DoubleValue.class}).createComparator();
		final ChannelSelector<Record> oe1 = new OutputEmitter<>(ShipStrategyType.PARTITION_HASH, doubleComp);

		Record rec = null;

		try {
			PipedInputStream pipedInput = new PipedInputStream(1024*1024);

			DataInputView in = new DataInputViewStreamWrapper(pipedInput);
			DataOutputView out = new DataOutputViewStreamWrapper(new PipedOutputStream(pipedInput));

			rec = new Record(1);
			rec.setField(0, new IntValue());

			rec.write(out);
			rec = new Record();
			rec.read(in);
		} catch (IOException e) {
			fail("Test erroneous");
		}

		try {
			oe1.selectChannel(rec, 100);
		} catch (DeserializationException re) {
			return;
		}
		Assert.fail("Expected a NullKeyFieldException.");
	}

	@SuppressWarnings({"serial", "rawtypes"})
	private static class TestIntComparator extends TypeComparator<Integer> {
		private TypeComparator[] comparators = new TypeComparator[]{new IntComparator(true)};

		@Override
		public int hash(Integer record) {
			return record;
		}

		@Override
		public void setReference(Integer toCompare) { throw new UnsupportedOperationException(); }

		@Override
		public boolean equalToReference(Integer candidate) { throw new UnsupportedOperationException(); }

		@Override
		public int compareToReference( TypeComparator<Integer> referencedComparator) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int compare(Integer first, Integer second) { throw new UnsupportedOperationException(); }

		@Override
		public int compareSerialized(DataInputView firstSource, DataInputView secondSource) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean supportsNormalizedKey() { throw new UnsupportedOperationException(); }

		@Override
		public boolean supportsSerializationWithKeyNormalization() { throw new UnsupportedOperationException(); }

		@Override
		public int getNormalizeKeyLen() { throw new UnsupportedOperationException(); }

		@Override
		public boolean isNormalizedKeyPrefixOnly(int keyBytes) { throw new UnsupportedOperationException(); }

		@Override
		public void putNormalizedKey(Integer record, MemorySegment target, int offset, int numBytes) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void writeWithKeyNormalization(Integer record, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Integer readWithKeyDenormalization(Integer reuse, DataInputView source) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean invertNormalizedKey() { throw new UnsupportedOperationException(); }

		@Override
		public TypeComparator<Integer> duplicate() { throw new UnsupportedOperationException(); }

		@Override
		public int extractKeys(Object record, Object[] target, int index) {
			target[index] = record;
			return 1;
		}

		@Override
		public TypeComparator[] getFlatComparators() {
			return comparators;
		}
	}
}
