/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.lang.NotImplementedException;
import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializerFactory;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.pact.runtime.shipping.OutputEmitter;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.types.DeserializationException;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.KeyFieldOutOfBoundsException;
import eu.stratosphere.types.NullKeyFieldException;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class OutputEmitterTest extends TestCase {
	
//	private static final long SEED = 485213591485399L;
	
	@Test
	public void testPartitionHash() {
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe1 = new OutputEmitter<Record>(ShipStrategyType.PARTITION_HASH, intComp);
		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
		
		int numChans = 100;
		int numRecs = 50000;
		int[] hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);
			
			delegate.setInstance(rec);
			
			int[] chans = oe1.selectChannels(delegate, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		int cnt = 0;
		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i] > 0);
			cnt += hit[i];
		}
		assertTrue(cnt == numRecs);

		// Test for StringValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> stringComp = new RecordComparatorFactory(new int[] {0}, new Class[] {StringValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe2 = new OutputEmitter<Record>(ShipStrategyType.PARTITION_HASH, stringComp);

		numChans = 100;
		numRecs = 10000;
		
		hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);
			delegate.setInstance(rec);
				
			int[] chans = oe2.selectChannels(delegate, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		cnt = 0;
		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i] > 0);
			cnt += hit[i];
		}
		assertTrue(cnt == numRecs);
		
		// test hash corner cases
		final TestIntComparator testIntComp = new TestIntComparator();
		final ChannelSelector<SerializationDelegate<Integer>> oe3 = new OutputEmitter<Integer>(ShipStrategyType.PARTITION_HASH, testIntComp);
		final SerializationDelegate<Integer> intDel = new SerializationDelegate<Integer>(new IntSerializer());
		
		numChans = 100;
		
		// MinVal hash
		intDel.setInstance(Integer.MIN_VALUE);
		int[] chans = oe3.selectChannels(intDel, numChans);
		assertTrue(chans.length == 1);
		assertTrue(chans[0] >= 0 && chans[0] <= numChans-1);
		
		// -1 hash
		intDel.setInstance(-1);
		chans = oe3.selectChannels(intDel, hit.length);
		assertTrue(chans.length == 1);
		assertTrue(chans[0] >= 0 && chans[0] <= numChans-1);
		
		// 0 hash
		intDel.setInstance(0);
		chans = oe3.selectChannels(intDel, hit.length);
		assertTrue(chans.length == 1);
		assertTrue(chans[0] >= 0 && chans[0] <= numChans-1);
		
		// 1 hash
		intDel.setInstance(1);
		chans = oe3.selectChannels(intDel, hit.length);
		assertTrue(chans.length == 1);
		assertTrue(chans[0] >= 0 && chans[0] <= numChans-1);
		
		// MaxVal hash
		intDel.setInstance(Integer.MAX_VALUE);
		chans = oe3.selectChannels(intDel, hit.length);
		assertTrue(chans.length == 1);
		assertTrue(chans[0] >= 0 && chans[0] <= numChans-1);
	}
	
	@Test
	public void testForward() {
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe1 = new OutputEmitter<Record>(ShipStrategyType.FORWARD, intComp);
		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
		
		int numChannels = 100;
		int numRecords = 50000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);
			delegate.setInstance(rec);
			
			int[] chans = oe1.selectChannels(delegate, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		int cnt = 0;
		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i] == (numRecords/numChannels) || hit[i] == (numRecords/numChannels)-1);
			cnt += hit[i];
		}
		assertTrue(cnt == numRecords);

		// Test for StringValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> stringComp = new RecordComparatorFactory(new int[] {0}, new Class[] {StringValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe2 = new OutputEmitter<Record>(ShipStrategyType.FORWARD, stringComp);

		numChannels = 100;
		numRecords = 10000;
		
		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);
			delegate.setInstance(rec);
				
			int[] chans = oe2.selectChannels(delegate, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		cnt = 0;
		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i] == (numRecords/numChannels) || hit[i] == (numRecords/numChannels)-1);
			cnt += hit[i];
		}
		assertTrue(cnt == numRecords);
		
	}
	
	@Test
	public void testBroadcast() {
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe1 = new OutputEmitter<Record>(ShipStrategyType.BROADCAST, intComp);
		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
		
		int numChannels = 100;
		int numRecords = 50000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);
			delegate.setInstance(rec);
			
			int[] chans = oe1.selectChannels(delegate, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i]+"", hit[i] == numRecords);
		}
		
		// Test for StringValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> stringComp = new RecordComparatorFactory(new int[] {0}, new Class[] {StringValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe2 = new OutputEmitter<Record>(ShipStrategyType.BROADCAST, stringComp);

		numChannels = 100;
		numRecords = 5000;
		
		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);
			delegate.setInstance(rec);
				
			int[] chans = oe2.selectChannels(delegate, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i]+"", hit[i] == numRecords);
		}
	}
	
	@Test
	public void testMultiKeys() {
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> multiComp = new RecordComparatorFactory(new int[] {0,1,3}, new Class[] {IntValue.class, StringValue.class, DoubleValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe1 = new OutputEmitter<Record>(ShipStrategyType.PARTITION_HASH, multiComp);
		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
		
		int numChannels = 100;
		int numRecords = 5000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			Record rec = new Record(4);
			rec.setField(0, new IntValue(i));
			rec.setField(1, new StringValue("AB"+i+"CD"+i));
			rec.setField(3, new DoubleValue(i*3.141d));
			delegate.setInstance(rec);
			
			int[] chans = oe1.selectChannels(delegate, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		int cnt = 0;
		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i] > 0);
			cnt += hit[i];
		}
		assertTrue(cnt == numRecords);
		
	}
	
	@Test
	public void testMissingKey() {
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {1}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe1 = new OutputEmitter<Record>(ShipStrategyType.PARTITION_HASH, intComp);
		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
		
		Record rec = new Record(0);
		rec.setField(0, new IntValue(1));
		delegate.setInstance(rec);
		
		try {
			oe1.selectChannels(delegate, 100);
		} catch (KeyFieldOutOfBoundsException re) {
			Assert.assertEquals(1, re.getFieldNumber());
			return;
		}
		Assert.fail("Expected a KeyFieldOutOfBoundsException.");
	}
	
	@Test
	public void testNullKey() {
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe1 = new OutputEmitter<Record>(ShipStrategyType.PARTITION_HASH, intComp);
		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
		
		Record rec = new Record(2);
		rec.setField(1, new IntValue(1));
		delegate.setInstance(rec);

		try {
			oe1.selectChannels(delegate, 100);
		} catch (NullKeyFieldException re) {
			Assert.assertEquals(0, re.getFieldNumber());
			return;
		}
		Assert.fail("Expected a NullKeyFieldException.");
	}
	
	@Test
	public void testWrongKeyClass() {
		
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final TypeComparator<Record> doubleComp = new RecordComparatorFactory(new int[] {0}, new Class[] {DoubleValue.class}).createComparator();
		final ChannelSelector<SerializationDelegate<Record>> oe1 = new OutputEmitter<Record>(ShipStrategyType.PARTITION_HASH, doubleComp);
		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
		
		PipedInputStream pipedInput = new PipedInputStream(1024*1024);
		DataInputStream in = new DataInputStream(pipedInput);
		DataOutputStream out;
		Record rec = null;
		
		try {
			out = new DataOutputStream(new PipedOutputStream(pipedInput));
			
			rec = new Record(1);
			rec.setField(0, new IntValue());
			
			rec.write(out);
			rec = new Record();
			rec.read(in);
		
		} catch (IOException e) {
			fail("Test erroneous");
		}

		try {
			delegate.setInstance(rec);
			oe1.selectChannels(delegate, 100);
		} catch (DeserializationException re) {
			return;
		}
		Assert.fail("Expected a NullKeyFieldException.");
	}
	
	@SuppressWarnings("serial")
	private static class TestIntComparator extends TypeComparator<Integer> {

		@Override
		public int hash(Integer record) {
			return record;
		}

		@Override
		public void setReference(Integer toCompare) { throw new NotImplementedException(); }

		@Override
		public boolean equalToReference(Integer candidate) { throw new NotImplementedException(); }

		@Override
		public int compareToReference( TypeComparator<Integer> referencedComparator) {
			throw new NotImplementedException();
		}

		@Override
		public int compare(Integer first, Integer second) { throw new NotImplementedException(); }

		@Override
		public int compare(DataInputView firstSource, DataInputView secondSource) {
			throw new NotImplementedException();
		}

		@Override
		public boolean supportsNormalizedKey() { throw new NotImplementedException(); }

		@Override
		public boolean supportsSerializationWithKeyNormalization() { throw new NotImplementedException(); }

		@Override
		public int getNormalizeKeyLen() { throw new NotImplementedException(); }

		@Override
		public boolean isNormalizedKeyPrefixOnly(int keyBytes) { throw new NotImplementedException(); }

		@Override
		public void putNormalizedKey(Integer record, MemorySegment target, int offset, int numBytes) {
			throw new NotImplementedException();
		}

		@Override
		public void writeWithKeyNormalization(Integer record, DataOutputView target) throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public Integer readWithKeyDenormalization(Integer reuse, DataInputView source) throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public boolean invertNormalizedKey() { throw new NotImplementedException(); }

		@Override
		public TypeComparator<Integer> duplicate() { throw new NotImplementedException(); }
		
	}
	
//	@Test
//	public void testPartitionRange() {
//		final Random rnd = new Random(SEED);
//		
//		final int DISTR_MIN = 0;
//		final int DISTR_MAX = 1000000;
//		final int DISTR_RANGE = DISTR_MAX - DISTR_MIN + 1;
//		final int NUM_BUCKETS = 137;
//		final float BUCKET_WIDTH = DISTR_RANGE / ((float) NUM_BUCKETS);
//		
//		final int NUM_ELEMENTS = 10000000;
//		
//		final DataDistribution distri = new UniformIntegerDistribution(DISTR_MIN, DISTR_MAX);
//		
//		@SuppressWarnings("unchecked")
//		final TypeComparator<Record> intComp = new RecordComparatorFactory(new int[] {0}, new Class[] {IntValue.class}).createComparator();
//		final ChannelSelector<SerializationDelegate<Record>> oe = new OutputEmitter<Record>(ShipStrategyType.PARTITION_RANGE, intComp, distri);
//		final SerializationDelegate<Record> delegate = new SerializationDelegate<Record>(new RecordSerializerFactory().getSerializer());
//		
//		final IntValue integer = new IntValue();
//		final Record rec = new Record();
//		
//		for (int i = 0; i < NUM_ELEMENTS; i++) {
//			final int nextValue = rnd.nextInt(DISTR_RANGE) + DISTR_MIN;
//			integer.setValue(nextValue);
//			rec.setField(0, integer);
//			delegate.setInstance(rec);
//			
//			final int[] channels = oe.selectChannels(delegate, NUM_BUCKETS);
//			if (channels.length != 1) {
//				Assert.fail("Resulting channels array has more than one channel.");
//			}
//			
//			final int bucket = channels[0];
//			final int shouldBeBucket = (int) ((nextValue - DISTR_MIN) / BUCKET_WIDTH);
//			
//			if (shouldBeBucket != bucket) {
//				// we may have a rounding imprecision in the 'should be bucket' computation.
//				final int lowerBoundaryForSelectedBucket = DISTR_MIN + (int) ((bucket    ) * BUCKET_WIDTH);
//				final int upperBoundaryForSelectedBucket = DISTR_MIN + (int) ((bucket + 1) * BUCKET_WIDTH);
//				if (nextValue <= lowerBoundaryForSelectedBucket || nextValue > upperBoundaryForSelectedBucket) {
//					Assert.fail("Wrong bucket selected");
//				}
//			}
//			
//		}
//	}
	
}
