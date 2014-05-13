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
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.Test;

import eu.stratosphere.api.common.distributions.DataDistribution;
import eu.stratosphere.api.common.distributions.UniformIntegerDistribution;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparator;
import eu.stratosphere.pact.runtime.shipping.RecordOutputEmitter;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.types.DeserializationException;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.KeyFieldOutOfBoundsException;
import eu.stratosphere.types.NullKeyFieldException;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class RecordOutputEmitterTest extends TestCase {
	
	private static final long SEED = 485213591485399L;
	
	@Test
	public void testPartitionHash() {
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final RecordComparator intComp = new RecordComparator(new int[] {0}, new Class[] {IntValue.class});
		final ChannelSelector<Record> oe1 = new RecordOutputEmitter(ShipStrategyType.PARTITION_HASH, intComp);

		int numChans = 100;
		int numRecs = 50000;
		int[] hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);
			
			int[] chans = oe1.selectChannels(rec, hit.length);
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
		final RecordComparator stringComp = new RecordComparator(new int[] {0}, new Class[] {StringValue.class});
		ChannelSelector<Record> oe2 = new RecordOutputEmitter(ShipStrategyType.PARTITION_HASH, stringComp);

		numChans = 100;
		numRecs = 10000;
		
		hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);
				
			int[] chans = oe2.selectChannels(rec, hit.length);
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
		
	}
	
	@Test
	public void testForward() {
		// Test for IntValue
		@SuppressWarnings("unchecked")
		final RecordComparator intComp = new RecordComparator(new int[] {0}, new Class[] {IntValue.class});
		final ChannelSelector<Record> oe1 = new RecordOutputEmitter(ShipStrategyType.FORWARD, intComp);

		int numChannels = 100;
		int numRecords = 50000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);
			
			int[] chans = oe1.selectChannels(rec, hit.length);
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
		final RecordComparator stringComp = new RecordComparator(new int[] {0}, new Class[] {StringValue.class});
		final ChannelSelector<Record> oe2 = new RecordOutputEmitter(ShipStrategyType.FORWARD, stringComp);

		numChannels = 100;
		numRecords = 10000;
		
		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);
				
			int[] chans = oe2.selectChannels(rec, hit.length);
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
		final RecordComparator intComp = new RecordComparator(new int[] {0}, new Class[] {IntValue.class});
		final ChannelSelector<Record> oe1 = new RecordOutputEmitter(ShipStrategyType.BROADCAST, intComp);

		int numChannels = 100;
		int numRecords = 50000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			IntValue k = new IntValue(i);
			Record rec = new Record(k);
			
			int[] chans = oe1.selectChannels(rec, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i]+"", hit[i] == numRecords);
		}
		
		// Test for StringValue
		@SuppressWarnings("unchecked")
		final RecordComparator stringComp = new RecordComparator(new int[] {0}, new Class[] {StringValue.class});
		final ChannelSelector<Record> oe2 = new RecordOutputEmitter(ShipStrategyType.BROADCAST, stringComp);

		numChannels = 100;
		numRecords = 5000;
		
		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			StringValue k = new StringValue(i + "");
			Record rec = new Record(k);
				
			int[] chans = oe2.selectChannels(rec, hit.length);
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
		final RecordComparator multiComp = new RecordComparator(new int[] {0,1,3}, new Class[] {IntValue.class, StringValue.class, DoubleValue.class});
		final ChannelSelector<Record> oe1 = new RecordOutputEmitter(ShipStrategyType.PARTITION_HASH, multiComp);

		int numChannels = 100;
		int numRecords = 5000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			Record rec = new Record(4);
			rec.setField(0, new IntValue(i));
			rec.setField(1, new StringValue("AB"+i+"CD"+i));
			rec.setField(3, new DoubleValue(i*3.141d));
			
			int[] chans = oe1.selectChannels(rec, hit.length);
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
		final RecordComparator intComp = new RecordComparator(new int[] {1}, new Class[] {IntValue.class});
		final ChannelSelector<Record> oe1 = new RecordOutputEmitter(ShipStrategyType.PARTITION_HASH, intComp);

		Record rec = new Record(0);
		rec.setField(0, new IntValue(1));
		
		try {
			oe1.selectChannels(rec, 100);
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
		final RecordComparator intComp = new RecordComparator(new int[] {0}, new Class[] {IntValue.class});
		final ChannelSelector<Record> oe1 = new RecordOutputEmitter(ShipStrategyType.PARTITION_HASH, intComp);

		Record rec = new Record(2);
		rec.setField(1, new IntValue(1));

		try {
			oe1.selectChannels(rec, 100);
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
		final RecordComparator doubleComp = new RecordComparator(new int[] {0}, new Class[] {DoubleValue.class});
		final ChannelSelector<Record> oe1 = new RecordOutputEmitter(ShipStrategyType.PARTITION_HASH, doubleComp);

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
			oe1.selectChannels(rec, 100);
		} catch (DeserializationException re) {
			return;
		}
		Assert.fail("Expected a NullKeyFieldException.");
	}
	
	@Test
	public void testPartitionRange() {
		final Random rnd = new Random(SEED);
		
		final int DISTR_MIN = 0;
		final int DISTR_MAX = 1000000;
		final int DISTR_RANGE = DISTR_MAX - DISTR_MIN + 1;
		final int NUM_BUCKETS = 137;
		final double BUCKET_WIDTH = DISTR_RANGE / ((double) NUM_BUCKETS);
		
		final int NUM_ELEMENTS = 10000000;
		
		final DataDistribution distri = new UniformIntegerDistribution(DISTR_MIN, DISTR_MAX);
		
		@SuppressWarnings("unchecked")
		final RecordComparator intComp = new RecordComparator(new int[] {0}, new Class[] {IntValue.class});
		final ChannelSelector<Record> oe = new RecordOutputEmitter(ShipStrategyType.PARTITION_RANGE, intComp, distri);
		
		final IntValue integer = new IntValue();
		final Record rec = new Record();
		
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			final int nextValue = rnd.nextInt(DISTR_RANGE) + DISTR_MIN;
			integer.setValue(nextValue);
			rec.setField(0, integer);
			
			final int[] channels = oe.selectChannels(rec, NUM_BUCKETS);
			if (channels.length != 1) {
				Assert.fail("Resulting channels array has more than one channel.");
			}
			
			final int bucket = channels[0];
			final int shouldBeBucket = (int) ((nextValue - DISTR_MIN) / BUCKET_WIDTH);
			
			if (shouldBeBucket != bucket) {
				// we may have a rounding imprecision in the 'should be bucket' computation.
				final int lowerBoundaryForSelectedBucket = DISTR_MIN + (int) ((bucket    ) * BUCKET_WIDTH);
				final int upperBoundaryForSelectedBucket = DISTR_MIN + (int) ((bucket + 1) * BUCKET_WIDTH);
				if (nextValue <= lowerBoundaryForSelectedBucket || nextValue > upperBoundaryForSelectedBucket) {
					Assert.fail("Wrong bucket selected");
				}
			}
			
		}
	}
}
