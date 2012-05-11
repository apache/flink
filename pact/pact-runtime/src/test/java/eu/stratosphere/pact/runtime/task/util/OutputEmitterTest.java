/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.Test;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.common.type.DeserializationException;
import eu.stratosphere.pact.common.type.KeyFieldOutOfBoundsException;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class OutputEmitterTest extends TestCase
{
	@Test
	public static void testPartitionHash()
	{
		// Test for PactInteger
		@SuppressWarnings("unchecked")
		final PactRecordComparator intComp = new PactRecordComparator(new int[] {0}, new Class[] {PactInteger.class});
		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.PARTITION_HASH, intComp);

		int numChans = 100;
		int numRecs = 50000;
		int[] hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			PactInteger k = new PactInteger(i);
			PactRecord rec = new PactRecord(k);
			
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

		// Test for PactString
		@SuppressWarnings("unchecked")
		final PactRecordComparator stringComp = new PactRecordComparator(new int[] {0}, new Class[] {PactString.class});
		ChannelSelector<PactRecord> oe2 = new PactRecordOutputEmitter(ShipStrategy.PARTITION_HASH, stringComp);

		numChans = 100;
		numRecs = 10000;
		
		hit = new int[numChans];

		for (int i = 0; i < numRecs; i++) {
			PactString k = new PactString(i + "");
			PactRecord rec = new PactRecord(k);
				
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
	public static void testForward()
	{
		// Test for PactInteger
		@SuppressWarnings("unchecked")
		final PactRecordComparator intComp = new PactRecordComparator(new int[] {0}, new Class[] {PactInteger.class});
		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.FORWARD, intComp);

		int numChannels = 100;
		int numRecords = 50000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			PactInteger k = new PactInteger(i);
			PactRecord rec = new PactRecord(k);
			
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

		// Test for PactString
		@SuppressWarnings("unchecked")
		final PactRecordComparator stringComp = new PactRecordComparator(new int[] {0}, new Class[] {PactString.class});
		final ChannelSelector<PactRecord> oe2 = new PactRecordOutputEmitter(ShipStrategy.FORWARD, stringComp);

		numChannels = 100;
		numRecords = 10000;
		
		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			PactString k = new PactString(i + "");
			PactRecord rec = new PactRecord(k);
				
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
	public static void testBroadcast()
	{
		// Test for PactInteger
		@SuppressWarnings("unchecked")
		final PactRecordComparator intComp = new PactRecordComparator(new int[] {0}, new Class[] {PactInteger.class});
		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.BROADCAST, intComp);

		int numChannels = 100;
		int numRecords = 50000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			PactInteger k = new PactInteger(i);
			PactRecord rec = new PactRecord(k);
			
			int[] chans = oe1.selectChannels(rec, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i]+"", hit[i] == numRecords);
		}
		
		// Test for PactString
		@SuppressWarnings("unchecked")
		final PactRecordComparator stringComp = new PactRecordComparator(new int[] {0}, new Class[] {PactString.class});
		final ChannelSelector<PactRecord> oe2 = new PactRecordOutputEmitter(ShipStrategy.BROADCAST, stringComp);

		numChannels = 100;
		numRecords = 5000;
		
		hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			PactString k = new PactString(i + "");
			PactRecord rec = new PactRecord(k);
				
			int[] chans = oe2.selectChannels(rec, hit.length);
			for(int j=0; j < chans.length; j++) {
				hit[chans[j]]++;
			}
		}

		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i]+"", hit[i] == numRecords);
		}
	}
	
//	@Test
//	public static void testPartitionRange()
//	{
//		// Test for PactInteger
//		@SuppressWarnings("unchecked")
//		final PactRecordComparator intComp = new PactRecordComparator(new int[] {0}, new Class[] {PactInteger.class});
//		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.PARTITION_HASH, intComp);
//
//		boolean correctException = false;
//		try {
//			oe1.selectChannels(new PactRecord(new PactInteger(1)), 2);
//		} catch(RuntimeException re) {
//			if(re.getMessage().equals("Partition function for RangePartitioner not set!"))
//				correctException = true;
//		}
//		assertTrue(correctException);
//		
//		// TODO: extend!
//	}
	
	@Test
	public static void testMultiKeys()
	{	
		@SuppressWarnings("unchecked")
		final PactRecordComparator multiComp = new PactRecordComparator(new int[] {0,1,3}, new Class[] {PactInteger.class, PactString.class, PactDouble.class});
		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.PARTITION_HASH, multiComp);

		int numChannels = 100;
		int numRecords = 5000;
		
		int[] hit = new int[numChannels];

		for (int i = 0; i < numRecords; i++) {
			PactRecord rec = new PactRecord(4);
			rec.setField(0, new PactInteger(i));
			rec.setField(1, new PactString("AB"+i+"CD"+i));
			rec.setField(3, new PactDouble(i*3.141d));
			
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
	public static void testMissingKey()
	{
		// Test for PactInteger
		@SuppressWarnings("unchecked")
		final PactRecordComparator intComp = new PactRecordComparator(new int[] {1}, new Class[] {PactInteger.class});
		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.PARTITION_HASH, intComp);

		PactRecord rec = new PactRecord(0);
		rec.setField(0, new PactInteger(1));
		
		try {
			oe1.selectChannels(rec, 100);
		} catch (KeyFieldOutOfBoundsException re) {
			Assert.assertEquals(1, re.getFieldNumber());
			return;
		}
		Assert.fail("Expected a KeyFieldOutOfBoundsException.");
	}
	
	@Test
	public static void testNullKey()
	{
		// Test for PactInteger
		@SuppressWarnings("unchecked")
		final PactRecordComparator intComp = new PactRecordComparator(new int[] {0}, new Class[] {PactInteger.class});
		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.PARTITION_HASH, intComp);

		PactRecord rec = new PactRecord(2);
		rec.setField(1, new PactInteger(1));

		try {
			oe1.selectChannels(rec, 100);
		} catch (NullKeyFieldException re) {
			Assert.assertEquals(0, re.getFieldNumber());
			return;
		}
		Assert.fail("Expected a NullKeyFieldException.");
	}
	
	@Test
	public static void testWrongKeyClass() {
		
		// Test for PactInteger
		@SuppressWarnings("unchecked")
		final PactRecordComparator intComp = new PactRecordComparator(new int[] {0}, new Class[] {PactDouble.class});
		final ChannelSelector<PactRecord> oe1 = new PactRecordOutputEmitter(ShipStrategy.PARTITION_HASH, intComp);

		PipedInputStream pipedInput = new PipedInputStream(1024*1024);
		DataInputStream in = new DataInputStream(pipedInput);
		DataOutputStream out;
		PactRecord rec = null;
		
		try {
			out = new DataOutputStream(new PipedOutputStream(pipedInput));
			
			rec = new PactRecord(1);
			rec.setField(0, new PactInteger());
			
			rec.write(out);
			rec = new PactRecord();
			rec.read(in);
		
		} catch (IOException e) {
			fail("Test erroneous");
		}

		try {
			oe1.selectChannels(rec, 100);
		} catch (DeserializationException re) {
			return;
//			if(re.getMessage().equals("Key field 0 of type 'eu.stratosphere.pact.common.type.base.PactDouble' could not be deserialized."))
//				correctException = true;
		}
		Assert.fail("Expected a NullKeyFieldException.");
	}

}
