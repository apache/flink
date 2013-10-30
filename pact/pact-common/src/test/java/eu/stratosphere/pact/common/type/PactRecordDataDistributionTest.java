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

package eu.stratosphere.pact.common.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class PactRecordDataDistributionTest {

	@Test
	public void testConstructorSingleKey() {

		// check correct data distribution
		DataDistribution<PactRecord> dd = new PactRecordDataDistribution(new int[]{0}, 
				new Key[][] {{new PactInteger(1)}, {new PactInteger(2)}, {new PactInteger(3)}});
		Assert.assertTrue(dd != null);
		
		// check incorrect key types
		try {
			dd = new PactRecordDataDistribution(new int[]{0}, 
					new Key[][] {{new PactInteger(1)}, {new PactString("ABC")}, {new PactInteger(3)}});
			Assert.fail("Data distribution accepts inconsistent key types");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
		
		// check inconsistent number of keys
		try {
			dd = new PactRecordDataDistribution(new int[]{0}, 
					new Key[][] {{new PactInteger(1)}, {new PactInteger(2), new PactInteger(2)}, {new PactInteger(3)}});
			Assert.fail("Data distribution accepts inconsistent many keys");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
	}
	
	@Test 
	public void testConstructorMultiKey() {
		
		// check correct data distribution
		DataDistribution<PactRecord> dd = new PactRecordDataDistribution(new int[]{0,5,4}, 
				new Key[][] {{new PactInteger(1), new PactString("A"), new PactInteger(1)}, 
				             {new PactInteger(2), new PactString("A"), new PactInteger(1)}, 
				             {new PactInteger(3), new PactString("A"), new PactInteger(1)}});
		Assert.assertTrue(dd != null);
		
		// check inconsistent array parameter lengths
		try {
			dd = new PactRecordDataDistribution(new int[]{0,5,4}, 
					new Key[][] {{new PactInteger(1), new PactString("A")}, 
					             {new PactInteger(2), new PactString("A")}, 
					             {new PactInteger(3), new PactString("A")}});
			Assert.fail("Data distribution accepts key position and key boundary arrays of different length");
		} catch(IllegalArgumentException iae) {
			// do nothing 
		}
		
		// check inconsistent key types
		try {
			dd = new PactRecordDataDistribution(new int[]{0,5,4}, 
					new Key[][] {{new PactInteger(1), new PactString("A"), new PactDouble(1.3d)}, 
								 {new PactInteger(2), new PactString("B"), new PactInteger(1)}});
			Assert.fail("Data distribution accepts incorrect key types");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
		
		// check inconsistent number of keys
		try {
			dd = new PactRecordDataDistribution(new int[]{0,5}, 
					new Key[][] {{new PactInteger(1), new PactInteger(2)}, 
					             {new PactInteger(2), new PactInteger(2)}, 
					             {new PactInteger(3)}});
			Assert.fail("Data distribution accepts bucket boundaries with inconsistent many keys");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
		
	}
	
	@Test
	public void testWriteRead() {
		
		DataDistribution<PactRecord> ddWrite = new PactRecordDataDistribution(new int[]{0,5,4}, 
				new Key[][] {{new PactInteger(1), new PactString("A"), new PactInteger(1)}, 
				             {new PactInteger(2), new PactString("A"), new PactInteger(1)}, 
				             {new PactInteger(2), new PactString("B"), new PactInteger(4)},
				             {new PactInteger(2), new PactString("B"), new PactInteger(3)},
				             {new PactInteger(2), new PactString("B"), new PactInteger(2)}});
		
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);
		try {
			ddWrite.write(dos);
		} catch (IOException e) {
			Assert.fail("Error serializing the DataDistribution: " + e.getMessage());
		}

		byte[] seralizedDD = baos.toByteArray();
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(seralizedDD);
		final DataInputStream in = new DataInputStream(bais);
		
		DataDistribution<PactRecord> ddRead = new PactRecordDataDistribution();
		
		try {
			ddRead.read(in);
		} catch (Exception ex) {
			Assert.fail("The deserialization of the encoded data distribution caused an error");
		}
		
		// compare written and read distributions
		for(int i=0;i<6;i++) {
			PactRecord recW = ddWrite.getBucketBoundary(0, 6);
			PactRecord recR = ddWrite.getBucketBoundary(0, 6);
			
			Assert.assertTrue(recW.getField(0, PactInteger.class).compareTo(recR.getField(0,PactInteger.class)) == 0);
			Assert.assertTrue(recW.getField(5, PactString.class).compareTo(recR.getField(5,PactString.class)) == 0);
			Assert.assertTrue(recW.getField(4, PactInteger.class).compareTo(recR.getField(4,PactInteger.class)) == 0);
		}
	}
	
	@Test
	public void testGetBucketBoundary() {
		
		DataDistribution<PactRecord> dd = new PactRecordDataDistribution(new int[]{0,5}, 
				new Key[][] {{new PactInteger(1), new PactString("A")}, 
				             {new PactInteger(2), new PactString("B")}, 
				             {new PactInteger(3), new PactString("C")},
				             {new PactInteger(4), new PactString("D")},
				             {new PactInteger(5), new PactString("E")},
				             {new PactInteger(6), new PactString("F")},
				             {new PactInteger(7), new PactString("G")}});
		
		PactRecord boundRec = dd.getBucketBoundary(0, 8);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 1);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("A"));
		boundRec = dd.getBucketBoundary(1, 8);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 2);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("B"));
		boundRec = dd.getBucketBoundary(2, 8);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 3);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("C"));
		boundRec = dd.getBucketBoundary(3, 8);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 4);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("D"));
		boundRec = dd.getBucketBoundary(4, 8);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 5);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("E"));
		boundRec = dd.getBucketBoundary(5, 8);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 6);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("F"));
		boundRec = dd.getBucketBoundary(6, 8);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 7);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("G"));
		
		
		boundRec = dd.getBucketBoundary(0, 4);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 2);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("B"));
		boundRec = dd.getBucketBoundary(1, 4);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 4);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("D"));
		boundRec = dd.getBucketBoundary(2, 4);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 6);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("F"));
		
		boundRec = dd.getBucketBoundary(0, 2);
		Assert.assertEquals(boundRec.getField(0, PactInteger.class).getValue(), 4);
		Assert.assertTrue(boundRec.getField(5, PactString.class).getValue().equals("D"));
		
		try {
			boundRec = dd.getBucketBoundary(0, 7);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
		
		try {
			boundRec = dd.getBucketBoundary(3, 4);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
		
		try {
			boundRec = dd.getBucketBoundary(-1, 4);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
		
		try {
			boundRec = dd.getBucketBoundary(0, 0);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
	}

}
