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

package org.apache.flink.api.distributions;

import org.apache.flink.api.common.distributions.SimpleDistribution;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Key;
import org.apache.flink.types.StringValue;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SimpleDataDistributionTest {

	@Test
	public void testConstructorSingleKey() {

		// check correct data distribution
		try {
			SimpleDistribution dd = new SimpleDistribution(new Key<?>[] {new IntValue(1), new IntValue(2), new IntValue(3)});
			Assert.assertEquals(1, dd.getNumberOfFields());
		}
		catch (Throwable t) {
			Assert.fail();
		}
		
		// check incorrect key types
		try {
			new SimpleDistribution(new Key<?>[] {new IntValue(1), new StringValue("ABC"), new IntValue(3)});
			Assert.fail("Data distribution accepts inconsistent key types");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
		
		// check inconsistent number of keys
		try {
			new SimpleDistribution(new Key<?>[][] {{new IntValue(1)}, {new IntValue(2), new IntValue(2)}, {new IntValue(3)}});
			Assert.fail("Data distribution accepts inconsistent many keys");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
	}
	
	@Test 
	public void testConstructorMultiKey() {
		
		// check correct data distribution
		SimpleDistribution dd = new SimpleDistribution(
				new Key<?>[][] {{new IntValue(1), new StringValue("A"), new IntValue(1)}, 
							{new IntValue(2), new StringValue("A"), new IntValue(1)}, 
							{new IntValue(3), new StringValue("A"), new IntValue(1)}});
		Assert.assertEquals(3, dd.getNumberOfFields());
		
		// check inconsistent key types
		try {
			new SimpleDistribution( 
					new Key<?>[][] {{new IntValue(1), new StringValue("A"), new DoubleValue(1.3d)}, 
								{new IntValue(2), new StringValue("B"), new IntValue(1)}});
			Assert.fail("Data distribution accepts incorrect key types");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
		
		// check inconsistent number of keys
		try {
			new SimpleDistribution(
					new Key<?>[][] {{new IntValue(1), new IntValue(2)}, 
								{new IntValue(2), new IntValue(2)}, 
								{new IntValue(3)}});
			Assert.fail("Data distribution accepts bucket boundaries with inconsistent many keys");
		} catch(IllegalArgumentException iae) {
			// do nothing
		}
		
	}
	
	@Test
	public void testWriteRead() {
		
		SimpleDistribution ddWrite = new SimpleDistribution(
				new Key<?>[][] {{new IntValue(1), new StringValue("A"), new IntValue(1)}, 
							{new IntValue(2), new StringValue("A"), new IntValue(1)}, 
							{new IntValue(2), new StringValue("B"), new IntValue(4)},
							{new IntValue(2), new StringValue("B"), new IntValue(3)},
							{new IntValue(2), new StringValue("B"), new IntValue(2)}});
		Assert.assertEquals(3, ddWrite.getNumberOfFields());
		
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		try {
			ddWrite.write(new DataOutputViewStreamWrapper(baos));
		} catch (IOException e) {
			Assert.fail("Error serializing the DataDistribution: " + e.getMessage());
		}

		byte[] seralizedDD = baos.toByteArray();
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(seralizedDD);
		
		SimpleDistribution ddRead = new SimpleDistribution();
		
		try {
			ddRead.read(new DataInputViewStreamWrapper(bais));
		} catch (Exception ex) {
			Assert.fail("The deserialization of the encoded data distribution caused an error");
		}
		
		Assert.assertEquals(3, ddRead.getNumberOfFields());
		
		// compare written and read distributions
		for(int i=0;i<6;i++) {
			Key<?>[] recW = ddWrite.getBucketBoundary(0, 6);
			Key<?>[] recR = ddWrite.getBucketBoundary(0, 6);
			
			Assert.assertEquals(recW[0], recR[0]);
			Assert.assertEquals(recW[1], recR[1]);
			Assert.assertEquals(recW[2], recR[2]);
		}
	}
	
	@Test
	public void testGetBucketBoundary() {
		
		SimpleDistribution dd = new SimpleDistribution(
				new Key<?>[][] {{new IntValue(1), new StringValue("A")}, 
							{new IntValue(2), new StringValue("B")}, 
							{new IntValue(3), new StringValue("C")},
							{new IntValue(4), new StringValue("D")},
							{new IntValue(5), new StringValue("E")},
							{new IntValue(6), new StringValue("F")},
							{new IntValue(7), new StringValue("G")}});
		
		Key<?>[] boundRec = dd.getBucketBoundary(0, 8);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 1);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("A"));
		
		boundRec = dd.getBucketBoundary(1, 8);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 2);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("B"));
		
		boundRec = dd.getBucketBoundary(2, 8);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 3);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("C"));
		
		boundRec = dd.getBucketBoundary(3, 8);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 4);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("D"));
		
		boundRec = dd.getBucketBoundary(4, 8);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 5);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("E"));
		
		boundRec = dd.getBucketBoundary(5, 8);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 6);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("F"));
		
		boundRec = dd.getBucketBoundary(6, 8);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 7);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("G"));
		
		boundRec = dd.getBucketBoundary(0, 4);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 2);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("B"));
		
		boundRec = dd.getBucketBoundary(1, 4);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 4);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("D"));
		
		boundRec = dd.getBucketBoundary(2, 4);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 6);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("F"));
		
		boundRec = dd.getBucketBoundary(0, 2);
		Assert.assertEquals(((IntValue) boundRec[0]).getValue(), 4);
		Assert.assertTrue(((StringValue) boundRec[1]).getValue().equals("D"));
		
		try {
			dd.getBucketBoundary(0, 7);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
		
		try {
			dd.getBucketBoundary(3, 4);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
		
		try {
			dd.getBucketBoundary(-1, 4);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
		
		try {
			dd.getBucketBoundary(0, 0);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// nothing to do
		}
	}
}
