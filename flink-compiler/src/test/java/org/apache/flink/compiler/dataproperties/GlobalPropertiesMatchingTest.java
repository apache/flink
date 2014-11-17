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

package org.apache.flink.compiler.dataproperties;

import static org.junit.Assert.*;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.junit.Test;

public class GlobalPropertiesMatchingTest {

	@Test
	public void testMatchingAnyPartitioning() {
		try {
			
			RequestedGlobalProperties req = new RequestedGlobalProperties();
			req.setAnyPartitioning(new FieldSet(6, 2));
			
			// match any partitioning
			{
				GlobalProperties gp1 = new GlobalProperties();
				gp1.setAnyPartitioning(new FieldList(2, 6));
				assertTrue(req.isMetBy(gp1));
				
				GlobalProperties gp2 = new GlobalProperties();
				gp2.setAnyPartitioning(new FieldList(6, 2));
				assertTrue(req.isMetBy(gp2));
				
				GlobalProperties gp3 = new GlobalProperties();
				gp3.setAnyPartitioning(new FieldList(6, 1));
				assertFalse(req.isMetBy(gp3));
				
				GlobalProperties gp4 = new GlobalProperties();
				gp4.setAnyPartitioning(new FieldList(2));
				assertTrue(req.isMetBy(gp4));
			}
			
			// match hash partitioning
			{
				GlobalProperties gp1 = new GlobalProperties();
				gp1.setHashPartitioned(new FieldList(2, 6));
				assertTrue(req.isMetBy(gp1));
				
				GlobalProperties gp2 = new GlobalProperties();
				gp2.setHashPartitioned(new FieldList(6, 2));
				assertTrue(req.isMetBy(gp2));
				
				GlobalProperties gp3 = new GlobalProperties();
				gp3.setHashPartitioned(new FieldList(6, 1));
				assertFalse(req.isMetBy(gp3));
			}
			
			// match range partitioning
			{
				GlobalProperties gp1 = new GlobalProperties();
				gp1.setRangePartitioned(new Ordering(2, null, Order.DESCENDING).appendOrdering(6, null, Order.ASCENDING));
				assertTrue(req.isMetBy(gp1));
				
				GlobalProperties gp2 = new GlobalProperties();
				gp2.setRangePartitioned(new Ordering(6, null, Order.DESCENDING).appendOrdering(2, null, Order.ASCENDING));
				assertTrue(req.isMetBy(gp2));

				GlobalProperties gp3 = new GlobalProperties();
				gp3.setRangePartitioned(new Ordering(6, null, Order.DESCENDING).appendOrdering(1, null, Order.ASCENDING));
				assertFalse(req.isMetBy(gp3));
				
				GlobalProperties gp4 = new GlobalProperties();
				gp4.setRangePartitioned(new Ordering(6, null, Order.DESCENDING));
				assertTrue(req.isMetBy(gp4));
			}
			
			// match custom partitioning
			{
				GlobalProperties gp1 = new GlobalProperties();
				gp1.setCustomPartitioned(new FieldList(2, 6), new MockPartitioner());
				assertTrue(req.isMetBy(gp1));
				
				GlobalProperties gp2 = new GlobalProperties();
				gp2.setCustomPartitioned(new FieldList(6, 2), new MockPartitioner());
				assertTrue(req.isMetBy(gp2));
				
				GlobalProperties gp3 = new GlobalProperties();
				gp3.setCustomPartitioned(new FieldList(6, 1), new MockPartitioner());
				assertFalse(req.isMetBy(gp3));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testMatchingCustomPartitioning() {
		try {
			final Partitioner<Long> partitioner = new MockPartitioner();
			
			RequestedGlobalProperties req = new RequestedGlobalProperties();
			req.setCustomPartitioned(new FieldSet(6, 2), partitioner);
			
			// match custom partitionings
			{
				GlobalProperties gp1 = new GlobalProperties();
				gp1.setCustomPartitioned(new FieldList(2, 6), partitioner);
				assertTrue(req.isMetBy(gp1));
				
				GlobalProperties gp2 = new GlobalProperties();
				gp2.setCustomPartitioned(new FieldList(6, 2), partitioner);
				assertTrue(req.isMetBy(gp2));
				
				GlobalProperties gp3 = new GlobalProperties();
				gp3.setCustomPartitioned(new FieldList(6, 2), new MockPartitioner());
				assertFalse(req.isMetBy(gp3));
			}
			
			// cannot match other types of partitionings
			{
				GlobalProperties gp1 = new GlobalProperties();
				gp1.setAnyPartitioning(new FieldList(6, 2));
				assertFalse(req.isMetBy(gp1));
				
				GlobalProperties gp2 = new GlobalProperties();
				gp2.setHashPartitioned(new FieldList(6, 2));
				assertFalse(req.isMetBy(gp2));
				
				GlobalProperties gp3 = new GlobalProperties();
				gp3.setRangePartitioned(new Ordering(2, null, Order.DESCENDING).appendOrdering(6, null, Order.ASCENDING));
				assertFalse(req.isMetBy(gp3));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------

}
