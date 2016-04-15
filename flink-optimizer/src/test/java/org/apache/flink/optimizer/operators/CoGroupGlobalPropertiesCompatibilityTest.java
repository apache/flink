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

package org.apache.flink.optimizer.operators;

import static org.junit.Assert.*;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.junit.Test;

@SuppressWarnings("serial")
public class CoGroupGlobalPropertiesCompatibilityTest {

	@Test
	public void checkCompatiblePartitionings() {
		try {
			final FieldList keysLeft = new FieldList(1, 4);
			final FieldList keysRight = new FieldList(3, 1);
			
			CoGroupDescriptor descr = new CoGroupDescriptor(keysLeft, keysRight);
			
			// test compatible hash partitioning
			{
				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setHashPartitioned(keysLeft);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setHashPartitioned(keysRight);
				
				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setHashPartitioned(keysLeft);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setHashPartitioned(keysRight);
				
				assertTrue(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}
			
			// test compatible custom partitioning
			{
				Partitioner<Object> part = new Partitioner<Object>() {
					@Override
					public int partition(Object key, int numPartitions) {
						return 0;
					}
				};
				
				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setCustomPartitioned(keysLeft, part);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setCustomPartitioned(keysRight, part);
				
				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setCustomPartitioned(keysLeft, part);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setCustomPartitioned(keysRight, part);
				
				assertTrue(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}
			
			// test custom partitioning matching any partitioning
			{
				Partitioner<Object> part = new Partitioner<Object>() {
					@Override
					public int partition(Object key, int numPartitions) {
						return 0;
					}
				};
				
				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setAnyPartitioning(keysLeft);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setAnyPartitioning(keysRight);
				
				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setCustomPartitioned(keysLeft, part);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setCustomPartitioned(keysRight, part);
				
				assertTrue(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}

			TestDistribution dist1 = new TestDistribution(1);
			TestDistribution dist2 = new TestDistribution(1);
			
			// test compatible range partitioning with one ordering
			{
				Ordering ordering1 = new Ordering();
				for (int field : keysLeft) {
					ordering1.appendOrdering(field, null, Order.ASCENDING);
				}
				Ordering ordering2 = new Ordering();
				for (int field : keysRight) {
					ordering2.appendOrdering(field, null, Order.ASCENDING);
				}
				
				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setRangePartitioned(ordering1, dist1);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setRangePartitioned(ordering2, dist2);

				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setRangePartitioned(ordering1, dist1);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setRangePartitioned(ordering2, dist2);
				assertTrue(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}
			// test compatible range partitioning with two orderings
			{
				Ordering ordering1 = new Ordering();
				ordering1.appendOrdering(keysLeft.get(0), null, Order.DESCENDING);
				ordering1.appendOrdering(keysLeft.get(1), null, Order.ASCENDING);
				Ordering ordering2 = new Ordering();
				ordering2.appendOrdering(keysRight.get(0), null, Order.DESCENDING);
				ordering2.appendOrdering(keysRight.get(1), null, Order.ASCENDING);

				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setRangePartitioned(ordering1, dist1);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setRangePartitioned(ordering2, dist2);

				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setRangePartitioned(ordering1, dist1);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setRangePartitioned(ordering2, dist2);
				assertTrue(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void checkInompatiblePartitionings() {
		try {
			final FieldList keysLeft = new FieldList(1);
			final FieldList keysRight = new FieldList(3);
			
			final Partitioner<Object> part = new Partitioner<Object>() {
				@Override
				public int partition(Object key, int numPartitions) {
					return 0;
				}
			};
			final Partitioner<Object> part2 = new Partitioner<Object>() {
				@Override
				public int partition(Object key, int numPartitions) {
					return 0;
				}
			};
			
			CoGroupDescriptor descr = new CoGroupDescriptor(keysLeft, keysRight);
			
			// test incompatible hash with custom partitioning
			{
				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setAnyPartitioning(keysLeft);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setAnyPartitioning(keysRight);
				
				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setHashPartitioned(keysLeft);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setCustomPartitioned(keysRight, part);
				
				assertFalse(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}
			
			// test incompatible custom partitionings
			{
				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setAnyPartitioning(keysLeft);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setAnyPartitioning(keysRight);
				
				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setCustomPartitioned(keysLeft, part);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setCustomPartitioned(keysRight, part2);
				
				assertFalse(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}

			TestDistribution dist1 = new TestDistribution(1);
			TestDistribution dist2 = new TestDistribution(1);

			// test incompatible range partitioning with different key size
			{
				Ordering ordering1 = new Ordering();
				for (int field : keysLeft) {
					ordering1.appendOrdering(field, null, Order.ASCENDING);
				}
				Ordering ordering2 = new Ordering();
				for (int field : keysRight) {
					ordering1.appendOrdering(field, null, Order.ASCENDING);
					ordering2.appendOrdering(field, null, Order.ASCENDING);
				}

				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setRangePartitioned(ordering1, dist1);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setRangePartitioned(ordering2, dist2);

				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setRangePartitioned(ordering1, dist1);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setRangePartitioned(ordering2, dist2);
				assertFalse(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}

			// test incompatible range partitioning with different ordering
			{
				Ordering ordering1 = new Ordering();
				for (int field : keysLeft) {
					ordering1.appendOrdering(field, null, Order.ASCENDING);
				}
				Ordering ordering2 = new Ordering();
				for (int field : keysRight) {
					ordering2.appendOrdering(field, null, Order.DESCENDING);
				}

				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setRangePartitioned(ordering1, dist1);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setRangePartitioned(ordering2, dist2);

				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setRangePartitioned(ordering1, dist1);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setRangePartitioned(ordering2, dist2);
				assertFalse(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}

			TestDistribution dist3 = new TestDistribution(1);
			TestDistribution dist4 = new TestDistribution(2);

			// test incompatible range partitioning with different distribution
			{
				Ordering ordering1 = new Ordering();
				for (int field : keysLeft) {
					ordering1.appendOrdering(field, null, Order.ASCENDING);
				}
				Ordering ordering2 = new Ordering();
				for (int field : keysRight) {
					ordering2.appendOrdering(field, null, Order.ASCENDING);
				}

				RequestedGlobalProperties reqLeft = new RequestedGlobalProperties();
				reqLeft.setRangePartitioned(ordering1, dist3);
				RequestedGlobalProperties reqRight = new RequestedGlobalProperties();
				reqRight.setRangePartitioned(ordering2, dist4);

				GlobalProperties propsLeft = new GlobalProperties();
				propsLeft.setRangePartitioned(ordering1, dist3);
				GlobalProperties propsRight = new GlobalProperties();
				propsRight.setRangePartitioned(ordering2, dist4);
				assertFalse(descr.areCompatible(reqLeft, reqRight, propsLeft, propsRight));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
