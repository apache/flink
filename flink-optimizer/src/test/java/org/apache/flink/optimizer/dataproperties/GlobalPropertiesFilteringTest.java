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

package org.apache.flink.optimizer.dataproperties;

import static org.junit.Assert.*;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class GlobalPropertiesFilteringTest {

	private TupleTypeInfo<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tupleInfo =
			new TupleTypeInfo<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
			);

	@Test
	public void testAllErased1() {

		SingleInputSemanticProperties semProps = new SingleInputSemanticProperties();

		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1));
		gprops.addUniqueFieldCombination(new FieldSet(3, 4));
		gprops.addUniqueFieldCombination(new FieldSet(5, 6));

		GlobalProperties result = gprops.filterBySemanticProperties(semProps, 0);

		assertEquals(PartitioningProperty.RANDOM_PARTITIONED, result.getPartitioning());
		assertNull(result.getPartitioningFields());
		assertNull(result.getPartitioningOrdering());
		assertNull(result.getUniqueFieldCombination());
	}

	@Test
	public void testAllErased2() {

		SingleInputSemanticProperties semProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(semProps, new String[]{"2"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1));
		gprops.addUniqueFieldCombination(new FieldSet(3, 4));
		gprops.addUniqueFieldCombination(new FieldSet(5, 6));

		GlobalProperties result = gprops.filterBySemanticProperties(semProps, 0);

		assertEquals(PartitioningProperty.RANDOM_PARTITIONED, result.getPartitioning());
		assertNull(result.getPartitioningFields());
		assertNull(result.getPartitioningOrdering());
		assertNull(result.getUniqueFieldCombination());
	}

	@Test
	public void testHashPartitioningPreserved1() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1;4"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 4));

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.HASH_PARTITIONED, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(3, pFields.size());
		assertTrue(pFields.contains(0));
		assertTrue(pFields.contains(1));
		assertTrue(pFields.contains(4));
	}

	@Test
	public void testHashPartitioningPreserved2() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 4));

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.HASH_PARTITIONED, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(3, pFields.size());
		assertTrue(pFields.contains(1));
		assertTrue(pFields.contains(2));
		assertTrue(pFields.contains(3));
	}

	@Test
	public void testHashPartitioningErased() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 4));

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.RANDOM_PARTITIONED, result.getPartitioning());
		assertNull(result.getPartitioningFields());
	}

	@Test
	public void testAnyPartitioningPreserved1() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1;4"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setAnyPartitioning(new FieldList(0, 1, 4));

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.ANY_PARTITIONING, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(3, pFields.size());
		assertTrue(pFields.contains(0));
		assertTrue(pFields.contains(1));
		assertTrue(pFields.contains(4));
	}

	@Test
	public void testAnyPartitioningPreserved2() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setAnyPartitioning(new FieldList(0, 1, 4));

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.ANY_PARTITIONING, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(3, pFields.size());
		assertTrue(pFields.contains(1));
		assertTrue(pFields.contains(2));
		assertTrue(pFields.contains(3));
	}

	@Test
	public void testAnyPartitioningErased() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setAnyPartitioning(new FieldList(0, 1, 4));

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.RANDOM_PARTITIONED, result.getPartitioning());
		assertNull(result.getPartitioningFields());
	}

	@Test
	public void testCustomPartitioningPreserved1() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1;4"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		Partitioner<Tuple2<Long, Integer>> myP = new MockPartitioner();
		gprops.setCustomPartitioned(new FieldList(0, 4), myP);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.CUSTOM_PARTITIONING, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(2, pFields.size());
		assertTrue(pFields.contains(0));
		assertTrue(pFields.contains(4));
		assertEquals(myP, result.getCustomPartitioner());
	}

	@Test
	public void testCustomPartitioningPreserved2() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		Partitioner<Tuple2<Long, Integer>> myP = new MockPartitioner();
		gprops.setCustomPartitioned(new FieldList(0, 4), myP);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.CUSTOM_PARTITIONING, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(2, pFields.size());
		assertTrue(pFields.contains(1));
		assertTrue(pFields.contains(3));
		assertEquals(myP, result.getCustomPartitioner());
	}

	@Test
	public void testCustomPartitioningErased() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		Partitioner<Tuple2<Long, Integer>> myP = new MockPartitioner();
		gprops.setCustomPartitioned(new FieldList(0, 4), myP);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.RANDOM_PARTITIONED, result.getPartitioning());
		assertNull(result.getPartitioningFields());
		assertNull(result.getCustomPartitioner());
	}

	@Test
	public void testRangePartitioningPreserved1() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"1;2;5"}, null, null, tupleInfo, tupleInfo);

		Ordering o = new Ordering();
		o.appendOrdering(1, IntValue.class, Order.ASCENDING);
		o.appendOrdering(5, LongValue.class, Order.DESCENDING);
		o.appendOrdering(2, StringValue.class, Order.ASCENDING);
		GlobalProperties gprops = new GlobalProperties();
		gprops.setRangePartitioned(o);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.RANGE_PARTITIONED, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(3, pFields.size());
		assertEquals(1, pFields.get(0).intValue());
		assertEquals(5, pFields.get(1).intValue());
		assertEquals(2, pFields.get(2).intValue());
		Ordering pOrder = result.getPartitioningOrdering();
		assertEquals(3, pOrder.getNumberOfFields());
		assertEquals(1, pOrder.getFieldNumber(0).intValue());
		assertEquals(5, pOrder.getFieldNumber(1).intValue());
		assertEquals(2, pOrder.getFieldNumber(2).intValue());
		assertEquals(Order.ASCENDING, pOrder.getOrder(0));
		assertEquals(Order.DESCENDING, pOrder.getOrder(1));
		assertEquals(Order.ASCENDING, pOrder.getOrder(2));
		assertEquals(IntValue.class, pOrder.getType(0));
		assertEquals(LongValue.class, pOrder.getType(1));
		assertEquals(StringValue.class, pOrder.getType(2));
	}

	@Test
	public void testRangePartitioningPreserved2() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"1->3; 2->0; 5->1"}, null, null, tupleInfo, tupleInfo);

		Ordering o = new Ordering();
		o.appendOrdering(1, IntValue.class, Order.ASCENDING);
		o.appendOrdering(5, LongValue.class, Order.DESCENDING);
		o.appendOrdering(2, StringValue.class, Order.ASCENDING);
		GlobalProperties gprops = new GlobalProperties();
		gprops.setRangePartitioned(o);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.RANGE_PARTITIONED, result.getPartitioning());
		FieldList pFields = result.getPartitioningFields();
		assertEquals(3, pFields.size());
		assertEquals(3, pFields.get(0).intValue());
		assertEquals(1, pFields.get(1).intValue());
		assertEquals(0, pFields.get(2).intValue());
		Ordering pOrder = result.getPartitioningOrdering();
		assertEquals(3, pOrder.getNumberOfFields());
		assertEquals(3, pOrder.getFieldNumber(0).intValue());
		assertEquals(1, pOrder.getFieldNumber(1).intValue());
		assertEquals(0, pOrder.getFieldNumber(2).intValue());
		assertEquals(Order.ASCENDING, pOrder.getOrder(0));
		assertEquals(Order.DESCENDING, pOrder.getOrder(1));
		assertEquals(Order.ASCENDING, pOrder.getOrder(2));
		assertEquals(IntValue.class, pOrder.getType(0));
		assertEquals(LongValue.class, pOrder.getType(1));
		assertEquals(StringValue.class, pOrder.getType(2));
	}

	@Test
	public void testRangePartitioningErased() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"1;5"}, null, null, tupleInfo, tupleInfo);

		Ordering o = new Ordering();
		o.appendOrdering(1, IntValue.class, Order.ASCENDING);
		o.appendOrdering(5, LongValue.class, Order.DESCENDING);
		o.appendOrdering(2, StringValue.class, Order.ASCENDING);
		GlobalProperties gprops = new GlobalProperties();
		gprops.setRangePartitioned(o);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.RANDOM_PARTITIONED, result.getPartitioning());
		assertNull(result.getPartitioningOrdering());
		assertNull(result.getPartitioningFields());
	}

	@Test
	public void testRebalancingPreserved() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setForcedRebalanced();

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		assertEquals(PartitioningProperty.FORCED_REBALANCED, result.getPartitioning());
		assertNull(result.getPartitioningFields());
	}

	@Test
	public void testUniqueFieldGroupsPreserved1() {
		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1;2;3;4"}, null, null, tupleInfo, tupleInfo);

		FieldSet set1 = new FieldSet(0, 1, 2);
		FieldSet set2 = new FieldSet(3, 4);
		FieldSet set3 = new FieldSet(4, 5, 6, 7);
		GlobalProperties gprops = new GlobalProperties();
		gprops.addUniqueFieldCombination(set1);
		gprops.addUniqueFieldCombination(set2);
		gprops.addUniqueFieldCombination(set3);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);
		Set<FieldSet> unique = result.getUniqueFieldCombination();
		FieldSet expected1 = new FieldSet(0, 1, 2);
		FieldSet expected2 = new FieldSet(3, 4);

		Assert.assertTrue(unique.size() == 2);
		Assert.assertTrue(unique.contains(expected1));
		Assert.assertTrue(unique.contains(expected2));
	}

	@Test
	public void testUniqueFieldGroupsPreserved2() {
		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0->5;1;2;3->6;4"}, null, null, tupleInfo, tupleInfo);

		FieldSet set1 = new FieldSet(0, 1, 2);
		FieldSet set2 = new FieldSet(3, 4);
		FieldSet set3 = new FieldSet(4, 5, 6, 7);
		GlobalProperties gprops = new GlobalProperties();
		gprops.addUniqueFieldCombination(set1);
		gprops.addUniqueFieldCombination(set2);
		gprops.addUniqueFieldCombination(set3);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);
		Set<FieldSet> unique = result.getUniqueFieldCombination();
		FieldSet expected1 = new FieldSet(1, 2, 5);
		FieldSet expected2 = new FieldSet(4, 6);

		Assert.assertTrue(unique.size() == 2);
		Assert.assertTrue(unique.contains(expected1));
		Assert.assertTrue(unique.contains(expected2));
	}

	@Test
	public void testUniqueFieldGroupsErased() {
		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0; 3; 5; 6; 7"}, null, null, tupleInfo, tupleInfo);

		FieldSet set1 = new FieldSet(0, 1, 2);
		FieldSet set2 = new FieldSet(3, 4);
		FieldSet set3 = new FieldSet(4, 5, 6, 7);
		GlobalProperties gprops = new GlobalProperties();
		gprops.addUniqueFieldCombination(set1);
		gprops.addUniqueFieldCombination(set2);
		gprops.addUniqueFieldCombination(set3);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);
		Assert.assertNull(result.getUniqueFieldCombination());
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testInvalidInputIndex() {

		SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sprops, new String[]{"0;1"}, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1));

		gprops.filterBySemanticProperties(sprops, 1);
	}

}
