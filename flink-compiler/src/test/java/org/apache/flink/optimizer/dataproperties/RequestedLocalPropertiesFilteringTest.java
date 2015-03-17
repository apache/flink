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

import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.junit.Test;

public class RequestedLocalPropertiesFilteringTest {

	private TupleTypeInfo<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tupleInfo =
			new TupleTypeInfo<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
			);

	@Test(expected = NullPointerException.class)
	public void testNullProps() {

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setGroupedFields(new FieldSet(0, 2, 3));

		rlProp.filterBySemanticProperties(null, 0);
	}

	@Test
	public void testAllErased() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setGroupedFields(new FieldSet(0, 2, 3));

		RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

		assertNull(filtered);
	}

	@Test
	public void testGroupingPreserved1() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sProps, new String[]{"0;2;3"}, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setGroupedFields(new FieldSet(0, 2, 3));

		RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

		assertNotNull(filtered);
		assertNotNull(filtered.getGroupedFields());
		assertEquals(3, filtered.getGroupedFields().size());
		assertTrue(filtered.getGroupedFields().contains(0));
		assertTrue(filtered.getGroupedFields().contains(2));
		assertTrue(filtered.getGroupedFields().contains(3));
		assertNull(filtered.getOrdering());
	}

	@Test
	public void testGroupingPreserved2() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sProps, new String[]{"3->0;5->2;1->3"}, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setGroupedFields(new FieldSet(0, 2, 3));

		RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

		assertNotNull(filtered);
		assertNotNull(filtered.getGroupedFields());
		assertEquals(3, filtered.getGroupedFields().size());
		assertTrue(filtered.getGroupedFields().contains(3));
		assertTrue(filtered.getGroupedFields().contains(5));
		assertTrue(filtered.getGroupedFields().contains(1));
		assertNull(filtered.getOrdering());
	}

	@Test
	public void testGroupingErased() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sProps, new String[]{"0;2"}, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setGroupedFields(new FieldSet(0, 2, 3));

		RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

		assertNull(filtered);
	}

	@Test
	public void testOrderPreserved1() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sProps, new String[]{"1;4;6"}, null, null, tupleInfo, tupleInfo);

		Ordering o = new Ordering();
		o.appendOrdering(4, LongValue.class, Order.DESCENDING);
		o.appendOrdering(1, IntValue.class, Order.ASCENDING);
		o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setOrdering(o);

		RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

		assertNotNull(filtered);
		assertNotNull(filtered.getOrdering());
		assertEquals(3, filtered.getOrdering().getNumberOfFields());
		assertEquals(4, filtered.getOrdering().getFieldNumber(0).intValue());
		assertEquals(1, filtered.getOrdering().getFieldNumber(1).intValue());
		assertEquals(6, filtered.getOrdering().getFieldNumber(2).intValue());
		assertEquals(LongValue.class, filtered.getOrdering().getType(0));
		assertEquals(IntValue.class, filtered.getOrdering().getType(1));
		assertEquals(ByteValue.class, filtered.getOrdering().getType(2));
		assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(0));
		assertEquals(Order.ASCENDING, filtered.getOrdering().getOrder(1));
		assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(2));
		assertNull(filtered.getGroupedFields());
	}

	@Test
	public void testOrderPreserved2() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sProps, new String[]{"5->1;0->4;2->6"}, null, null, tupleInfo, tupleInfo);

		Ordering o = new Ordering();
		o.appendOrdering(4, LongValue.class, Order.DESCENDING);
		o.appendOrdering(1, IntValue.class, Order.ASCENDING);
		o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setOrdering(o);

		RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

		assertNotNull(filtered);
		assertNotNull(filtered.getOrdering());
		assertEquals(3, filtered.getOrdering().getNumberOfFields());
		assertEquals(0, filtered.getOrdering().getFieldNumber(0).intValue());
		assertEquals(5, filtered.getOrdering().getFieldNumber(1).intValue());
		assertEquals(2, filtered.getOrdering().getFieldNumber(2).intValue());
		assertEquals(LongValue.class, filtered.getOrdering().getType(0));
		assertEquals(IntValue.class, filtered.getOrdering().getType(1));
		assertEquals(ByteValue.class, filtered.getOrdering().getType(2));
		assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(0));
		assertEquals(Order.ASCENDING, filtered.getOrdering().getOrder(1));
		assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(2));
		assertNull(filtered.getGroupedFields());
	}

	@Test
	public void testOrderErased() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sProps, new String[]{"1; 4"}, null, null, tupleInfo, tupleInfo);

		Ordering o = new Ordering();
		o.appendOrdering(4, LongValue.class, Order.DESCENDING);
		o.appendOrdering(1, IntValue.class, Order.ASCENDING);
		o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setOrdering(o);

		RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

		assertNull(filtered);
	}

	@Test
	public void testDualGroupingPreserved() {

		DualInputSemanticProperties dprops = new DualInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsDualFromString(dprops, new String[]{"1->0;3;2->4"}, new String[]{"0->7;1"},
				null, null, null, null, tupleInfo, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops1 = new RequestedLocalProperties();
		lprops1.setGroupedFields(new FieldSet(0,3,4));

		RequestedLocalProperties lprops2 = new RequestedLocalProperties();
		lprops2.setGroupedFields(new FieldSet(7, 1));

		RequestedLocalProperties filtered1 = lprops1.filterBySemanticProperties(dprops, 0);
		RequestedLocalProperties filtered2 = lprops2.filterBySemanticProperties(dprops, 1);

		assertNotNull(filtered1);
		assertNotNull(filtered1.getGroupedFields());
		assertEquals(3, filtered1.getGroupedFields().size());
		assertTrue(filtered1.getGroupedFields().contains(1));
		assertTrue(filtered1.getGroupedFields().contains(2));
		assertTrue(filtered1.getGroupedFields().contains(3));
		assertNull(filtered1.getOrdering());

		assertNotNull(filtered2);
		assertNotNull(filtered2.getGroupedFields());
		assertEquals(2, filtered2.getGroupedFields().size());
		assertTrue(filtered2.getGroupedFields().contains(0));
		assertTrue(filtered2.getGroupedFields().contains(1));
		assertNull(filtered2.getOrdering());
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testInvalidInputIndex() {

		SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsSingleFromString(sProps, new String[]{"1; 4"}, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties rlProp = new RequestedLocalProperties();
		rlProp.setGroupedFields(new FieldSet(1, 4));

		rlProp.filterBySemanticProperties(sProps, 1);
	}

}
