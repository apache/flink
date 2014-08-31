/**
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

package org.apache.flink.compiler;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.compiler.dataproperties.GlobalProperties;
import org.apache.flink.compiler.dataproperties.LocalProperties;
import org.apache.flink.compiler.dataproperties.PartitioningProperty;
import org.apache.flink.compiler.dataproperties.RequestedGlobalProperties;
import org.apache.flink.compiler.dataproperties.RequestedLocalProperties;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.plan.DualInputPlanNode;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.PlanNode;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class SemanticPropOptimizerTest extends CompilerTestBase {

	private TupleTypeInfo<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tupleInfo =
			new TupleTypeInfo<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
			);

	public static class SimpleReducer implements ReduceFunction<Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> value1, Tuple3<Integer, Integer, Integer> value2) throws Exception {
			return null;
		}
	}

	public static class SimpleMap implements MapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> map(Tuple3<Integer, Integer, Integer> value) throws Exception {
			return null;
		}
	}

	@Test
	public void forwardFieldsTestMapReduce() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		set = set.map(new SimpleMap()).withConstantSet("*")
				.groupBy(0)
				.reduce(new SimpleReducer()).withConstantSet("0->1")
				.map(new SimpleMap()).withConstantSet("*")
				.groupBy(1)
				.reduce(new SimpleReducer()).withConstantSet("*");

		set.print();
		JavaPlan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		oPlan.accept(new Visitor<PlanNode>() {
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof ReduceOperatorBase) {
					for (Channel input: visitable.getInputs()) {
						GlobalProperties gprops = visitable.getGlobalProperties();
						LocalProperties lprops = visitable.getLocalProperties();

						Assert.assertTrue("Reduce should just forward the input if it is already partitioned",
								input.getShipStrategy() == ShipStrategyType.FORWARD);
						Assert.assertTrue("Wrong GlobalProperties on Reducer",
								gprops.isPartitionedOnFields(new FieldSet(1)));
						Assert.assertTrue("Wrong GlobalProperties on Reducer",
								gprops.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
						Assert.assertTrue("Wrong LocalProperties on Reducer",
								lprops.getGroupedFields().contains(1));
					}
				}
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof MapOperatorBase) {
					for (Channel input: visitable.getInputs()) {
						GlobalProperties gprops = visitable.getGlobalProperties();
						LocalProperties lprops = visitable.getLocalProperties();

						Assert.assertTrue("Map should just forward the input if it is already partitioned",
								input.getShipStrategy() == ShipStrategyType.FORWARD);
						Assert.assertTrue("Wrong GlobalProperties on Mapper",
								gprops.isPartitionedOnFields(new FieldSet(1)));
						Assert.assertTrue("Wrong GlobalProperties on Mapper",
								gprops.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
						Assert.assertTrue("Wrong LocalProperties on Mapper",
								lprops.getGroupedFields().contains(1));
					}
					return false;
				}
				return true;
			}

			@Override
			public void postVisit(PlanNode visitable) {

			}
		});
	}

	@Test
	public void localPropertiesFilterNothingPreservedTest() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		LocalProperties lprops = LocalProperties.forGrouping(grouping);

		LocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result.getOrdering() == null);
		Assert.assertTrue(result.getGroupedFields() == null);
		Assert.assertTrue(result.getUniqueFields() == null);
	}

	@Test
	public void localPropertiesFilterWrongInputTest() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		LocalProperties lprops = LocalProperties.forGrouping(grouping);

		try {
			LocalProperties result = lprops.filterBySemanticProperties(sprops, 1);
		} catch (Exception e) {
			return;
		}
		Assert.fail();
	}

	@Test
	public void localPropertiesFilterGroupingPreservedTest() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->2", "3->1", "5->5", "7->0,4"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		LocalProperties lprops = LocalProperties.forGrouping(grouping);

		LocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result.getOrdering() == null);

		Assert.assertTrue(result.getGroupedFields().size() == 5);
		Assert.assertTrue(result.getGroupedFields().contains(2));
		Assert.assertTrue(result.getGroupedFields().contains(1));
		Assert.assertTrue(result.getGroupedFields().contains(5));
		Assert.assertTrue(result.getGroupedFields().contains(0));
		Assert.assertTrue(result.getGroupedFields().contains(4));

		Assert.assertTrue(result.getUniqueFields() == null);
	}

	@Test
	public void localPropertiesFilterGroupingPreservedTest2() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->0", "3->3", "5->5", "7->7"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		LocalProperties lprops = LocalProperties.forGrouping(grouping);

		LocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result.getOrdering() == null);

		Assert.assertTrue(result.getGroupedFields().size() == 4);
		Assert.assertTrue(result.getGroupedFields().contains(0));
		Assert.assertTrue(result.getGroupedFields().contains(3));
		Assert.assertTrue(result.getGroupedFields().contains(5));
		Assert.assertTrue(result.getGroupedFields().contains(7));

		Assert.assertTrue(result.getUniqueFields() == null);
	}

	@Test
	public void localPropertiesFilterGroupingNothingPreservedTest() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->2", "3->1", "7->0,4"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		LocalProperties lprops = LocalProperties.forGrouping(grouping);

		LocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result.getGroupedFields() == null);
	}

	@Test
	public void localPropertiesFilterOrderingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(0, null, Order.ASCENDING);
		ordering.appendOrdering(1, null, Order.ASCENDING);
		ordering.appendOrdering(2, null, Order.ASCENDING);

		String[] constantSet = {"0->2", "1->3", "2->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		LocalProperties lprops = LocalProperties.forOrdering(ordering);

		LocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		FieldList involved = result.getOrdering().getInvolvedIndexes();
		Order[] orders = result.getOrdering().getFieldOrders();


		Assert.assertTrue(involved.size() == 3);
		Assert.assertTrue(involved.contains(2));
		Assert.assertTrue(involved.contains(3));
		Assert.assertTrue(involved.contains(0));
		Assert.assertTrue(orders[0] == Order.ASCENDING);
		Assert.assertTrue(orders[1] == Order.ASCENDING);
		Assert.assertTrue(orders[2] == Order.ASCENDING);
	}

	@Test
	public void localPropertiesFilterOrderingNothingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(0, null, Order.ASCENDING);
		ordering.appendOrdering(1, null, Order.ASCENDING);
		ordering.appendOrdering(2, null, Order.ASCENDING);

		String[] constantSet = {"0->2", "2->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		LocalProperties lprops = LocalProperties.forOrdering(ordering);

		LocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result.getOrdering() == null);
	}



	@Test
	public void localPropertiesFilterUniqueFieldsPreservedTest() {
		String[] constantSet = {"0->1", "1->2", "3->4", "2->3", "6->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);


		FieldSet set1 = new FieldSet(0, 1, 2, 3);
		FieldSet set2 = new FieldSet(4, 5, 6, 7);
		FieldSet set3 = new FieldSet(0, 1, 6, 2);
		LocalProperties lprops = new LocalProperties();
		lprops = lprops.addUniqueFields(set1).addUniqueFields(set2).addUniqueFields(set3);


		LocalProperties result = lprops.filterBySemanticProperties(sprops, 0);
		Set<FieldSet> unique = result.getUniqueFields();
		FieldSet expected1 = new FieldSet(1, 2, 3, 4);
		FieldSet expected2 = new FieldSet(0, 1, 2, 3);


		Assert.assertTrue(unique.size() == 2);
		Assert.assertTrue(unique.contains(expected1));
		Assert.assertTrue(unique.contains(expected2));
	}

	@Test
	public void requestedLocalPropertiesFilterNothingPreservedTest() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops = new RequestedLocalProperties();
		lprops.setGroupedFields(grouping);

		RequestedLocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result == null);
	}

	@Test
	public void requestedLocalPropertiesFilterWrongInputTest() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops = new RequestedLocalProperties();
		lprops.setGroupedFields(grouping);

		try {
			RequestedLocalProperties result = lprops.filterBySemanticProperties(sprops, 1);
		} catch (Exception e) {
			return;
		}
		Assert.fail();
	}

	@Test
	public void requestedLocalPropertiesFilterGroupingPreservedTest() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"6->7", "1->5", "4->3", "7->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops = new RequestedLocalProperties();
		lprops.setGroupedFields(grouping);

		RequestedLocalProperties result = lprops.filterBySemanticProperties(sprops, 0);


		Assert.assertTrue(result.getGroupedFields().size() == 4);
		Assert.assertTrue(result.getGroupedFields().contains(7));
		Assert.assertTrue(result.getGroupedFields().contains(4));
		Assert.assertTrue(result.getGroupedFields().contains(1));
		Assert.assertTrue(result.getGroupedFields().contains(6));
	}

	@Test
	public void requestedLocalPropertiesFilterGroupingPreservedTest2() {
		FieldList grouping = new FieldList().addFields(0, 3, 5, 7);

		String[] constantSet = {"0->0", "3->3", "5->5", "7->7"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops = new RequestedLocalProperties();
		lprops.setGroupedFields(grouping);

		RequestedLocalProperties result = lprops.filterBySemanticProperties(sprops, 0);


		Assert.assertTrue(result.getGroupedFields().size() == 4);
		Assert.assertTrue(result.getGroupedFields().contains(0));
		Assert.assertTrue(result.getGroupedFields().contains(3));
		Assert.assertTrue(result.getGroupedFields().contains(5));
		Assert.assertTrue(result.getGroupedFields().contains(7));
	}

	@Test
	public void requestedLocalPropertiesFilterOrderingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(2, null, Order.ASCENDING);
		ordering.appendOrdering(3, null, Order.ASCENDING);
		ordering.appendOrdering(0, null, Order.ASCENDING);

		String[] constantSet = {"6->2", "2->3", "5->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops = new RequestedLocalProperties();
		lprops.setOrdering(ordering);

		RequestedLocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		FieldList involved = result.getOrdering().getInvolvedIndexes();
		Order[] orders = result.getOrdering().getFieldOrders();

		Assert.assertTrue(involved.size() == 3);
		Assert.assertTrue(involved.contains(6));
		Assert.assertTrue(involved.contains(2));
		Assert.assertTrue(involved.contains(5));
		Assert.assertTrue(orders[0] == Order.ASCENDING);
		Assert.assertTrue(orders[1] == Order.ASCENDING);
		Assert.assertTrue(orders[2] == Order.ASCENDING);
	}

	@Test
	public void requestedLocalPropertiesFilterOrderingPreservedTest2() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(0, null, Order.ASCENDING);
		ordering.appendOrdering(3, null, Order.ASCENDING);
		ordering.appendOrdering(6, null, Order.ASCENDING);

		String[] constantSet = {"0->0", "3->3", "6->6"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops = new RequestedLocalProperties();
		lprops.setOrdering(ordering);

		RequestedLocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		FieldList involved = result.getOrdering().getInvolvedIndexes();
		Order[] orders = result.getOrdering().getFieldOrders();

		Assert.assertTrue(involved.size() == 3);
		Assert.assertTrue(involved.contains(0));
		Assert.assertTrue(involved.contains(3));
		Assert.assertTrue(involved.contains(6));
		Assert.assertTrue(orders[0] == Order.ASCENDING);
		Assert.assertTrue(orders[1] == Order.ASCENDING);
		Assert.assertTrue(orders[2] == Order.ASCENDING);
	}

	@Test
	public void requestedLocalPropertiesFilterOrderingNothingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(2, null, Order.ASCENDING);
		ordering.appendOrdering(3, null, Order.ASCENDING);
		ordering.appendOrdering(0, null, Order.ASCENDING);

		String[] constantSet = {"6->2", "5->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops = new RequestedLocalProperties();
		lprops.setOrdering(ordering);

		RequestedLocalProperties result = lprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result == null);
	}

	@Test
	public void requestedLocalPropertiesDualFilterOrderingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(2, null, Order.ASCENDING);
		ordering.appendOrdering(3, null, Order.ASCENDING);
		ordering.appendOrdering(0, null, Order.ASCENDING);

		String[] constantSet = {"6->2", "2->3", "5->0"};
		DualInputSemanticProperties dprops = new DualInputSemanticProperties();
		SemanticPropUtil.getSemanticPropsDualFromString(dprops, constantSet, constantSet, null, null, null, null, tupleInfo, tupleInfo, tupleInfo);

		RequestedLocalProperties lprops1 = new RequestedLocalProperties();
		lprops1.setOrdering(ordering);

		RequestedLocalProperties lprops2 = new RequestedLocalProperties();
		lprops2.setOrdering(ordering);

		RequestedLocalProperties result1 = lprops1.filterBySemanticProperties(dprops, 0);
		RequestedLocalProperties result2 = lprops2.filterBySemanticProperties(dprops, 1);

		FieldSet involved1 = result1.getOrdering().getInvolvedIndexes();
		FieldSet involved2 = result2.getOrdering().getInvolvedIndexes();
		Order[] orders1 = result1.getOrdering().getFieldOrders();
		Order[] orders2 = result2.getOrdering().getFieldOrders();


		Assert.assertTrue(involved1.size() == 3);
		Assert.assertTrue(involved1.contains(6));
		Assert.assertTrue(involved1.contains(2));
		Assert.assertTrue(involved1.contains(5));
		Assert.assertTrue(orders1[0] == Order.ASCENDING);
		Assert.assertTrue(orders1[1] == Order.ASCENDING);
		Assert.assertTrue(orders1[2] == Order.ASCENDING);

		Assert.assertTrue(involved2.size() == 3);
		Assert.assertTrue(involved2.contains(6));
		Assert.assertTrue(involved2.contains(2));
		Assert.assertTrue(involved2.contains(5));
		Assert.assertTrue(orders2[0] == Order.ASCENDING);
		Assert.assertTrue(orders2[1] == Order.ASCENDING);
		Assert.assertTrue(orders2[2] == Order.ASCENDING);
	}

	@Test
	public void globalPropertiesFilterNothingPreservedTest() {
		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result.getOrdering() == null);
		Assert.assertTrue(result.getPartitioningFields() == null);
	}

	@Test
	public void globalPropertiesFilterWrongInputTest() {
		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 3, 4, 6));

		try {
			GlobalProperties result = gprops.filterBySemanticProperties(sprops, 1);
		} catch (Exception e) {
			return;
		}
		Assert.fail();
	}

	@Test
	public void globalPropertiesFilterPartitioningPreservedTest() {
		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 4));

		String[] constantSet = {"0->2", "1->4", "4->7"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);


		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldList list = result.getPartitioningFields();
		Assert.assertTrue(list.size() == 3);
		Assert.assertTrue(list.contains(2));
		Assert.assertTrue(list.contains(4));
		Assert.assertTrue(list.contains(7));
		Assert.assertTrue(result.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
	}

	@Test
	public void globalPropertiesFilterPartitioningPreservedTest2() {
		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 4));

		String[] constantSet = {"0->2, 6", "1->4, 5", "4->7"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);


		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldList list = result.getPartitioningFields();
		Assert.assertTrue(list.size() == 5);
		Assert.assertTrue(list.contains(2));
		Assert.assertTrue(list.contains(4));
		Assert.assertTrue(list.contains(6));
		Assert.assertTrue(list.contains(5));
		Assert.assertTrue(list.contains(7));
		Assert.assertTrue(result.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
	}

	@Test
	public void globalPropertiesFilterPartitioningPreservedTest3() {
		GlobalProperties gprops = new GlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 4));

		String[] constantSet = {"0->0", "1->1", "4->4"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);


		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldList list = result.getPartitioningFields();
		Assert.assertTrue(list.size() == 3);
		Assert.assertTrue(list.contains(0));
		Assert.assertTrue(list.contains(1));
		Assert.assertTrue(list.contains(4));
		Assert.assertTrue(result.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
	}

	@Test
	public void globalPropertiesFilterOrderingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(0, null, Order.ASCENDING);
		ordering.appendOrdering(1, null, Order.ASCENDING);
		ordering.appendOrdering(2, null, Order.ASCENDING);

		String[] constantSet = {"0->2", "1->3", "2->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setOrdering(ordering);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldList involved = result.getOrdering().getInvolvedIndexes();
		Order[] orders = result.getOrdering().getFieldOrders();


		Assert.assertTrue(involved.size() == 3);
		Assert.assertTrue(involved.contains(2));
		Assert.assertTrue(involved.contains(3));
		Assert.assertTrue(involved.contains(0));
		Assert.assertTrue(orders[0] == Order.ASCENDING);
		Assert.assertTrue(orders[1] == Order.ASCENDING);
		Assert.assertTrue(orders[2] == Order.ASCENDING);
	}

	@Test
	public void globalPropertiesFilterOrderingPreservedTest2() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(0, null, Order.ASCENDING);
		ordering.appendOrdering(1, null, Order.ASCENDING);
		ordering.appendOrdering(2, null, Order.ASCENDING);

		String[] constantSet = {"0->2,4", "1->3", "2->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setOrdering(ordering);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldList involved = result.getOrdering().getInvolvedIndexes();
		Order[] orders = result.getOrdering().getFieldOrders();


		Assert.assertTrue(involved.size() == 4);
		Assert.assertTrue(involved.contains(2));
		Assert.assertTrue(involved.contains(3));
		Assert.assertTrue(involved.contains(4));
		Assert.assertTrue(involved.contains(0));
		Assert.assertTrue(orders[0] == Order.ASCENDING);
		Assert.assertTrue(orders[1] == Order.ASCENDING);
		Assert.assertTrue(orders[2] == Order.ASCENDING);
		Assert.assertTrue(orders[3] == Order.ASCENDING);
	}

	@Test
	public void globalPropertiesFilterOrderingPreservedTest3() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(0, null, Order.ASCENDING);
		ordering.appendOrdering(1, null, Order.ASCENDING);
		ordering.appendOrdering(2, null, Order.ASCENDING);

		String[] constantSet = {"0->0", "1->1", "2->2"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setOrdering(ordering);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldList involved = result.getOrdering().getInvolvedIndexes();
		Order[] orders = result.getOrdering().getFieldOrders();


		Assert.assertTrue(involved.size() == 3);
		Assert.assertTrue(involved.contains(0));
		Assert.assertTrue(involved.contains(1));
		Assert.assertTrue(involved.contains(2));
		Assert.assertTrue(orders[0] == Order.ASCENDING);
		Assert.assertTrue(orders[1] == Order.ASCENDING);
		Assert.assertTrue(orders[2] == Order.ASCENDING);
	}

	@Test
	public void globalPropertiesFilterOrderingNothingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(0, null, Order.ASCENDING);
		ordering.appendOrdering(1, null, Order.ASCENDING);
		ordering.appendOrdering(2, null, Order.ASCENDING);

		String[] constantSet = {"0->2", "2->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		GlobalProperties gprops = new GlobalProperties();
		gprops.setOrdering(ordering);

		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result.getOrdering() == null);
	}

	@Test
	public void globalPropertiesFilterUniqueFieldsPreservedTest() {
		String[] constantSet = {"0->1", "1->2", "3->4", "2->3", "6->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);


		FieldSet set1 = new FieldSet(0, 1, 2, 3);
		FieldSet set2 = new FieldSet(4, 5, 6, 7);
		FieldSet set3 = new FieldSet(0, 1, 6, 2);
		GlobalProperties gprops = new GlobalProperties();
		gprops.addUniqueFieldCombination(set1);
		gprops.addUniqueFieldCombination(set2);
		gprops.addUniqueFieldCombination(set3);


		GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);
		Set<FieldSet> unique = result.getUniqueFieldCombination();
		FieldSet expected1 = new FieldSet(1, 2, 3, 4);
		FieldSet expected2 = new FieldSet(0, 1, 2, 3);


		Assert.assertTrue(unique.size() == 2);
		Assert.assertTrue(unique.contains(expected1));
		Assert.assertTrue(unique.contains(expected2));
	}

	@Test
	public void requestedGlobalPropertiesFilterNothingPreservedTest() {
		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedGlobalProperties gprops = new RequestedGlobalProperties();
		RequestedGlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result == null);
	}

	@Test
	public void requestedGlobalPropertiesDualFilterEverythingPreservedTest() {
		String[] constantSet1 = {"0->1,2", "3->0", "2->4"};
		String[] constantSet2 = {"0->3", "4->6", "6->7"};


        DualInputSemanticProperties dprops = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(dprops, constantSet1, constantSet2, null, null, null, null, tupleInfo, tupleInfo, tupleInfo);

		RequestedGlobalProperties gprops1 = new RequestedGlobalProperties();
		RequestedGlobalProperties gprops2 = new RequestedGlobalProperties();

        gprops1.setHashPartitioned(new FieldSet(2, 0, 4));
        gprops2.setHashPartitioned(new FieldSet(3, 6, 7));

        gprops1 = gprops1.filterBySemanticProperties(dprops, 0);
        gprops2 = gprops2.filterBySemanticProperties(dprops, 1);

        Assert.assertTrue(gprops1.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
        Assert.assertTrue(gprops1.getPartitionedFields().size() == 3);
        Assert.assertTrue(gprops1.getPartitionedFields().contains(0));
        Assert.assertTrue(gprops1.getPartitionedFields().contains(3));
        Assert.assertTrue(gprops1.getPartitionedFields().contains(2));

        Assert.assertTrue(gprops2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
        Assert.assertTrue(gprops2.getPartitionedFields().size() == 3);
        Assert.assertTrue(gprops2.getPartitionedFields().contains(0));
        Assert.assertTrue(gprops2.getPartitionedFields().contains(4));
        Assert.assertTrue(gprops2.getPartitionedFields().contains(6));
	}

	@Test
	public void requestedGlobalPropertiesDualFilterNothingPreservedTest() {
		String[] constantSet1 = {"0->1,2", "3->0", "2->4"};
		String[] constantSet2 = {"0->3", "4->6", "6->7"};


        DualInputSemanticProperties dprops = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(dprops, constantSet1, constantSet2, null, null, null, null, tupleInfo, tupleInfo, tupleInfo);

		RequestedGlobalProperties gprops1 = new RequestedGlobalProperties();
		RequestedGlobalProperties gprops2 = new RequestedGlobalProperties();

        gprops1.setHashPartitioned(new FieldSet(6, 7));
        gprops2.setHashPartitioned(new FieldSet(3, 6, 7));

        gprops1 = gprops1.filterBySemanticProperties(dprops, 0);
        gprops2 = gprops2.filterBySemanticProperties(dprops, 1);

        Assert.assertTrue(gprops1 == null);

        Assert.assertTrue(gprops2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
        Assert.assertTrue(gprops2.getPartitionedFields().size() == 3);
        Assert.assertTrue(gprops2.getPartitionedFields().contains(0));
        Assert.assertTrue(gprops2.getPartitionedFields().contains(4));
        Assert.assertTrue(gprops2.getPartitionedFields().contains(6));
	}

	@Test
	public void requestedGlobalPropertiesFilterWrongInputTest() {
		String[] constantSet = {"0->2", "3->1", "5->5"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedGlobalProperties gprops = new RequestedGlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 3, 4, 6));

		try {
			RequestedGlobalProperties result = gprops.filterBySemanticProperties(sprops, 1);
		} catch (Exception e) {
			return;
		}
		Assert.fail();
	}

	@Test
	public void requestedGlobalPropertiesFilterPartitioningPreservedTest() {
		RequestedGlobalProperties gprops = new RequestedGlobalProperties();
		gprops.setHashPartitioned(new FieldList(0, 1, 4));

		String[] constantSet = {"3->0", "5->1", "2->4"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);


		RequestedGlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldSet set = result.getPartitionedFields();
		Assert.assertTrue(set.size() == 3);
		Assert.assertTrue(set.contains(3));
		Assert.assertTrue(set.contains(5));
		Assert.assertTrue(set.contains(2));
		Assert.assertTrue(result.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
	}

	@Test
	public void requestedGlobalPropertiesFilterPartitioningPreservedTest2() {
		RequestedGlobalProperties gprops = new RequestedGlobalProperties();
		gprops.setHashPartitioned(new FieldList(3, 5, 2));

		String[] constantSet = {"3->3", "5->5", "2->2"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);


		RequestedGlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldSet set = result.getPartitionedFields();
		Assert.assertTrue(set.size() == 3);
		Assert.assertTrue(set.contains(3));
		Assert.assertTrue(set.contains(5));
		Assert.assertTrue(set.contains(2));
		Assert.assertTrue(result.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
	}

	@Test
	public void semanticPropertySourceFieldTest() {
		SingleInputSemanticProperties props = new SingleInputSemanticProperties();
		props.addForwardedField(2, 4);
		props.addForwardedField(3, 8);
		props.addForwardedField(4, 10);
		props.addForwardedField(4, 11);

		Assert.assertTrue(props.getSourceField(0, 4).size() == 1);
		Assert.assertTrue(props.getSourceField(0, 4).contains(2));
		Assert.assertTrue(props.getSourceField(0, 8).size() == 1);
		Assert.assertTrue(props.getSourceField(0, 8).contains(3));
		Assert.assertTrue(props.getSourceField(0, 10).size() == 1);
		Assert.assertTrue(props.getSourceField(0, 10).contains(4));
		Assert.assertTrue(props.getSourceField(0, 11).size() == 1);
		Assert.assertTrue(props.getSourceField(0, 11).contains(4));
	}

	@Test
	public void requestedGlobalPropertiesFilterOrderingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(2, null, Order.ASCENDING);
		ordering.appendOrdering(3, null, Order.ASCENDING);
		ordering.appendOrdering(0, null, Order.ASCENDING);

		String[] constantSet = {"6->2", "2->3", "5->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedGlobalProperties gprops = new RequestedGlobalProperties();
		gprops.setOrdering(ordering);

		RequestedGlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		FieldList involved = result.getOrdering().getInvolvedIndexes();
		Order[] orders = result.getOrdering().getFieldOrders();


		Assert.assertTrue(involved.size() == 3);
		Assert.assertTrue(involved.contains(6));
		Assert.assertTrue(involved.contains(2));
		Assert.assertTrue(involved.contains(5));
		Assert.assertTrue(orders[0] == Order.ASCENDING);
		Assert.assertTrue(orders[1] == Order.ASCENDING);
		Assert.assertTrue(orders[2] == Order.ASCENDING);
	}

	@Test
	public void requestedGlobalPropertiesFilterOrderingNothingPreservedTest() {
		Ordering ordering = new Ordering();
		ordering.appendOrdering(2, null, Order.ASCENDING);
		ordering.appendOrdering(3, null, Order.ASCENDING);
		ordering.appendOrdering(0, null, Order.ASCENDING);

		String[] constantSet = {"6->2", "5->0"};
		SingleInputSemanticProperties sprops = SemanticPropUtil.getSemanticPropsSingleFromString(constantSet, null, null, tupleInfo, tupleInfo);

		RequestedGlobalProperties gprops = new RequestedGlobalProperties();
		gprops.setOrdering(ordering);

		RequestedGlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

		Assert.assertTrue(result == null);
	}

	@Test
	public void forwardFieldsTestJoin() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> in1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> in2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		in1 = in1.map(new SimpleMap()).withConstantSet("*")
				.groupBy(0)
				.reduce(new SimpleReducer()).withConstantSet("0->1");
		in2 = in2.map(new SimpleMap()).withConstantSet("*")
				.groupBy(1)
				.reduce(new SimpleReducer()).withConstantSet("1->2");
		DataSet<Tuple3<Integer, Integer, Integer>> out = in1.join(in2).where(1).equalTo(2).with(new JoinFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple3<Integer, Integer, Integer> join(Tuple3<Integer, Integer, Integer> first, Tuple3<Integer, Integer, Integer> second) throws Exception {
				return null;
			}
		});

		out.print();
		JavaPlan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		oPlan.accept(new Visitor<PlanNode>() {
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof DualInputPlanNode && visitable.getPactContract() instanceof JoinOperatorBase) {
					DualInputPlanNode node = ((DualInputPlanNode) visitable);

					final Channel inConn1 = node.getInput1();
					final Channel inConn2 = node.getInput2();

					Assert.assertTrue("Join should just forward the input if it is already partitioned",
							inConn1.getShipStrategy() == ShipStrategyType.FORWARD);
					Assert.assertTrue("Join should just forward the input if it is already partitioned",
							inConn2.getShipStrategy() == ShipStrategyType.FORWARD);
					return false;
				}
				return true;
			}

			@Override
			public void postVisit(PlanNode visitable) {

			}
		});
	}
}

