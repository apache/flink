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


package org.apache.flink.api.java.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class SemanticPropertiesProjectionTest {

	final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData = new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();


	final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo = new
			TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO
	);

	final List<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>> emptyNestedTupleData =
			new ArrayList<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>>();

	final TupleTypeInfo<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>> nestedTupleTypeInfo = new
			TupleTypeInfo<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>>(
			BasicTypeInfo.INT_TYPE_INFO,
			new TupleTypeInfo<Tuple3<String,  Integer, Long>>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO),
			new TupleTypeInfo<Tuple2<Long, Long>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO),
			BasicTypeInfo.STRING_TYPE_INFO
			);

	@Test
	public void testProjectionSemProps1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		tupleDs.project(1, 3, 2).project(0, 3).output(new DiscardingOutputFormat<Tuple>());

		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		PlanProjectOperator<?, ?> projectOperator = ((PlanProjectOperator<?, ?>) sink.getInput());

		SingleInputSemanticProperties props = projectOperator.getSemanticProperties();

		assertEquals(1, props.getForwardingTargetFields(0, 0).size());
		assertEquals(1, props.getForwardingTargetFields(0, 1).size());
		assertEquals(1, props.getForwardingTargetFields(0, 2).size());
		assertEquals(2, props.getForwardingTargetFields(0, 3).size());

		assertTrue(props.getForwardingTargetFields(0, 1).contains(0));
		assertTrue(props.getForwardingTargetFields(0, 3).contains(1));
		assertTrue(props.getForwardingTargetFields(0, 2).contains(2));
		assertTrue(props.getForwardingTargetFields(0, 0).contains(3));
		assertTrue(props.getForwardingTargetFields(0, 3).contains(4));
	}

	@Test
	public void testProjectionSemProps2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>> tupleDs = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);

		tupleDs.project(2, 3, 1).project(2).output(new DiscardingOutputFormat<Tuple>());

		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		PlanProjectOperator<?, ?> projectOperator = ((PlanProjectOperator<?, ?>) sink.getInput());

		SingleInputSemanticProperties props = projectOperator.getSemanticProperties();

		assertNotNull(props.getForwardingTargetFields(0, 0));
		assertEquals(1, props.getForwardingTargetFields(0, 1).size());
		assertEquals(1, props.getForwardingTargetFields(0, 2).size());
		assertEquals(1, props.getForwardingTargetFields(0, 3).size());
		assertEquals(2, props.getForwardingTargetFields(0, 4).size());
		assertEquals(2, props.getForwardingTargetFields(0, 5).size());
		assertEquals(1, props.getForwardingTargetFields(0, 6).size());
		assertEquals(0, props.getForwardingTargetFields(0, 0).size());

		assertTrue(props.getForwardingTargetFields(0, 4).contains(0));
		assertTrue(props.getForwardingTargetFields(0, 5).contains(1));
		assertTrue(props.getForwardingTargetFields(0, 6).contains(2));
		assertTrue(props.getForwardingTargetFields(0, 1).contains(3));
		assertTrue(props.getForwardingTargetFields(0, 2).contains(4));
		assertTrue(props.getForwardingTargetFields(0, 3).contains(5));
		assertTrue(props.getForwardingTargetFields(0, 4).contains(6));
		assertTrue(props.getForwardingTargetFields(0, 5).contains(7));
	}

	@Test
	public void testJoinProjectionSemProps1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		tupleDs.join(tupleDs).where(0).equalTo(0)
				.projectFirst(2, 3)
				.projectSecond(1, 4)
				.output(new DiscardingOutputFormat<Tuple>());

		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> projectJoinOperator = ((JoinOperatorBase<?, ?, ?, ?>) sink.getInput());

		DualInputSemanticProperties props = projectJoinOperator.getSemanticProperties();

		assertEquals(1, props.getForwardingTargetFields(0, 2).size());
		assertEquals(1, props.getForwardingTargetFields(0, 3).size());
		assertEquals(1, props.getForwardingTargetFields(1, 1).size());
		assertEquals(1, props.getForwardingTargetFields(1, 4).size());

		assertTrue(props.getForwardingTargetFields(0, 2).contains(0));
		assertTrue(props.getForwardingTargetFields(0, 3).contains(1));
		assertTrue(props.getForwardingTargetFields(1, 1).contains(2));
		assertTrue(props.getForwardingTargetFields(1, 4).contains(3));
	}

	@Test
	public void testJoinProjectionSemProps2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>> tupleDs = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);

		tupleDs.join(tupleDs).where(0).equalTo(0)
				.projectFirst(2,0)
				.projectSecond(1,3)
				.output(new DiscardingOutputFormat<Tuple>());

		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> projectJoinOperator = ((JoinOperatorBase<?, ?, ?, ?>) sink.getInput());

		DualInputSemanticProperties props = projectJoinOperator.getSemanticProperties();

		assertEquals(1, props.getForwardingTargetFields(0, 0).size());
		assertNotNull(props.getForwardingTargetFields(0, 1));
		assertNotNull(props.getForwardingTargetFields(0, 2));
		assertNotNull(props.getForwardingTargetFields(0, 3));
		assertEquals(1, props.getForwardingTargetFields(0, 4).size());
		assertEquals(1, props.getForwardingTargetFields(0, 5).size());
		assertNotNull(props.getForwardingTargetFields(0, 6));
		assertEquals(0, props.getForwardingTargetFields(0, 1).size());
		assertEquals(0, props.getForwardingTargetFields(0, 2).size());
		assertEquals(0, props.getForwardingTargetFields(0, 3).size());
		assertEquals(0, props.getForwardingTargetFields(0, 6).size());

		assertNotNull(props.getForwardingTargetFields(1, 0));
		assertEquals(1, props.getForwardingTargetFields(1, 1).size());
		assertEquals(1, props.getForwardingTargetFields(1, 2).size());
		assertEquals(1, props.getForwardingTargetFields(1, 3).size());
		assertNotNull(props.getForwardingTargetFields(1, 4));
		assertNotNull(props.getForwardingTargetFields(1, 5));
		assertEquals(1, props.getForwardingTargetFields(1, 6).size());
		assertEquals(0, props.getForwardingTargetFields(1, 0).size());
		assertEquals(0, props.getForwardingTargetFields(1, 4).size());
		assertEquals(0, props.getForwardingTargetFields(1, 5).size());

		assertTrue(props.getForwardingTargetFields(0, 4).contains(0));
		assertTrue(props.getForwardingTargetFields(0, 5).contains(1));
		assertTrue(props.getForwardingTargetFields(0, 0).contains(2));
		assertTrue(props.getForwardingTargetFields(1, 1).contains(3));
		assertTrue(props.getForwardingTargetFields(1, 2).contains(4));
		assertTrue(props.getForwardingTargetFields(1, 3).contains(5));
		assertTrue(props.getForwardingTargetFields(1, 6).contains(6));
	}

	@Test
	public void testCrossProjectionSemProps1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		tupleDs.cross(tupleDs)
				.projectFirst(2, 3)
				.projectSecond(1, 4)
				.output(new DiscardingOutputFormat<Tuple>());

		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		CrossOperatorBase<?, ?, ?, ?> projectCrossOperator = ((CrossOperatorBase<?, ?, ?, ?>) sink.getInput());

		DualInputSemanticProperties props = projectCrossOperator.getSemanticProperties();

		assertEquals(1, props.getForwardingTargetFields(0, 2).size());
		assertEquals(1, props.getForwardingTargetFields(0, 3).size());
		assertEquals(1, props.getForwardingTargetFields(1, 1).size());
		assertEquals(1, props.getForwardingTargetFields(1, 4).size());

		assertTrue(props.getForwardingTargetFields(0, 2).contains(0));
		assertTrue(props.getForwardingTargetFields(0, 3).contains(1));
		assertTrue(props.getForwardingTargetFields(1, 1).contains(2));
		assertTrue(props.getForwardingTargetFields(1, 4).contains(3));
	}

	@Test
	public void testCrossProjectionSemProps2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>> tupleDs = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);

		tupleDs.cross(tupleDs)
				.projectFirst(2, 0)
				.projectSecond(1,3)
				.output(new DiscardingOutputFormat<Tuple>());

		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		CrossOperatorBase<?, ?, ?, ?> projectCrossOperator = ((CrossOperatorBase<?, ?, ?, ?>) sink.getInput());

		DualInputSemanticProperties props = projectCrossOperator.getSemanticProperties();

		assertEquals(1, props.getForwardingTargetFields(0, 0).size());
		assertNotNull(props.getForwardingTargetFields(0, 1));
		assertNotNull(props.getForwardingTargetFields(0, 2));
		assertNotNull(props.getForwardingTargetFields(0, 3));
		assertEquals(1, props.getForwardingTargetFields(0, 4).size());
		assertEquals(1, props.getForwardingTargetFields(0, 5).size());
		assertNotNull(props.getForwardingTargetFields(0, 6));
		assertEquals(0, props.getForwardingTargetFields(0, 1).size());
		assertEquals(0, props.getForwardingTargetFields(0, 2).size());
		assertEquals(0, props.getForwardingTargetFields(0, 3).size());
		assertEquals(0, props.getForwardingTargetFields(0, 6).size());

		assertNotNull(props.getForwardingTargetFields(1, 0));
		assertEquals(1, props.getForwardingTargetFields(1, 1).size());
		assertEquals(1, props.getForwardingTargetFields(1, 2).size());
		assertEquals(1, props.getForwardingTargetFields(1, 3).size());
		assertNotNull(props.getForwardingTargetFields(1, 4));
		assertNotNull(props.getForwardingTargetFields(1, 5));
		assertEquals(1, props.getForwardingTargetFields(1, 6).size());
		assertEquals(0, props.getForwardingTargetFields(1, 0).size());
		assertEquals(0, props.getForwardingTargetFields(1, 4).size());
		assertEquals(0, props.getForwardingTargetFields(1, 5).size());

		assertTrue(props.getForwardingTargetFields(0, 4).contains(0));
		assertTrue(props.getForwardingTargetFields(0, 5).contains(1));
		assertTrue(props.getForwardingTargetFields(0, 0).contains(2));
		assertTrue(props.getForwardingTargetFields(1, 1).contains(3));
		assertTrue(props.getForwardingTargetFields(1, 2).contains(4));
		assertTrue(props.getForwardingTargetFields(1, 3).contains(5));
		assertTrue(props.getForwardingTargetFields(1, 6).contains(6));
	}

}
