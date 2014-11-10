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
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by sebastian on 6/19/14.
 */
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


	@Test
	public void ProjectOperatorTest() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

			tupleDs.project(1, 3, 2).types(Long.class, Long.class, String.class).print();

			Plan plan = env.createProgramPlan();

			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			PlanProjectOperator<?, ?> projectOperator = ((PlanProjectOperator<?, ?>) sink.getInput());

			SingleInputSemanticProperties props = projectOperator.getSemanticProperties();

			assertTrue(props.getForwardedField(1).size() == 1);
			assertTrue(props.getForwardedField(3).size() == 1);
			assertTrue(props.getForwardedField(2).size() == 1);
			assertTrue(props.getForwardedField(1).contains(0));
			assertTrue(props.getForwardedField(3).contains(1));
			assertTrue(props.getForwardedField(2).contains(2));
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	@Test
	public void ProjectOperatorWithoutTypesClassTest() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);
			
			tupleDs.projection(1, 3).projection(2).projection(0).print();

			Plan plan = env.createProgramPlan();

			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			PlanProjectOperator<?, ?> projectOperator = ((PlanProjectOperator<?, ?>) sink.getInput());

			SingleInputSemanticProperties props = projectOperator.getSemanticProperties();

			assertTrue(props.getForwardedField(1).size() == 1);
			assertTrue(props.getForwardedField(3).size() == 1);
			assertTrue(props.getForwardedField(2).size() == 1);
			assertTrue(props.getForwardedField(0).size() == 1);
			assertTrue(props.getForwardedField(1).contains(0));
			assertTrue(props.getForwardedField(3).contains(1));
			assertTrue(props.getForwardedField(2).contains(2));
			assertTrue(props.getForwardedField(0).contains(3));
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void JoinProjectionTest() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

			tupleDs.join(tupleDs).where(0).equalTo(0).projectFirst(2, 3).projectSecond(1, 4).types(String.class, Long.class, Long.class, Integer.class).print();

			Plan plan = env.createProgramPlan();

			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			JoinOperatorBase<?, ?, ?, ?> projectJoinOperator = ((JoinOperatorBase<?, ?, ?, ?>) sink.getInput());

			DualInputSemanticProperties props = projectJoinOperator.getSemanticProperties();

			assertTrue(props.getForwardedField1(2).size() == 1);
			assertTrue(props.getForwardedField1(3).size() == 1);
			assertTrue(props.getForwardedField2(1).size() == 1);
			assertTrue(props.getForwardedField2(4).size() == 1);
			assertTrue(props.getForwardedField1(2).contains(0));
			assertTrue(props.getForwardedField1(3).contains(1));
			assertTrue(props.getForwardedField2(1).contains(2));
			assertTrue(props.getForwardedField2(4).contains(3));
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	@Test
	public void JoinProjectionWithoutTypesClassTest() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

//			tupleDs.join(tupleDs).where(0).equalTo(0).projectFirst(2, 3).projectSecond(1, 4).types().print();
			
			tupleDs.join(tupleDs).where(0).equalTo(0).projectionFirst(2).projectionFirst(3).projectionSecond(1, 4).print();

			Plan plan = env.createProgramPlan();

			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			JoinOperatorBase<?, ?, ?, ?> projectJoinOperator = ((JoinOperatorBase<?, ?, ?, ?>) sink.getInput());

			DualInputSemanticProperties props = projectJoinOperator.getSemanticProperties();

			assertTrue(props.getForwardedField1(2).size() == 1);
			assertTrue(props.getForwardedField1(3).size() == 1);
			assertTrue(props.getForwardedField2(1).size() == 1);
			assertTrue(props.getForwardedField2(4).size() == 1);
			assertTrue(props.getForwardedField1(2).contains(0));
			assertTrue(props.getForwardedField1(3).contains(1));
			assertTrue(props.getForwardedField2(1).contains(2));
			assertTrue(props.getForwardedField2(4).contains(3));
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void CrossProjectionTest() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

			DataSet<Tuple4<String, Long, Long, Integer>> result = tupleDs.cross(tupleDs).projectFirst(2, 3).projectSecond(1, 4).types(String.class, Long.class, Long.class, Integer.class);
			result.print();

			Plan plan = env.createProgramPlan();

			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			CrossOperatorBase<?, ?, ?, ?> projectCrossOperator = ((CrossOperatorBase<?, ?, ?, ?>) sink.getInput());

			DualInputSemanticProperties props = projectCrossOperator.getSemanticProperties();

			assertTrue(props.getForwardedField1(2).size() == 1);
			assertTrue(props.getForwardedField1(3).size() == 1);
			assertTrue(props.getForwardedField2(1).size() == 1);
			assertTrue(props.getForwardedField2(4).size() == 1);
			assertTrue(props.getForwardedField1(2).contains(0));
			assertTrue(props.getForwardedField1(3).contains(1));
			assertTrue(props.getForwardedField2(1).contains(2));
			assertTrue(props.getForwardedField2(4).contains(3));
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	@Test
	public void CrossProjectionWithoutTypesClassTest() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

			DataSet<Tuple4<String, Long, Long, Integer>> result = tupleDs.cross(tupleDs).projectionFirst(2, 3).projectionSecond(1, 4);
			result.print();

			Plan plan = env.createProgramPlan();

			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			CrossOperatorBase<?, ?, ?, ?> projectCrossOperator = ((CrossOperatorBase<?, ?, ?, ?>) sink.getInput());

			DualInputSemanticProperties props = projectCrossOperator.getSemanticProperties();

			assertTrue(props.getForwardedField1(2).size() == 1);
			assertTrue(props.getForwardedField1(3).size() == 1);
			assertTrue(props.getForwardedField2(1).size() == 1);
			assertTrue(props.getForwardedField2(4).size() == 1);
			assertTrue(props.getForwardedField1(2).contains(0));
			assertTrue(props.getForwardedField1(3).contains(1));
			assertTrue(props.getForwardedField2(1).contains(2));
			assertTrue(props.getForwardedField2(4).contains(3));
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
}
