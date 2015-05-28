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

import static org.junit.Assert.*;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


/**
 * This is a minimal test to verify that semantic annotations are evaluated against
 * the type information properly translated correctly to the common data flow API.
 *
 */
@SuppressWarnings("serial")
public class SemanticPropertiesTranslationTest {
	
	@Test
	public void testUnaryFunctionWildcardForwardedAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, String, Integer>> input = env.fromElements(new Tuple3<Long, String, Integer>(3l, "test", 42));
		input.map(new WildcardForwardedMapper<Tuple3<Long,String,Integer>>()).output(new DiscardingOutputFormat<Tuple3<Long, String, Integer>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
		FieldSet fw2 = semantics.getForwardingTargetFields(0, 1);
		FieldSet fw3 = semantics.getForwardingTargetFields(0, 2);
		assertNotNull(fw1);
		assertNotNull(fw2);
		assertNotNull(fw3);
		assertTrue(fw1.contains(0));
		assertTrue(fw2.contains(1));
		assertTrue(fw3.contains(2));
	}
	
	@Test
	public void testUnaryFunctionInPlaceForwardedAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, String, Integer>> input = env.fromElements(new Tuple3<Long, String, Integer>(3l, "test", 42));
		input.map(new IndividualForwardedMapper<Long, String, Integer>()).output(new DiscardingOutputFormat<Tuple3<Long, String, Integer>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
		FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
		assertNotNull(fw1);
		assertNotNull(fw2);
		assertTrue(fw1.contains(0));
		assertTrue(fw2.contains(2));
	}

	@Test
	public void testUnaryFunctionMovingForwardedAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new ShufflingMapper<Long>()).output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
		FieldSet fw2 = semantics.getForwardingTargetFields(0, 1);
		FieldSet fw3 = semantics.getForwardingTargetFields(0, 2);
		assertNotNull(fw1);
		assertNotNull(fw2);
		assertNotNull(fw3);
		assertTrue(fw1.contains(2));
		assertTrue(fw2.contains(0));
		assertTrue(fw3.contains(1));
	}

	@Test
	public void testUnaryFunctionForwardedInLine1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new NoAnnotationMapper<Tuple3<Long, Long, Long>>()).withForwardedFields("0->1; 2")
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
		FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
		assertNotNull(fw1);
		assertNotNull(fw2);
		assertTrue(fw1.contains(1));
		assertTrue(fw2.contains(2));
	}

	@Test
	public void testUnaryFunctionForwardedInLine2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new ReadSetMapper<Tuple3<Long, Long, Long>>()).withForwardedFields("0->1; 2")
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
		FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
		assertNotNull(fw1);
		assertNotNull(fw2);
		assertTrue(fw1.contains(1));
		assertTrue(fw2.contains(2));
	}

	@Test
	public void testUnaryFunctionForwardedInLine3() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new ReadSetMapper<Tuple3<Long, Long, Long>>()).withForwardedFields("0->1; 2")
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
		FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
		assertNotNull(fw1);
		assertNotNull(fw2);
		assertTrue(fw1.contains(1));
		assertTrue(fw2.contains(2));
	}

	@Test
	public void testUnaryFunctionAllForwardedExceptAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new AllForwardedExceptMapper<Tuple3<Long, Long, Long>>()).output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
		FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
		assertNotNull(fw1);
		assertNotNull(fw2);
		assertTrue(fw1.contains(0));
		assertTrue(fw2.contains(2));
	}

	@Test
	public void testUnaryFunctionReadFieldsAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new ReadSetMapper<Tuple3<Long, Long, Long>>()).output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

		SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

		FieldSet read = semantics.getReadFields(0);
		assertNotNull(read);
		assertEquals(2, read.size());
		assertTrue(read.contains(0));
		assertTrue(read.contains(2));
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testUnaryForwardedOverwritingInLine1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new WildcardForwardedMapper<Tuple3<Long, Long, Long>>()).withForwardedFields("0->1; 2");
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testUnaryForwardedOverwritingInLine2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input.map(new AllForwardedExceptMapper<Tuple3<Long, Long, Long>>()).withForwardedFields("0->1; 2");
	}

	@Test
	public void testBinaryForwardedAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, String>> input1 = env.fromElements(new Tuple2<Long, String>(3l, "test"));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Double>> input2 = env.fromElements(new Tuple2<Long, Double>(3l, 3.1415));
		input1.join(input2).where(0).equalTo(0).with(new ForwardedBothAnnotationJoin<Long, String, Long, Double>())
				.output(new DiscardingOutputFormat<Tuple2<String, Double>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();

		DualInputSemanticProperties semantics = join.getSemanticProperties();
		assertNotNull(semantics.getForwardingTargetFields(0, 0));
		assertNotNull(semantics.getForwardingTargetFields(1, 0));
		assertEquals(1, semantics.getForwardingTargetFields(0, 1).size());
		assertEquals(1, semantics.getForwardingTargetFields(1, 1).size());
		assertTrue(semantics.getForwardingTargetFields(0, 1).contains(0));
		assertTrue(semantics.getForwardingTargetFields(1, 1).contains(1));
		assertEquals(0, semantics.getForwardingTargetFields(0, 0).size());
		assertEquals(0, semantics.getForwardingTargetFields(1, 0).size());
	}

	@Test
	public void testBinaryForwardedInLine1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new NoAnnotationJoin<Long>())
				.withForwardedFieldsFirst("0->1; 1->2").withForwardedFieldsSecond("1->0")
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();

		DualInputSemanticProperties semantics = join.getSemanticProperties();
		assertNotNull(semantics.getForwardingTargetFields(1, 0));
		assertEquals(1, semantics.getForwardingTargetFields(0, 0).size());
		assertEquals(1, semantics.getForwardingTargetFields(0, 1).size());
		assertEquals(1, semantics.getForwardingTargetFields(1, 1).size());
		assertTrue(semantics.getForwardingTargetFields(0, 0).contains(1));
		assertTrue(semantics.getForwardingTargetFields(0, 1).contains(2));
		assertTrue(semantics.getForwardingTargetFields(1, 1).contains(0));
		assertEquals(0, semantics.getForwardingTargetFields(1, 0).size());
	}

	@Test
	public void testBinaryForwardedInLine2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ReadSetJoin<Long>())
				.withForwardedFieldsFirst("0->1; 1->2").withForwardedFieldsSecond("1->0")
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();

		DualInputSemanticProperties semantics = join.getSemanticProperties();
		assertNotNull(semantics.getForwardingTargetFields(1, 0));
		assertEquals(1, semantics.getForwardingTargetFields(0, 0).size());
		assertEquals(1, semantics.getForwardingTargetFields(0, 1).size());
		assertEquals(1, semantics.getForwardingTargetFields(1, 1).size());
		assertTrue(semantics.getForwardingTargetFields(0, 0).contains(1));
		assertTrue(semantics.getForwardingTargetFields(0, 1).contains(2));
		assertTrue(semantics.getForwardingTargetFields(1, 1).contains(0));
		assertNotNull(semantics.getReadFields(0));
		assertNotNull(semantics.getReadFields(1));
		assertEquals(1, semantics.getReadFields(0).size());
		assertEquals(1, semantics.getReadFields(1).size());
		assertTrue(semantics.getReadFields(0).contains(1));
		assertTrue(semantics.getReadFields(1).contains(0));
		assertEquals(0, semantics.getForwardingTargetFields(1, 0).size());
	}

	@Test
	public void testBinaryForwardedAnnotationInLineMixed1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ForwardedFirstAnnotationJoin<Long>())
				.withForwardedFieldsSecond("1")
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();

		DualInputSemanticProperties semantics = join.getSemanticProperties();
		assertNotNull(semantics.getForwardingTargetFields(0, 1));
		assertNotNull(semantics.getForwardingTargetFields(1, 0));
		assertNotNull(semantics.getForwardingTargetFields(0, 0));
		assertNotNull(semantics.getForwardingTargetFields(1, 1));
		assertEquals(1, semantics.getForwardingTargetFields(0, 0).size());
		assertEquals(1, semantics.getForwardingTargetFields(1, 1).size());
		assertTrue(semantics.getForwardingTargetFields(0, 0).contains(2));
		assertTrue(semantics.getForwardingTargetFields(1, 1).contains(1));
		assertEquals(0, semantics.getForwardingTargetFields(0, 1).size());
		assertEquals(0, semantics.getForwardingTargetFields(1, 0).size());

	}

	@Test
	public void testBinaryForwardedAnnotationInLineMixed2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ForwardedSecondAnnotationJoin<Long>())
				.withForwardedFieldsFirst("0->1")
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();

		DualInputSemanticProperties semantics = join.getSemanticProperties();
		assertNotNull(semantics.getForwardingTargetFields(0, 1));
		assertNotNull(semantics.getForwardingTargetFields(1, 0));
		assertNotNull(semantics.getForwardingTargetFields(0, 0));
		assertNotNull(semantics.getForwardingTargetFields(1, 1));
		assertEquals(1, semantics.getForwardingTargetFields(0, 0).size());
		assertEquals(1, semantics.getForwardingTargetFields(1, 1).size());
		assertTrue(semantics.getForwardingTargetFields(0, 0).contains(1));
		assertTrue(semantics.getForwardingTargetFields(1, 1).contains(2));
		assertEquals(0, semantics.getForwardingTargetFields(0, 1).size());
		assertEquals(0, semantics.getForwardingTargetFields(1, 0).size());
	}

	@Test
	public void testBinaryAllForwardedExceptAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input1 = env.fromElements(new Tuple3<Long, Long, Long>(3l, 4l, 5l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input2 = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input1.join(input2).where(0).equalTo(0).with(new AllForwardedExceptJoin<Long>())
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();

		DualInputSemanticProperties semantics = join.getSemanticProperties();
		assertNotNull(semantics.getForwardingTargetFields(0, 0));
		assertNotNull(semantics.getForwardingTargetFields(0, 2));
		assertNotNull(semantics.getForwardingTargetFields(1, 0));
		assertNotNull(semantics.getForwardingTargetFields(1, 1));
		assertEquals(1, semantics.getForwardingTargetFields(0, 1).size());
		assertEquals(1, semantics.getForwardingTargetFields(1, 2).size());
		assertTrue(semantics.getForwardingTargetFields(0, 1).contains(1));
		assertTrue(semantics.getForwardingTargetFields(1, 2).contains(2));
		assertEquals(0, semantics.getForwardingTargetFields(0, 0).size());
		assertEquals(0, semantics.getForwardingTargetFields(0, 2).size());
		assertEquals(0, semantics.getForwardingTargetFields(1, 0).size());
		assertEquals(0, semantics.getForwardingTargetFields(1, 1).size());
	}

	@Test
	public void testBinaryReadFieldsAnnotation() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ReadSetJoin<Long>())
				.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
		Plan plan = env.createProgramPlan();

		GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
		JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();

		DualInputSemanticProperties semantics = join.getSemanticProperties();
		assertNotNull(semantics.getReadFields(0));
		assertNotNull(semantics.getReadFields(1));
		assertEquals(1, semantics.getReadFields(0).size());
		assertEquals(1, semantics.getReadFields(1).size());
		assertTrue(semantics.getReadFields(0).contains(1));
		assertTrue(semantics.getReadFields(1).contains(0));
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testBinaryForwardedOverwritingInLine1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ForwardedFirstAnnotationJoin<Long>())
				.withForwardedFieldsFirst("0->1");
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testBinaryForwardedOverwritingInLine2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ForwardedSecondAnnotationJoin<Long>())
				.withForwardedFieldsSecond("0->1");
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testBinaryForwardedOverwritingInLine3() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ForwardedBothAnnotationJoin<Long, Long, Long, Long>())
				.withForwardedFieldsFirst("0->1;");
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testBinaryForwardedOverwritingInLine4() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(3l, 4l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<Long, Long>(3l, 2l));
		input1.join(input2).where(0).equalTo(0).with(new ForwardedBothAnnotationJoin<Long, Long, Long, Long>())
				.withForwardedFieldsSecond("0->1;");
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testBinaryForwardedOverwritingInLine5() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input1 = env.fromElements(new Tuple3<Long, Long, Long>(3l, 4l, 5l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input2 = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input1.join(input2).where(0).equalTo(0).with(new AllForwardedExceptJoin<Long>())
				.withForwardedFieldsFirst("0->1;");
	}

	@Test(expected = SemanticProperties.InvalidSemanticAnnotationException.class)
	public void testBinaryForwardedOverwritingInLine6() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input1 = env.fromElements(new Tuple3<Long, Long, Long>(3l, 4l, 5l));
		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> input2 = env.fromElements(new Tuple3<Long, Long, Long>(3l, 2l, 1l));
		input1.join(input2).where(0).equalTo(0).with(new AllForwardedExceptJoin<Long>())
				.withForwardedFieldsSecond("0->1;");
	}

	// --------------------------------------------------------------------------------------------

	public static class NoAnnotationMapper<T> implements MapFunction<T, T> {

		@Override
		public T map(T value)  {
			return value;
		}
	}
	
	@ForwardedFields("*")
	public static class WildcardForwardedMapper<T> implements MapFunction<T, T> {

		@Override
		public T map(T value)  {
			return value;
		}
	}
	
	@ForwardedFields("0;2")
	public static class IndividualForwardedMapper<X, Y, Z> implements MapFunction<Tuple3<X, Y, Z>, Tuple3<X, Y, Z>> {

		@Override
		public Tuple3<X, Y, Z> map(Tuple3<X, Y, Z> value) {
			return value;
		}
	}

	@ForwardedFields("0->2;1->0;2->1")
	public static class ShufflingMapper<X> implements MapFunction<Tuple3<X, X, X>, Tuple3<X, X, X>> {

		@Override
		public Tuple3<X, X, X> map(Tuple3<X, X, X> value) {
			return value;
		}
	}

	@FunctionAnnotation.NonForwardedFields({"1"})
	public static class AllForwardedExceptMapper<T> implements MapFunction<T, T> {

		@Override
		public T map(T value)  {
			return value;
		}
	}

	@FunctionAnnotation.ReadFields({"0;2"})
	public static class ReadSetMapper<T> implements MapFunction<T, T> {

		@Override
		public T map(T value)  {
			return value;
		}
	}

	public static class NoAnnotationJoin<X> implements JoinFunction<Tuple2<X,X>, Tuple2<X,X>, Tuple3<X,X,X>> {

		@Override
		public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
			return null;
		}
	}

	@ForwardedFieldsFirst("0->2")
	public static class ForwardedFirstAnnotationJoin<X> implements JoinFunction<Tuple2<X,X>, Tuple2<X,X>, Tuple3<X,X,X>> {

		@Override
		public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
			return null;
		}
	}

	@ForwardedFieldsSecond("1->2")
	public static class ForwardedSecondAnnotationJoin<X> implements JoinFunction<Tuple2<X,X>, Tuple2<X,X>, Tuple3<X,X,X>> {

		@Override
		public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
			return null;
		}
	}

	@ForwardedFieldsFirst("1 -> 0")
	@ForwardedFieldsSecond("1 -> 1")
	public static class ForwardedBothAnnotationJoin<A, B, C, D> implements JoinFunction<Tuple2<A, B>, Tuple2<C, D>, Tuple2<B, D>> {

		@Override
		public Tuple2<B, D> join(Tuple2<A, B> first, Tuple2<C, D> second) {
			return new Tuple2<B, D>(first.f1, second.f1);
		}
	}

	@FunctionAnnotation.NonForwardedFieldsFirst("0;2")
	@FunctionAnnotation.NonForwardedFieldsSecond("0;1")
	public static class AllForwardedExceptJoin<X> implements JoinFunction<Tuple3<X,X,X>, Tuple3<X,X,X>, Tuple3<X,X,X>> {

		@Override
		public Tuple3<X, X, X> join(Tuple3<X, X, X> first, Tuple3<X, X, X> second) throws Exception {
			return null;
		}
	}

	@FunctionAnnotation.ReadFieldsFirst("1")
	@FunctionAnnotation.ReadFieldsSecond("0")
	public static class ReadSetJoin<X> implements JoinFunction<Tuple2<X,X>, Tuple2<X,X>, Tuple3<X,X,X>> {

		@Override
		public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
			return null;
		}
	}
}
