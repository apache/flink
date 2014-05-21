/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.api.java.functions;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase;
import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;


/**
 * This is a minimal test to verify that semantic annotations are evaluated against
 * the type information properly translated correctly to the common data flow API.
 * 
 * This covers only the constant fields annotations currently !!!
 */
@SuppressWarnings("serial")
public class SemanticPropertiesTranslationTest {
	
	/**
	 * A mapper that preserves all fields over a tuple data set.
	 */
	@Test
	public void translateUnaryFunctionAnnotationTuplesWildCard() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Long, String, Integer>> input = env.fromElements(new Tuple3<Long, String, Integer>(3l, "test", 42));
			input.map(new WildcardConstantMapper<Tuple3<Long,String,Integer>>()).print();
			
			Plan plan = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();
			
			SingleInputSemanticProperties semantics = mapper.getSemanticProperties();
			
			FieldSet fw1 = semantics.getForwardedField(0);
			FieldSet fw2 = semantics.getForwardedField(1);
			FieldSet fw3 = semantics.getForwardedField(2);
			
			assertNotNull(fw1);
			assertNotNull(fw2);
			assertNotNull(fw3);
			
			assertTrue(fw1.contains(0));
			assertTrue(fw2.contains(1));
			assertTrue(fw3.contains(2));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	/**
	 * A mapper that preserves fields 0, 1, 2 of a tuple data set.
	 */
	@Test
	public void translateUnaryFunctionAnnotationTuples() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Long, String, Integer>> input = env.fromElements(new Tuple3<Long, String, Integer>(3l, "test", 42));
			input.map(new IndividualConstantMapper<Long, String, Integer>()).print();
			
			Plan plan = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();
			
			SingleInputSemanticProperties semantics = mapper.getSemanticProperties();
			
			FieldSet fw1 = semantics.getForwardedField(0);
			FieldSet fw2 = semantics.getForwardedField(1);
			FieldSet fw3 = semantics.getForwardedField(2);
			
			assertNotNull(fw1);
			assertNotNull(fw2);
			assertNotNull(fw3);
			
			assertTrue(fw1.contains(0));
			assertTrue(fw2.contains(1));
			assertTrue(fw3.contains(2));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
//	/**
//	 * A mapper that preserves all fields over a data set of an atomic type.
//	 */
//	@Test
//	public void translateUnaryFunctionAnnotationAtomicWildCard() {
//		try {
//			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//			DataSet<Long> input = env.generateSequence(0, 1000);
//			input.map(new WildcardConstantMapper<Long>()).print();
//			
//			Plan plan = env.createProgramPlan();
//			
//			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
//			MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();
//			
//			SingleInputSemanticProperties semantics = mapper.getSemanticProperties();
//			
//			FieldSet fw1 = semantics.getForwardedField(0);
//			assertNotNull(fw1);
//			assertTrue(fw1.contains(0));
//		}
//		catch (Exception e) {
//			System.err.println(e.getMessage());
//			e.printStackTrace();
//			fail("Exception in test: " + e.getMessage());
//		}
//	}
	
//	/**
//	 * A mapper that preserves field zero of a data set of an atomic type.
//	 */
//	@Test
//	public void translateUnaryFunctionAnnotationAtomicZero() {
//		try {
//			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//			DataSet<Long> input = env.generateSequence(0, 1000);
//			input.map(new ZeroConstantMapper<Long>()).print();
//			
//			Plan plan = env.createProgramPlan();
//			
//			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
//			MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();
//			
//			SingleInputSemanticProperties semantics = mapper.getSemanticProperties();
//			
//			FieldSet fw1 = semantics.getForwardedField(0);
//			FieldSet fw2 = semantics.getForwardedField(1);
//			FieldSet fw3 = semantics.getForwardedField(2);
//			
//			assertNotNull(fw1);
//			assertNotNull(fw2);
//			assertNotNull(fw3);
//			
//			assertTrue(fw1.contains(0));
//			assertTrue(fw2.contains(1));
//			assertTrue(fw3.contains(2));
//		}
//		catch (Exception e) {
//			System.err.println(e.getMessage());
//			e.printStackTrace();
//			fail("Exception in test: " + e.getMessage());
//		}
//	}
	
	
	/**
	 * A join that preserves tuple fields from both sides.
	 */
	@Test
	public void translateBinaryFunctionAnnotationTuples() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Long, String>> input1 = env.fromElements(new Tuple2<Long, String>(3l, "test"));
			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Long, Double>> input2 = env.fromElements(new Tuple2<Long, Double>(3l, 3.1415));
			
			input1.join(input2).where(0).equalTo(0).with(new ForwardingTupleJoin<Long, String, Long, Double>())
				.print();
			
			Plan plan = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
			JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();
			
			DualInputSemanticProperties semantics = join.getSemanticProperties();
			
			FieldSet fw11 = semantics.getForwardedField1(0);
			FieldSet fw12 = semantics.getForwardedField1(1);
			FieldSet fw21 = semantics.getForwardedField2(0);
			FieldSet fw22 = semantics.getForwardedField2(1);
			
			assertNull(fw11);
			assertNull(fw21);
			
			assertNotNull(fw12);
			assertNotNull(fw22);
			
			assertTrue(fw12.contains(0));
			assertTrue(fw22.contains(1));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
//	/**
//	 * A join that preserves atomic fields from both sides.
//	 */
//	@Test
//	public void translateBinaryFunctionAnnotationAtomicType() {
//		try {
//			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//			
//			DataSet<Long> input1 = env.generateSequence(0, 1000);
//			DataSet<Long> input2 = env.generateSequence(0, 1000);
//			
//			input1.join(input2)
//					.where(new KeySelector<Long, Long>() { public Long getKey(Long value) { return value; }})
//					.equalTo(new KeySelector<Long, Long>() { public Long getKey(Long value) { return value; }})
//					.with(new ForwardingBasicJoin<Long, Long>())
//				.print();
//			
//			Plan plan = env.createProgramPlan();
//			
//			GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
//			JoinOperatorBase<?, ?, ?, ?> join = (JoinOperatorBase<?, ?, ?, ?>) sink.getInput();
//			
//			DualInputSemanticProperties semantics = join.getSemanticProperties();
//			
//			FieldSet fw11 = semantics.getForwardedField1(0);
//			FieldSet fw12 = semantics.getForwardedField1(1);
//			FieldSet fw21 = semantics.getForwardedField2(0);
//			FieldSet fw22 = semantics.getForwardedField2(1);
//			
//			assertNull(fw11);
//			assertNull(fw21);
//			
//			assertNotNull(fw12);
//			assertNotNull(fw22);
//			
//			assertTrue(fw12.contains(0));
//			assertTrue(fw22.contains(1));
//		}
//		catch (Exception e) {
//			System.err.println(e.getMessage());
//			e.printStackTrace();
//			fail("Exception in test: " + e.getMessage());
//		}
//	}
	
	// --------------------------------------------------------------------------------------------
	
	
	@ConstantFields("*")
	public static class WildcardConstantMapper<T> extends MapFunction<T, T> {

		@Override
		public T map(T value)  {
			return value;
		}
	}
	
	@ConstantFields("0->0;1->1;2->2")
	public static class IndividualConstantMapper<X, Y, Z> extends MapFunction<Tuple3<X, Y, Z>, Tuple3<X, Y, Z>> {

		@Override
		public Tuple3<X, Y, Z> map(Tuple3<X, Y, Z> value) {
			return value;
		}
	}
	
	@ConstantFields("0")
	public static class ZeroConstantMapper<T> extends MapFunction<T, T> {

		@Override
		public T map(T value)  {
			return value;
		}
	}
	
	@ConstantFieldsFirst("1 -> 0")
	@ConstantFieldsSecond("1 -> 1")
	public static class ForwardingTupleJoin<A, B, C, D> extends JoinFunction<Tuple2<A, B>, Tuple2<C, D>, Tuple2<B, D>> {

		@Override
		public Tuple2<B, D> join(Tuple2<A, B> first, Tuple2<C, D> second) {
			return new Tuple2<B, D>(first.f1, second.f1);
		}
	}
	
	@ConstantFieldsFirst("0 -> 0")
	@ConstantFieldsSecond("0 -> 1")
	public static class ForwardingBasicJoin<A, B> extends JoinFunction<A, B, Tuple2<A, B>> {

		@Override
		public Tuple2<A, B> join(A first, B second) {
			return new Tuple2<A, B>(first, second);
		}
	}
}
