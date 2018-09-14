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

package org.apache.flink.api.java.sca;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link UdfAnalyzer}.
 */
@SuppressWarnings("serial")
public class UdfAnalyzerTest {

	private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE2_TYPE_INFO = TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

	private static final TypeInformation<Tuple2<String, String>> STRING_STRING_TUPLE2_TYPE_INFO = TypeInformation.of(new TypeHint<Tuple2<String, String>>(){});

	@ForwardedFields("f0->*")
	private static class Map1 implements MapFunction<Tuple2<String, Integer>, String> {
		public String map(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

	@Test
	public void testSingleFieldExtract() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map1.class,
			STRING_INT_TUPLE2_TYPE_INFO, Types.STRING);
	}

	@ForwardedFields("f0->f0;f0->f1")
	private static class Map2 implements MapFunction<Tuple2<String, Integer>, Tuple2<String, String>> {
		public Tuple2<String, String> map(Tuple2<String, Integer> value) throws Exception {
			return new Tuple2<String, String>(value.f0, value.f0);
		}
	}

	@Test
	public void testForwardIntoTuple() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map2.class,
			STRING_INT_TUPLE2_TYPE_INFO, STRING_STRING_TUPLE2_TYPE_INFO);
	}

	private static class Map3 implements MapFunction<String[], Integer> {
		@Override
		public Integer map(String[] value) throws Exception {
			return value.length;
		}

	}

	@Test
	public void testForwardWithArrayAttrAccess() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map3.class,
			TypeInformation.of(new TypeHint<String[]>(){}), Types.INT);
	}

	private static class Map4 implements MapFunction<MyPojo, String> {
		@Override
		public String map(MyPojo value) throws Exception {
			return value.field2;
		}
	}

	@Test
	public void testForwardWithGenericTypePublicAttrAccess() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map4.class,
			new GenericTypeInfo<>(MyPojo.class), Types.STRING);
	}

	@ForwardedFields("field2->*")
	private static class Map5 implements MapFunction<MyPojo, String> {
		@Override
		public String map(MyPojo value) throws Exception {
			return value.field2;
		}
	}

	@Test
	public void testForwardWithPojoPublicAttrAccess() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map5.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}), Types.STRING);
	}

	@ForwardedFields("field->*")
	private static class Map6 implements MapFunction<MyPojo, String> {
		@Override
		public String map(MyPojo value) throws Exception {
			return value.field;
		}
	}

	@Test
	public void testForwardWithPojoPrivateAttrAccess() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map6.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}), Types.STRING);
	}

	@ForwardedFields("f0->f1")
	private static class Map7 implements MapFunction<Tuple2<String, Integer>, Tuple2<String, String>> {
		public Tuple2<String, String> map(Tuple2<String, Integer> value) throws Exception {
			if (value.f0.equals("whatever")) {
				return new Tuple2<String, String>(value.f0, value.f0);
			} else {
				return new Tuple2<String, String>("hello", value.f0);
			}
		}
	}

	@Test
	public void testForwardIntoTupleWithCondition() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map7.class,
			STRING_INT_TUPLE2_TYPE_INFO, STRING_STRING_TUPLE2_TYPE_INFO);
	}

	private static class Map8 implements MapFunction<Tuple2<String, String>, String> {
		public String map(Tuple2<String, String> value) throws Exception {
			if (value.f0.equals("whatever")) {
				return value.f0;
			} else {
				return value.f1;
			}
		}
	}

	@Test
	public void testSingleFieldExtractWithCondition() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map8.class,
			STRING_STRING_TUPLE2_TYPE_INFO, Types.STRING);
	}

	@ForwardedFields("*->f0")
	private static class Map9 implements MapFunction<String, Tuple1<String>> {
		private Tuple1<String> tuple = new Tuple1<String>();

		public Tuple1<String> map(String value) throws Exception {
			tuple.f0 = value;
			return tuple;
		}
	}

	@Test
	public void testForwardIntoTupleWithInstanceVar() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map9.class, Types.STRING,
			TypeInformation.of(new TypeHint<Tuple1<String>>(){}));
	}

	@ForwardedFields("*->f0.f0")
	private static class Map10 implements MapFunction<String, Tuple1<Tuple1<String>>> {
		private Tuple1<Tuple1<String>> tuple = new Tuple1<Tuple1<String>>();

		public Tuple1<Tuple1<String>> map(String value) throws Exception {
			tuple.f0.f0 = value;
			return tuple;
		}
	}

	@Test
	public void testForwardIntoTupleWithInstanceVar2() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map10.class, Types.STRING,
			TypeInformation.of(new TypeHint<Tuple1<Tuple1<String>>>(){}));
	}

	@ForwardedFields("*->f1")
	private static class Map11 implements MapFunction<String, Tuple2<String, String>> {
		private Tuple2<String, String> tuple = new Tuple2<String, String>();

		public Tuple2<String, String> map(String value) throws Exception {
			tuple.f0 = value;
			modify();
			tuple.f1 = value;
			return tuple;
		}

		private void modify() {
			tuple.f0 = null;
		}
	}

	@Test
	public void testForwardIntoTupleWithInstanceVarChangedByOtherMethod() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map11.class, Types.STRING,
			STRING_STRING_TUPLE2_TYPE_INFO);
	}

	@ForwardedFields("f0->f0.f0;f0->f1.f0")
	private static class Map12 implements MapFunction<Tuple2<String, Integer>, Tuple2<Tuple1<String>, Tuple1<String>>> {
		public Tuple2<Tuple1<String>, Tuple1<String>> map(Tuple2<String, Integer> value) throws Exception {
			return new Tuple2<Tuple1<String>, Tuple1<String>>(new Tuple1<String>(value.f0), new Tuple1<String>(
					value.f0));
		}
	}

	@Test
	public void testForwardIntoNestedTuple() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map12.class,
			STRING_INT_TUPLE2_TYPE_INFO,
			TypeInformation.of(new TypeHint<Tuple2<Tuple1<String>, Tuple1<String>>>(){}));
	}

	@ForwardedFields("f0->f1.f0")
	private static class Map13 implements MapFunction<Tuple2<String, Integer>, Tuple2<Tuple1<String>, Tuple1<String>>> {
		@SuppressWarnings("unchecked")
		public Tuple2<Tuple1<String>, Tuple1<String>> map(Tuple2<String, Integer> value) throws Exception {
			Tuple2<?, ?> t = new Tuple2<Tuple1<String>, Tuple1<String>>(new Tuple1<String>(value.f0),
					new Tuple1<String>(value.f0));
			t.f0 = null;
			return (Tuple2<Tuple1<String>, Tuple1<String>>) t;
		}
	}

	@Test
	public void testForwardIntoNestedTupleWithVarAndModification() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map13.class,
			STRING_INT_TUPLE2_TYPE_INFO,
			TypeInformation.of(new TypeHint<Tuple2<Tuple1<String>, Tuple1<String>>>(){}));
	}

	@ForwardedFields("f0")
	private static class Map14 implements MapFunction<Tuple2<String, Integer>, Tuple2<String, String>> {
		public Tuple2<String, String> map(Tuple2<String, Integer> value) throws Exception {
			Tuple2<String, String> t = new Tuple2<String, String>();
			t.f0 = value.f0;
			return t;
		}
	}

	@Test
	public void testForwardIntoTupleWithAssignment() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map14.class,
			STRING_INT_TUPLE2_TYPE_INFO, STRING_STRING_TUPLE2_TYPE_INFO);
	}

	@ForwardedFields("f0.f0->f0")
	private static class Map15 implements MapFunction<Tuple2<Tuple1<String>, Integer>, Tuple2<String, String>> {
		public Tuple2<String, String> map(Tuple2<Tuple1<String>, Integer> value) throws Exception {
			Tuple2<String, String> t = new Tuple2<String, String>();
			t.f0 = value.f0.f0;
			return t;
		}
	}

	@Test
	public void testForwardIntoTupleWithInputPath() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map15.class,
			TypeInformation.of(new TypeHint<Tuple2<Tuple1<String>, Integer>>(){}),
			STRING_STRING_TUPLE2_TYPE_INFO);
	}

	@ForwardedFields("field->field2;field2->field")
	private static class Map16 implements MapFunction<MyPojo, MyPojo> {
		public MyPojo map(MyPojo value) throws Exception {
			MyPojo p = new MyPojo();
			p.setField(value.getField2());
			p.setField2(value.getField());
			return p;
		}
	}

	@Test
	public void testForwardIntoPojoByGettersAndSetters() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map16.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}), TypeInformation.of(new TypeHint<MyPojo>(){}));
	}

	private static class Map17 implements MapFunction<String, Tuple1<String>> {
		private Tuple1<String> tuple = new Tuple1<String>();

		public Tuple1<String> map(String value) throws Exception {
			if (!tuple.f0.equals("")) {
				tuple.f0 = "empty";
			} else {
				tuple.f0 = value;
			}
			return tuple;
		}
	}

	@Test
	public void testForwardIntoTupleWithInstanceVarAndCondition() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map17.class, Types.STRING,
			TypeInformation.of(new TypeHint<Tuple1<String>>(){}));
	}

	private static class Map18 implements MapFunction<Tuple1<String>, ArrayList<String>> {
		private ArrayList<String> list = new ArrayList<String>();

		public ArrayList<String> map(Tuple1<String> value) throws Exception {
			list.add(value.f0);
			return list;
		}
	}

	@Test
	public void testForwardIntoUnsupportedObject() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map18.class,
			TypeInformation.of(new TypeHint<Tuple1<String>>(){}), TypeInformation.of(new TypeHint<java.util.ArrayList>(){}));
	}

	@ForwardedFields("*->f0")
	private static class Map19 implements MapFunction<Integer, Tuple1<Integer>> {
		@Override
		public Tuple1<Integer> map(Integer value) throws Exception {
			Tuple1<Integer> tuple = new Tuple1<Integer>();
			tuple.f0 = value;
			Tuple1<Integer> tuple2 = new Tuple1<Integer>();
			tuple2.f0 = tuple.f0;
			return tuple2;
		}
	}

	@Test
	public void testForwardWithNewTupleToNewTupleAssignment() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map19.class, Types.INT,
			TypeInformation.of(new TypeHint<Tuple1<Integer>>(){}));
	}

	@ForwardedFields("f0;f1")
	private static class Map20 implements
	MapFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>> {

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(Tuple4<Integer, Integer, Integer, Integer> value)
				throws Exception {
			Tuple4<Integer, Integer, Integer, Integer> t = new Tuple4<Integer, Integer, Integer, Integer>();
			t.f0 = value.getField(0);
			t.f1 = value.getField((int) 1L);
			return t;
		}
	}

	@Test
	public void testForwardWithGetMethod() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map20.class,
			TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Integer>>(){}),
			TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Integer>>(){}));
	}

	@ForwardedFields("f0->f1;f1->f0")
	private static class Map21 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
			Integer i = value.f0;
			value.setField(value.f1, 0);
			value.setField(i, 1);
			return value;
		}
	}

	@Test
	public void testForwardWithSetMethod() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map21.class,
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}), TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
	}

	@ForwardedFields("f0->f1;f1->f0")
	private static class Map22 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
			Tuple2<Integer, Integer> t = new Tuple2<Integer, Integer>();
			t.setField(value.f1, 0);
			t.setField(value.getField(0), 1);
			return t;
		}
	}

	@Test
	public void testForwardIntoNewTupleWithSetMethod() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map22.class,
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}), TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
	}

	@ForwardedFields("*")
	private static class Map23 implements MapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			if (value.f0.equals(23)) {
				return new Tuple1<Integer>(value.<Integer> getField(0));
			} else if (value.f0.equals(22)) {
				Tuple1<Integer> inputContainer = new Tuple1<Integer>();
				inputContainer.f0 = value.f0;
				return new Tuple1<Integer>(inputContainer.<Integer> getField(0));
			} else {
				return value;
			}
		}
	}

	@Test
	public void testForwardWithGetMethod2() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map23.class,
			TypeInformation.of(new TypeHint<Tuple1<Integer>>(){}), TypeInformation.of(new TypeHint<Tuple1<Integer>>(){}));
	}

	private static class Map24 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
			value.setField(2, 0);
			int i = 5;
			value.setField(i * i + 2, 1);
			return value;
		}
	}

	@Test
	public void testForwardWithSetMethod2() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map24.class,
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}), TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
	}

	@ForwardedFields("f1->f0;f1")
	private static class Map25 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
			value.f0 = value.f1;
			return value;
		}
	}

	@Test
	public void testForwardWithModifiedInput() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map25.class,
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}), TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
	}

	@ForwardedFields("*->1")
	private static class Map26 implements MapFunction<Integer, Tuple2<Integer, Integer>> {

		@Override
		public Tuple2<Integer, Integer> map(Integer value) throws Exception {
			Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>();
			// non-input content
			if (tuple.equals(new Tuple2<Integer, Integer>())) {
				tuple.f0 = 123456;
			}
			if (tuple.equals(new Tuple2<Integer, Integer>())) {
				tuple.f0 = value;
			}
			// forwarding
			tuple.f1 = value;
			return tuple;
		}
	}

	@Test
	public void testForwardWithTuplesGetSetFieldMethods() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map26.class, Types.INT,
			TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
	}

	@ForwardedFields("2->3;3->7")
	private static class Map27
	implements
	MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
	Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
		@Override
		public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(
				Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value)
						throws Exception {
			Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple =
				new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>();
			// non-input content
			if (tuple.f0 == null) {
				tuple.setField(123456, 0);
			} else {
				tuple.setField(value.f0, 0);
			}
			// forwarding
			tuple.setField(value.f2, 3);
			tuple.setField(value.f3, 7);
			// TODO multiple mapping is unsupported yet
			//tuple.setField(value.f1, 3);
			return tuple;
		}
	}

	@Test
	public void testForwardWithTuplesGetSetFieldMethods2() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map27.class,
			TypeInformation.of(new TypeHint<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(){}),
			TypeInformation.of(new TypeHint<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(){}));
	}

	private static class Map28 implements MapFunction<Integer, Integer> {
		@Override
		public Integer map(Integer value) throws Exception {
			if (value == null) {
				value = 123;
			}
			return value;
		}
	}

	@Test
	public void testForwardWithBranching1() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map28.class, Types.INT, Types.INT);
	}

	@ForwardedFields("0")
	private static class Map29 implements MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {
		@Override
		public Tuple3<String, String, String> map(Tuple3<String, String, String> value) throws Exception {
			String tmp = value.f0;
			for (int i = 0; i < 2; i++) {
				value.setField("Test", i);
			}
			Tuple3<String, String, String> tuple;
			if (value.f0.equals("x")) {
				tuple = new Tuple3<String, String, String>(tmp, value.f0, null);
			} else {
				tuple = new Tuple3<String, String, String>(tmp, value.f0, "");
			}
			return tuple;
		}
	}

	@Test
	public void testForwardWithBranching2() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map29.class,
			TypeInformation.of(new TypeHint<Tuple3<String, String, String>>(){}),
			TypeInformation.of(new TypeHint<Tuple3<String, String, String>>(){}));
	}

	private static class Map30 implements MapFunction<Tuple2<String, String>, String> {
		@Override
		public String map(Tuple2<String, String> value) throws Exception {
			String tmp;
			if (value.f0.equals("")) {
				tmp = value.f0;
			} else {
				tmp = value.f1;
			}
			return tmp;
		}
	}

	@Test
	public void testForwardWithBranching3() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map30.class,
			STRING_STRING_TUPLE2_TYPE_INFO, Types.STRING);
	}

	@ForwardedFields("1->1;1->0")
	private static class Map31 implements MapFunction<Tuple2<String, String>, ExtendingTuple> {
		@Override
		public ExtendingTuple map(Tuple2<String, String> value) throws Exception {
			ExtendingTuple t = new ExtendingTuple();
			t.f1 = value.f1;
			t.setFirstField();
			t.f0 = t.getSecondField();
			return t;
		}
	}

	@Test
	public void testForwardWithInheritance() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map31.class,
			STRING_STRING_TUPLE2_TYPE_INFO, STRING_STRING_TUPLE2_TYPE_INFO);
	}

	@ForwardedFields("*")
	private static class Map32
	implements
	MapFunction<Tuple8<Boolean, Character, Byte, Short, Integer, Long, Float, Double>,
	Tuple8<Boolean, Character, Byte, Short, Integer, Long, Float, Double>> {

		@Override
		public Tuple8<Boolean, Character, Byte, Short, Integer, Long, Float, Double> map(
				Tuple8<Boolean, Character, Byte, Short, Integer, Long, Float, Double> value) throws Exception {
			boolean f0 = value.f0;
			char f1 = value.f1;
			byte f2 = value.f2;
			short f3 = value.f3;
			int f4 = value.f4;
			long f5 = value.f5;
			float f6 = value.f6;
			double f7 = value.f7;
			return new Tuple8<Boolean, Character, Byte, Short, Integer, Long, Float, Double>(
					f0, f1, f2, f3, f4, f5, f6, f7);
		}
	}

	@Test
	public void testForwardWithUnboxingAndBoxing() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map32.class,
			TypeInformation.of(new TypeHint<Tuple8<Boolean, Character, Byte, Short, Integer, Long, Float, Double>>(){}),
			TypeInformation.of(new TypeHint<Tuple8<Boolean, Character, Byte, Short, Integer, Long, Float, Double>>(){}));
	}

	private static class Map33 implements MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
			Tuple2<Long, Long> t = new Tuple2<Long, Long>();
			if (value.f0 != null) {
				t.f0 = value.f0;
			}
			else {
				t.f0 = value.f1;
			}
			return t;
		}
	}

	@Test
	public void testForwardWithBranching4() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map33.class, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	@ForwardedFields("1")
	private static class Map34 implements MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		private Tuple2<Long, Long> t;
		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
			if (value != new Object()) {
				return value;
			}
			else if (value.f0 == 1L && value.f1 == 2L) {
				t = value;
				t.f0 = 23L;
				return t;
			}
			return new Tuple2<Long, Long>(value.f0, value.f1);
		}
	}

	@Test
	public void testForwardWithBranching5() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map34.class, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	private static class Map35 implements MapFunction<String[], Tuple2<String[], String[]>> {
		@Override
		public Tuple2<String[], String[]> map(String[] value) throws Exception {
			String[] tmp = value;
			value[0] = "Hello";
			return new Tuple2<String[], String[]>(value, tmp);
		}
	}

	@Test
	public void testForwardWithArrayModification() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map35.class, TypeInformation.of(new TypeHint<String[]>(){}),
			TypeInformation.of(new TypeHint<Tuple2<String[], String[]>>(){}));
	}

	private static class Map36 implements MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {
		@Override
		public Tuple3<String, String, String> map(Tuple3<String, String, String> value) throws Exception {
			int i = 0;
			do {
				value.setField("", i);
				i++;
			} while (i >= 2);
			return value;
		}
	}

	@Test
	public void testForwardWithBranching6() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map36.class, TypeInformation.of(new TypeHint<Tuple3<String, String, String>>(){}),
			TypeInformation.of(new TypeHint<Tuple3<String, String, String>>(){}));
	}

	private static class Map37 implements MapFunction<Tuple1<Tuple1<String>>, Tuple1<Tuple1<String>>> {
		@SuppressWarnings("unchecked")
		@Override
		public Tuple1<Tuple1<String>> map(Tuple1<Tuple1<String>> value) throws Exception {
			((Tuple1<String>) value.getField(Integer.parseInt("2."))).f0 = "Hello";
			return value;
		}
	}

	@Test
	public void testForwardWithGetAndModification() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map37.class, TypeInformation.of(new TypeHint<Tuple1<Tuple1<String>>>(){}),
			TypeInformation.of(new TypeHint<Tuple1<Tuple1<String>>>(){}));
	}

	@ForwardedFields("field")
	private static class Map38 implements MapFunction<MyPojo2, MyPojo2> {
		@Override
		public MyPojo2 map(MyPojo2 value) throws Exception {
			value.setField2("test");
			return value;
		}
	}

	@Test
	public void testForwardWithInheritance2() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map38.class,
			TypeInformation.of(new TypeHint<MyPojo2>(){}),
			TypeInformation.of(new TypeHint<MyPojo2>(){}));
	}

	private static class Map39 implements MapFunction<MyPojo, MyPojo> {
		@Override
		public MyPojo map(MyPojo value) throws Exception {
			MyPojo mp = new MyPojo();
			mp.field = value.field2;
			return mp;
		}
	}

	@Test
	public void testForwardWithGenericTypeOutput() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map39.class,
			TypeInformation.of(new TypeHint<GenericTypeInfo<MyPojo>>(){}),
			TypeInformation.of(new TypeHint<GenericTypeInfo<MyPojo>>(){}));
	}

	@ForwardedFields("field2")
	private static class Map40 implements MapFunction<MyPojo, MyPojo> {
		@Override
		public MyPojo map(MyPojo value) throws Exception {
			return recursiveFunction(value);
		}

		private MyPojo recursiveFunction(MyPojo value) {
			if (value.field.equals("xyz")) {
				value.field = value.field + "x";
				return recursiveFunction(value);
			}
			return value;
		}
	}

	@Test
	public void testForwardWithRecursion() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map40.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}),
			TypeInformation.of(new TypeHint<MyPojo>(){}));
	}

	@ForwardedFields("field;field2")
	private static class Map41 extends RichMapFunction<MyPojo, MyPojo> {
		private MyPojo field;
		@Override
		public MyPojo map(MyPojo value) throws Exception {
			field = value;
			getRuntimeContext().getIntCounter("test").getLocalValue();
			return field;
		}
	}

	@Test
	public void testForwardWithGetRuntimeContext() {
		compareAnalyzerResultWithAnnotationsSingleInput(MapFunction.class, Map41.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}),
			TypeInformation.of(new TypeHint<MyPojo>(){}));
	}

	@ForwardedFields("*")
	private static class FlatMap1 implements FlatMapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		@Override
		public void flatMap(Tuple1<Integer> value, Collector<Tuple1<Integer>> out) throws Exception {
			out.collect(value);
		}
	}

	@Test
	public void testForwardWithCollector() {
		compareAnalyzerResultWithAnnotationsSingleInput(FlatMapFunction.class, FlatMap1.class, TypeInformation.of(new TypeHint<Tuple1<Integer>>(){}),
			TypeInformation.of(new TypeHint<Tuple1<Integer>>(){}));
	}

	@ForwardedFields("0->1;1->0")
	private static class FlatMap2 implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(invertedEdge);
			out.collect(invertedEdge);
		}
	}

	@Test
	public void testForwardWith2Collectors() {
		compareAnalyzerResultWithAnnotationsSingleInput(FlatMapFunction.class, FlatMap2.class, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	private static class FlatMap3 implements FlatMapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		@Override
		public void flatMap(Tuple1<Integer> value, Collector<Tuple1<Integer>> out) throws Exception {
			addToCollector(out);
			out.collect(value);
		}

		private void addToCollector(Collector<Tuple1<Integer>> out) {
			out.collect(new Tuple1<Integer>());
		}
	}

	@Test
	public void testForwardWithCollectorPassing() {
		compareAnalyzerResultWithAnnotationsSingleInput(FlatMapFunction.class, FlatMap3.class, TypeInformation.of(new TypeHint<Tuple1<Integer>>(){}),
			TypeInformation.of(new TypeHint<Tuple1<Integer>>(){}));
	}

	@ForwardedFieldsFirst("f1->f1")
	@ForwardedFieldsSecond("f1->f0")
	private static class Join1 implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}

	@Test
	public void testForwardWithDualInput() {
		compareAnalyzerResultWithAnnotationsDualInput(JoinFunction.class, Join1.class, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	@ForwardedFieldsFirst("*")
	private static class Join2 implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}

	@Test
	public void testForwardWithDualInputAndCollector() {
		compareAnalyzerResultWithAnnotationsDualInput(FlatJoinFunction.class, Join2.class, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}),
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
	}

	@ForwardedFields("0")
	private static class GroupReduce1 implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(values.iterator().next());
		}
	}

	@Test
	public void testForwardWithIterable() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce1.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), new String[] { "0" });
	}

	@ForwardedFields("1->0")
	private static class GroupReduce2 implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
			final Iterator<Tuple2<Long, Long>> it = values.iterator();
			Tuple2<Long, Long> outTuple = new Tuple2<Long, Long>();
			Tuple2<Long, Long> first = it.next();
			outTuple.f0 = first.f1;
			outTuple.f1 = first.f0;

			while (it.hasNext()) {
				Tuple2<Long, Long> t = it.next();
				if (t.f0 == 42) {
					outTuple.f1 += t.f0;
				}
			}
			out.collect(outTuple);
		}
	}

	@Test
	public void testForwardWithIterable2() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce2.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), new String[] { "0", "1" });
	}

	@ForwardedFields("field2")
	private static class GroupReduce3 implements GroupReduceFunction<MyPojo, MyPojo> {
		@Override
		public void reduce(Iterable<MyPojo> values, Collector<MyPojo> out) throws Exception {
			for (MyPojo value : values) {
				out.collect(value);
			}
		}
	}

	@Test
	public void testForwardWithIterable3() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce3.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}),
			TypeInformation.of(new TypeHint<MyPojo>(){}), new String[] { "field2" });
	}

	@ForwardedFields("f0->*")
	private static class GroupReduce4 implements GroupReduceFunction<Tuple2<Long, Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Long> out) throws Exception {
			Long id = 0L;
			for (Tuple2<Long, Long> value : values) {
				id = value.f0;
			}
			out.collect(id);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumption() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce4.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), Types.LONG, new String[] { "f0" });
	}

	@ForwardedFields("f0->*")
	private static class GroupReduce4Javac implements GroupReduceFunction<Tuple2<Long, Long>, Long> {
		@SuppressWarnings("unchecked")
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Long> out) throws Exception {
			Long id = 0L;
			@SuppressWarnings("rawtypes")
			Iterator it = values.iterator();
			if (it.hasNext()) {
				id = ((Tuple2<Long, Long>) it.next()).f0;
			}
			else {
				System.out.println("hello world");
			}
			out.collect(id);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumptionForJavac() {
		// this test simulates javac behaviour in Eclipse IDE
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce4Javac.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), Types.LONG, new String[] { "f0" });
	}

	private static class GroupReduce5 implements GroupReduceFunction<Tuple2<Long, Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Long> out) throws Exception {
			Long id = 0L;
			for (Tuple2<Long, Long> value : values) {
				id = value.f0;
				if (value != null) {
					id = value.f1;
				}
			}
			out.collect(id);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumption2() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce5.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), Types.LONG, new String[] { "f1" });
	}

	private static class GroupReduce6 implements GroupReduceFunction<Tuple2<Long, Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Long> out) throws Exception {
			Long id = 0L;
			for (Tuple2<Long, Long> value : values) {
				id = value.f0;
			}
			id = 0L;
			out.collect(id);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumption3() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce6.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), Types.LONG, new String[] { "f0" });
	}

	private static class GroupReduce7 implements GroupReduceFunction<Tuple2<Long, Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Long> out) throws Exception {
			Long id = 0L;
			for (Tuple2<Long, Long> value : values) {
				id = value.f0;
			}
			id = 0L;
			out.collect(id);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumption4() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce7.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), Types.LONG, new String[] { "f0" });
	}

	@ForwardedFields("f0->*")
	private static class GroupReduce8 implements GroupReduceFunction<Tuple2<Long, Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Long> out) throws Exception {
			Long id = 0L;
			Iterator<Tuple2<Long, Long>> it = values.iterator();
			while (it.hasNext()) {
				id = it.next().f0;
			}
			out.collect(id);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumption5() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce8.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), Types.LONG, new String[] { "f0" });
	}

	@ForwardedFields("f0")
	private static class GroupReduce9 implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
			Tuple2<Long, Long> rv = null;
			Iterator<Tuple2<Long, Long>> it = values.iterator();
			while (it.hasNext()) {
				rv = it.next();
			}
			out.collect(rv);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumption6() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce9.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), new String[] { "f0" });
	}

	private static class GroupReduce10 implements GroupReduceFunction<Tuple2<Long, Long>, Boolean> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Boolean> out) throws Exception {
			Iterator<Tuple2<Long, Long>> it = values.iterator();
			boolean f = it.hasNext();
			if (!f) {
				System.out.println();
			}
			if (f) {
				System.out.println();
			}
			out.collect(f);
		}
	}

	@Test
	public void testForwardWithAtLeastOneIterationAssumption7() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(GroupReduceFunction.class, GroupReduce10.class,
			TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}), Types.BOOLEAN, new String[] { "f0" });
	}

	@ForwardedFields("field")
	private static class Reduce1 implements ReduceFunction<MyPojo> {
		@Override
		public MyPojo reduce(MyPojo value1, MyPojo value2) throws Exception {
			return new MyPojo(value1.getField(), value2.getField2());
		}
	}

	@Test
	public void testForwardWithReduce() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(ReduceFunction.class, Reduce1.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}),
			TypeInformation.of(new TypeHint<MyPojo>(){}),
			new String[] { "field" });
	}

	@ForwardedFields("field")
	private static class Reduce2 implements ReduceFunction<MyPojo> {
		@Override
		public MyPojo reduce(MyPojo value1, MyPojo value2) throws Exception {
			if (value1.field != null && value1.field.isEmpty()) {
				return value2;
			}
			return value1;
		}
	}

	@Test
	public void testForwardWithBranchingReduce() {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(ReduceFunction.class, Reduce2.class,
			TypeInformation.of(new TypeHint<MyPojo>(){}),
			TypeInformation.of(new TypeHint<MyPojo>(){}),
			new String[] { "field" });
	}

	private static class NullReturnMapper1 implements MapFunction<String, String> {
		@Override
		public String map(String value) throws Exception {
			return null;
		}
	}

	private static class NullReturnMapper2 implements MapFunction<String, String> {
		@Override
		public String map(String value) throws Exception {
			if (value.equals("test")) {
				return null;
			}
			return "";
		}
	}

	private static class NullReturnFlatMapper implements FlatMapFunction<String, String> {
		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			String s = null;
			if ("dd".equals("")) {
				s = "";
			}
			out.collect(s);
		}
	}

	@Test
	public void testNullReturnException() {
		try {
			final UdfAnalyzer ua = new UdfAnalyzer(MapFunction.class, NullReturnMapper1.class, "operator",
					BasicTypeInfo.STRING_TYPE_INFO, null, BasicTypeInfo.STRING_TYPE_INFO, null, null, true);
			ua.analyze();
			Assert.fail();
		}
		catch (CodeErrorException e) {
			// ok
		}
		try {
			final UdfAnalyzer ua = new UdfAnalyzer(MapFunction.class, NullReturnMapper2.class, "operator",
					BasicTypeInfo.STRING_TYPE_INFO, null, BasicTypeInfo.STRING_TYPE_INFO, null, null, true);
			ua.analyze();
			Assert.fail();
		}
		catch (CodeErrorException e) {
			// ok
		}
		try {
			final UdfAnalyzer ua = new UdfAnalyzer(FlatMapFunction.class, NullReturnFlatMapper.class, "operator",
					BasicTypeInfo.STRING_TYPE_INFO, null, BasicTypeInfo.STRING_TYPE_INFO, null, null, true);
			ua.analyze();
			Assert.fail();
		}
		catch (CodeErrorException e) {
			// ok
		}
	}

	private static class PutStaticMapper implements MapFunction<String, String> {
		public static String test = "";

		@Override
		public String map(String value) throws Exception {
			test = "test";
			return "";
		}
	}

	@Test
	public void testPutStaticException() {
		try {
			final UdfAnalyzer ua = new UdfAnalyzer(MapFunction.class, PutStaticMapper.class, "operator",
					BasicTypeInfo.STRING_TYPE_INFO, null, BasicTypeInfo.STRING_TYPE_INFO, null, null, true);
			ua.analyze();
			Assert.fail();
		}
		catch (CodeErrorException e) {
			// ok
		}
	}

	private static class FilterMod1 implements FilterFunction<Tuple2<String, String>> {

		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			value.f0 = value.f1;
			return false;
		}
	}

	@Test
	public void testFilterModificationException1() {
		try {
			final UdfAnalyzer ua = new UdfAnalyzer(FilterFunction.class, FilterMod1.class, "operator",
				STRING_STRING_TUPLE2_TYPE_INFO, null, null, null, null, true);
			ua.analyze();
			Assert.fail();
		}
		catch (CodeErrorException e) {
			// ok
		}
	}

	private static class FilterMod2 implements FilterFunction<Tuple2<String, String>> {

		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			value.f0 = "";
			return false;
		}
	}

	@Test
	public void testFilterModificationException2() {
		try {
			final UdfAnalyzer ua = new UdfAnalyzer(FilterFunction.class, FilterMod2.class, "operator",
				STRING_STRING_TUPLE2_TYPE_INFO, null, null, null, null, true);
			ua.analyze();
			Assert.fail();
		}
		catch (CodeErrorException e) {
			// ok
		}
	}

	// --------------------------------------------------------------------------------------------
	// Utils
	// --------------------------------------------------------------------------------------------

	/**
	 * Simple POJO with two fields.
	 */
	public static class MyPojo {
		private String field;
		public String field2;

		public MyPojo() {
			// default constructor
		}

		public MyPojo(String field, String field2) {
			this.field = field;
			this.field2 = field2;
		}

		public String getField() {
			return field;
		}

		public void setField(String field) {
			this.field = field;
		}

		public String getField2() {
			return field2;
		}

		public void setField2(String field2) {
			this.field2 = field2;
		}
	}

	/**
	 * Simple POJO extending {@link MyPojo}.
	 */
	public static class MyPojo2 extends MyPojo {

		public MyPojo2() {
			// default constructor
		}
	}

	private static class ExtendingTuple extends Tuple2<String, String> {
		public void setFirstField() {
			setField("Hello", 0);
		}

		public String getSecondField() {
			return getField(1);
		}
	}

	public static void compareAnalyzerResultWithAnnotationsSingleInput(Class<?> baseClass, Class<?> clazz,
		TypeInformation<?> inType, TypeInformation<?> outType) {
		compareAnalyzerResultWithAnnotationsSingleInputWithKeys(baseClass, clazz, inType, outType, null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void compareAnalyzerResultWithAnnotationsSingleInputWithKeys(Class<?> baseClass, Class<?> clazz,
		TypeInformation<?> inType, TypeInformation<?> outType, String[] keys) {
		// expected
		final Set<Annotation> annotations = FunctionAnnotation.readSingleForwardAnnotations(clazz);
		SingleInputSemanticProperties expected = SemanticPropUtil.getSemanticPropsSingle(annotations, inType,
				outType);
		if (expected == null) {
			expected = new SingleInputSemanticProperties();
		}

		// actual
		final UdfAnalyzer ua = new UdfAnalyzer(baseClass, clazz, "operator", inType, null, outType, (keys == null) ? null
				: new Keys.ExpressionKeys(keys, inType), null, true);
		ua.analyze();
		final SingleInputSemanticProperties actual = (SingleInputSemanticProperties) ua.getSemanticProperties();

		assertEquals(expected.toString(), actual.toString());
	}

	public static void compareAnalyzerResultWithAnnotationsDualInput(Class<?> baseClass, Class<?> clazz,
		TypeInformation<?> in1Type, TypeInformation<?> in2Type, TypeInformation<?> outType) {
		compareAnalyzerResultWithAnnotationsDualInputWithKeys(baseClass, clazz, in1Type, in2Type, outType, null, null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void compareAnalyzerResultWithAnnotationsDualInputWithKeys(Class<?> baseClass, Class<?> clazz,
		TypeInformation<?> in1Type, TypeInformation<?> in2Type, TypeInformation<?> outType, String[] keys1, String[] keys2) {
		// expected
		final Set<Annotation> annotations = FunctionAnnotation.readDualForwardAnnotations(clazz);
		final DualInputSemanticProperties expected = SemanticPropUtil.getSemanticPropsDual(annotations, in1Type,
				in2Type, outType);

		// actual
		final UdfAnalyzer ua = new UdfAnalyzer(baseClass, clazz, "operator", in1Type, in2Type, outType, (keys1 == null) ? null
				: new Keys.ExpressionKeys(keys1, in1Type), (keys2 == null) ? null : new Keys.ExpressionKeys(
						keys2, in2Type), true);
		ua.analyze();
		final DualInputSemanticProperties actual = (DualInputSemanticProperties) ua.getSemanticProperties();

		assertEquals(expected.toString(), actual.toString());
	}

}
