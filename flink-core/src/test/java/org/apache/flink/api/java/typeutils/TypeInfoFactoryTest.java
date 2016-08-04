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

package org.apache.flink.api.java.typeutils;

import java.lang.reflect.Type;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapFunction;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Tests for extracting {@link org.apache.flink.api.common.typeinfo.TypeInformation} from types
 * using a {@link org.apache.flink.api.common.typeinfo.TypeInfoFactory}
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TypeInfoFactoryTest {

	@Test
	public void testSimpleType() {
		TypeInformation ti = TypeExtractor.createTypeInfo(IntLike.class);
		assertEquals(INT_TYPE_INFO, ti);

		ti = TypeExtractor.getForClass(IntLike.class);
		assertEquals(INT_TYPE_INFO, ti);

		ti = TypeExtractor.getForObject(new IntLike());
		assertEquals(INT_TYPE_INFO, ti);
	}

	@Test
	public void testMyEitherGenericType() {
		MapFunction f = new MyEitherMapper();
		TypeInformation ti = TypeExtractor.getMapReturnTypes(f, (TypeInformation) BOOLEAN_TYPE_INFO);
		assertTrue(ti instanceof EitherTypeInfo);
		EitherTypeInfo eti = (EitherTypeInfo) ti;
		assertEquals(BOOLEAN_TYPE_INFO, eti.getLeftType());
		assertEquals(STRING_TYPE_INFO, eti.getRightType());
	}

	@Test
	public void testMyOptionGenericType() {
		TypeInformation inTypeInfo = new MyOptionTypeInfo(new TupleTypeInfo(BOOLEAN_TYPE_INFO, STRING_TYPE_INFO));
		MapFunction f = new MyOptionMapper();
		TypeInformation ti = TypeExtractor.getMapReturnTypes(f, inTypeInfo);
		assertTrue(ti instanceof MyOptionTypeInfo);
		MyOptionTypeInfo oti = (MyOptionTypeInfo) ti;
		assertTrue(oti.getInnerType() instanceof TupleTypeInfo);
		TupleTypeInfo tti = (TupleTypeInfo) oti.getInnerType();
		assertEquals(BOOLEAN_TYPE_INFO, tti.getTypeAt(0));
		assertEquals(BOOLEAN_TYPE_INFO, tti.getTypeAt(1));
	}

	@Test
	public void testMyTuple2() {
		TypeInformation inTypeInfo = new TupleTypeInfo(new MyTupleTypeInfo(DOUBLE_TYPE_INFO, STRING_TYPE_INFO));
		MapFunction f = new MyTupleMapperL2();
		TypeInformation ti = TypeExtractor.getMapReturnTypes(f, inTypeInfo);
		assertTrue(ti instanceof TupleTypeInfo);
		TupleTypeInfo tti = (TupleTypeInfo) ti;
		assertTrue(tti.getTypeAt(0) instanceof MyTupleTypeInfo);
		MyTupleTypeInfo mtti = (MyTupleTypeInfo) tti.getTypeAt(0);
		assertEquals(BOOLEAN_TYPE_INFO, mtti.getField0());
		assertEquals(DOUBLE_TYPE_INFO, mtti.getField1());
	}

	@Test
	public void testMyTupleHierarchy() {
		TypeInformation ti = TypeExtractor.createTypeInfo(MyTuple2.class);
		assertTrue(ti instanceof MyTupleTypeInfo);
		MyTupleTypeInfo mtti = (MyTupleTypeInfo) ti;
		assertEquals(STRING_TYPE_INFO, mtti.getField0());
		assertEquals(BOOLEAN_TYPE_INFO, mtti.getField1());
	}

	@Test
	public void testMyTupleHierarchyWithInference() {
		TypeInformation inTypeInfo = new TupleTypeInfo(new MyTupleTypeInfo(new TupleTypeInfo(FLOAT_TYPE_INFO), BOOLEAN_TYPE_INFO));
		MapFunction f = new MyTuple3Mapper();
		TypeInformation ti = TypeExtractor.getMapReturnTypes(f, inTypeInfo);
		assertTrue(ti instanceof TupleTypeInfo);
		TupleTypeInfo tti = (TupleTypeInfo) ti;
		assertTrue(tti.getTypeAt(0) instanceof MyTupleTypeInfo);
		MyTupleTypeInfo mtti = (MyTupleTypeInfo) tti.getTypeAt(0);
		assertEquals(new TupleTypeInfo(FLOAT_TYPE_INFO, STRING_TYPE_INFO), mtti.getField0());
		assertEquals(BOOLEAN_TYPE_INFO, mtti.getField1());
	}

	@Test(expected = InvalidTypesException.class)
	public void testMissingTypeInfo() {
		MapFunction f = new MyFaultyMapper();
		TypeExtractor.getMapReturnTypes(f, INT_TYPE_INFO);
	}

	@Test(expected = InvalidTypesException.class)
	public void testMissingTypeInference() {
		MapFunction f = new MyFaultyMapper2();
		TypeExtractor.getMapReturnTypes(f, new MyFaultyTypeInfo());
	}

	@Test
	public void testRegisteredFactories() {
		TypeExtractor.registerFactory(Integer.class, MyOptionTypeInfoFactory.class);
		TypeInformation ti = TypeExtractor.createTypeInfo(Integer.class);
		assertTrue(ti instanceof MyOptionTypeInfo);

		TypeExtractor.unregisterFactory(Integer.class);
		ti = TypeExtractor.createTypeInfo(Integer.class);
		assertEquals(INT_TYPE_INFO, ti);

		TypeExtractor.registerFactory(Integer.class, MyOptionTypeInfoFactory.class);
		ti = TypeExtractor.createTypeInfo(Integer.class);
		assertTrue(ti instanceof MyOptionTypeInfo);
	}

	@Test
	public void testRegisteredFactoriesHierarchy() {
		TypeExtractor.registerFactory(MyTuple.class, MyTupleTypeInfoFactory.class);
		TypeInformation ti = TypeExtractor.createTypeInfo(MyTuple3.class);
		assertTrue(ti instanceof MyTupleTypeInfo);
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	public static class MyTuple3Mapper<Y> implements MapFunction<Tuple1<MyTuple3<Tuple1<Y>>>, Tuple1<MyTuple3<Tuple2<Y, String>>>> {
		@Override
		public Tuple1<MyTuple3<Tuple2<Y, String>>> map(Tuple1<MyTuple3<Tuple1<Y>>> value) throws Exception {
			return null;
		}
	}

	public static class MyTuple3<T> extends MyTuple<T, Boolean> {
		// empty
	}

	public static class MyTuple2 extends MyTuple<String, Boolean> {
		// empty
	}

	public static class MyFaultyMapper2<T> implements MapFunction<MyFaulty<T>, MyFaulty<T>> {
		@Override
		public MyFaulty<T> map(MyFaulty<T> value) throws Exception {
			return null;
		}
	}

	public static class MyFaultyMapper<T> implements MapFunction<T, MyFaulty<T>> {
		@Override
		public MyFaulty<T> map(T value) throws Exception {
			return null;
		}
	}

	@TypeInfo(FaultyTypeInfoFactory.class)
	public static class MyFaulty<Y> {
		// empty
	}

	public static class FaultyTypeInfoFactory extends TypeInfoFactory {
		@Override
		public TypeInformation createTypeInfo(Type t, Map genericParameters) {
			return null;
		}
	}

	public static class MyFaultyTypeInfo extends TypeInformation<MyFaulty> {
		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 0;
		}

		@Override
		public int getTotalFields() {
			return 0;
		}

		@Override
		public Class<MyFaulty> getTypeClass() {
			return null;
		}

		@Override
		public boolean isKeyType() {
			return false;
		}

		@Override
		public TypeSerializer<MyFaulty> createSerializer(ExecutionConfig config) {
			return null;
		}

		@Override
		public String toString() {
			return null;
		}

		@Override
		public boolean equals(Object obj) {
			return false;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean canEqual(Object obj) {
			return false;
		}
	}

	public static class MyTupleMapperL1<A, B> implements MapFunction<Tuple1<MyTuple<A, String>>, Tuple1<MyTuple<B, A>>> {
		@Override
		public Tuple1<MyTuple<B, A>> map(Tuple1<MyTuple<A, String>> value) throws Exception {
			return null;
		}
	}

	public static class MyTupleMapperL2<C> extends MyTupleMapperL1<C, Boolean> {
		// empty
	}

	@TypeInfo(MyTupleTypeInfoFactory.class)
	public static class MyTuple<T0, T1> {
		// empty
	}

	public static class MyTupleTypeInfoFactory extends TypeInfoFactory<MyTuple> {
		@Override
		public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
			return new MyTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
		}

		@Override
		public TypeInformation<?> mapSubtypeInfo(String genericParameter, TypeInformation<MyTuple> typeInfo) {
			switch (genericParameter) {
				case "T0": return ((MyTupleTypeInfo) typeInfo).field0;
				case "T1": return ((MyTupleTypeInfo) typeInfo).field1;
			}
			return null;
		}
	}

	public static class MyTupleTypeInfo<T0, T1> extends TypeInformation<MyTuple<T0, T1>> {
		private TypeInformation field0;
		private TypeInformation field1;

		public TypeInformation getField0() {
			return field0;
		}

		public TypeInformation getField1() {
			return field1;
		}

		public MyTupleTypeInfo(TypeInformation field0, TypeInformation field1) {
			this.field0 = field0;
			this.field1 = field1;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 0;
		}

		@Override
		public int getTotalFields() {
			return 0;
		}

		@Override
		public Class<MyTuple<T0, T1>> getTypeClass() {
			return null;
		}

		@Override
		public boolean isKeyType() {
			return false;
		}

		@Override
		public TypeSerializer<MyTuple<T0, T1>> createSerializer(ExecutionConfig config) {
			return null;
		}

		@Override
		public String toString() {
			return null;
		}

		@Override
		public boolean equals(Object obj) {
			return false;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean canEqual(Object obj) {
			return false;
		}
	}

	public static class MyOptionMapper<T> implements MapFunction<MyOption<Tuple2<T, String>>, MyOption<Tuple2<T, T>>> {
		@Override
		public MyOption<Tuple2<T, T>> map(MyOption<Tuple2<T, String>> value) throws Exception {
			return null;
		}
	}

	@TypeInfo(MyOptionTypeInfoFactory.class)
	public static class MyOption<T> {
		// empty
	}

	public static class MyOptionTypeInfoFactory<T> extends TypeInfoFactory<MyOption<T>> {
		@Override
		public TypeInformation<MyOption<T>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParams) {
			return new MyOptionTypeInfo(genericParams.get("T"));
		}

		@Override
		public TypeInformation<?> mapSubtypeInfo(String genericParameter, TypeInformation<MyOption<T>> typeInfo) {
			return ((MyOptionTypeInfo<T>) typeInfo).getInnerType();
		}
	}

	public static class MyOptionTypeInfo<T> extends TypeInformation<MyOption<T>> {

		private final TypeInformation<T> innerType;

		public MyOptionTypeInfo(TypeInformation<T> innerType) {
			this.innerType = innerType;
		}

		public TypeInformation<T> getInnerType() {
			return innerType;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 0;
		}

		@Override
		public int getTotalFields() {
			return 0;
		}

		@Override
		public Class<MyOption<T>> getTypeClass() {
			return null;
		}

		@Override
		public boolean isKeyType() {
			return false;
		}

		@Override
		public TypeSerializer<MyOption<T>> createSerializer(ExecutionConfig config) {
			return null;
		}

		@Override
		public String toString() {
			return null;
		}

		@Override
		public boolean equals(Object obj) {
			return false;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean canEqual(Object obj) {
			return false;
		}
	}

	public static class MyEitherMapper<T> implements MapFunction<T, MyEither<T, String>> {
		@Override
		public MyEither<T, String> map(T value) throws Exception {
			return null;
		}
	}

	@TypeInfo(MyEitherTypeInfoFactory.class)
	public static class MyEither<A, B> {
		// empty
	}

	public static class MyEitherTypeInfoFactory<A, B> extends TypeInfoFactory<MyEither<A, B>> {
		@Override
		public TypeInformation createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParams) {
			return new EitherTypeInfo<>(genericParams.get("A"), genericParams.get("B"));
		}
	}

	@TypeInfo(IntLikeTypeInfoFactory.class)
	public static class IntLike {
		// empty
	}

	public static class IntLikeTypeInfoFactory extends TypeInfoFactory<IntLike> {
		@Override
		public TypeInformation createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParams) {
			return INT_TYPE_INFO;
		}
	}

}
