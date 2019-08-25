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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 *  Pojo Type tests.
 *
 * <p>Pojo is a bean-style class with getters, setters and empty ctor
 * OR a class with all fields public (or for every private field, there has to be a public getter/setter)
 * everything else is a generic type (that can't be used for field selection)
 */
public class PojoTypeExtractionTest {

	/**
	 * Simple test type that implements the {@link Value} interface.
	 */
	public static class MyValue implements Value {
		private static final long serialVersionUID = 8607223484689147046L;

		@Override
		public void write(DataOutputView out) throws IOException {}

		@Override
		public void read(DataInputView in) throws IOException {}
	}

	public static class HasDuplicateField extends WC {
		@SuppressWarnings("unused")
		private int count; // duplicate
	}

	@Test
	public void testDuplicateFieldException() {
		TypeInformation<?> ti = TypeExtractor.createTypeInfo(HasDuplicateField.class);
		Assert.assertTrue(ti instanceof GenericTypeInfo<?>);
	}

	// test with correct pojo types
	public static class WC { // is a pojo
		public ComplexNestedClass complex; // is a pojo
		private int count; // is a BasicType

		public WC() {
		}
		public int getCount() {
			return count;
		}
		public void setCount(int c) {
			this.count = c;
		}
	}
	public static class ComplexNestedClass { // pojo
		public static int ignoreStaticField;
		public transient int ignoreTransientField;
		public Date date; // generic type
		public Integer someNumberWithÜnicödeNäme; // BasicType
		public float someFloat; // BasicType
		public Tuple3<Long, Long, String> word; //Tuple Type with three basic types
		public Object nothing; // generic type
		public MyValue valueType;  // writableType
		public List<String> collection;
	}

	// all public test
	public static class AllPublic extends ComplexNestedClass {
		public ArrayList<String> somethingFancy; // generic type
		public FancyCollectionSubtype<Integer> fancyIds; // generic type
		public String[]	fancyArray;			 // generic type
	}

	public static class FancyCollectionSubtype<T> extends HashSet<T> {
		private static final long serialVersionUID = -3494469602638179921L;
	}

	public static class ParentSettingGenerics extends PojoWithGenerics<Integer, Long> {
		public String field3;
	}
	public static class PojoWithGenerics<T1, T2> {
		public int key;
		public T1 field1;
		public T2 field2;
	}

	public static class ComplexHierarchyTop extends ComplexHierarchy<Tuple1<String>> {}
	public static class ComplexHierarchy<T> extends PojoWithGenerics<FromTuple,T> {}

	// extends from Tuple and adds a field
	public static class FromTuple extends Tuple3<String, String, Long> {
		private static final long serialVersionUID = 1L;
		public int special;
	}

	public static class IncorrectPojo {
		private int isPrivate;
		public int getIsPrivate() {
			return isPrivate;
		}
		// setter is missing (intentional)
	}

	// correct pojo
	public static class BeanStylePojo {
		public String abc;
		private int field;
		public int getField() {
			return this.field;
		}
		public void setField(int f) {
			this.field = f;
		}
	}
	public static class WrongCtorPojo {
		public int a;
		public WrongCtorPojo(int a) {
			this.a = a;
		}
	}

	public static class PojoWithGenericFields {
		private Collection<String> users;
		private boolean favorited;

		public boolean isFavorited() {
			return favorited;
		}

		public void setFavorited(boolean favorited) {
			this.favorited = favorited;
		}

		public Collection<String> getUsers() {
			return users;
		}

		public void setUsers(Collection<String> users) {
			this.users = users;
		}
	}
	@Test
	public void testPojoWithGenericFields() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(PojoWithGenericFields.class);

		Assert.assertTrue(typeForClass instanceof PojoTypeInfo<?>);
	}


	// in this test, the location of the getters and setters is mixed across the type hierarchy.
	public static class TypedPojoGetterSetterCheck extends GenericPojoGetterSetterCheck<String> {
		public void setPackageProtected(String in) {
			this.packageProtected = in;
		}
	}
	public static class GenericPojoGetterSetterCheck<T> {
		T packageProtected;
		public T getPackageProtected() {
			return packageProtected;
		}
	}

	@Test
	public void testIncorrectPojos() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(IncorrectPojo.class);
		Assert.assertTrue(typeForClass instanceof GenericTypeInfo<?>);

		typeForClass = TypeExtractor.createTypeInfo(WrongCtorPojo.class);
		Assert.assertTrue(typeForClass instanceof GenericTypeInfo<?>);
	}

	@Test
	public void testCorrectPojos() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(BeanStylePojo.class);
		Assert.assertTrue(typeForClass instanceof PojoTypeInfo<?>);

		typeForClass = TypeExtractor.createTypeInfo(TypedPojoGetterSetterCheck.class);
		Assert.assertTrue(typeForClass instanceof PojoTypeInfo<?>);
	}

	@Test
	public void testPojoWC() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(WC.class);
		checkWCPojoAsserts(typeForClass);

		WC t = new WC();
		t.complex = new ComplexNestedClass();
		TypeInformation<?> typeForObject = TypeExtractor.getForObject(t);
		checkWCPojoAsserts(typeForObject);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void checkWCPojoAsserts(TypeInformation<?> typeInfo) {
		Assert.assertFalse(typeInfo.isBasicType());
		Assert.assertFalse(typeInfo.isTupleType());
		Assert.assertEquals(10, typeInfo.getTotalFields());
		Assert.assertTrue(typeInfo instanceof PojoTypeInfo);
		PojoTypeInfo<?> pojoType = (PojoTypeInfo<?>) typeInfo;

		List<FlatFieldDescriptor> ffd = new ArrayList<FlatFieldDescriptor>();
		String[] fields = {"count",
				"complex.date",
				"complex.collection",
				"complex.nothing",
				"complex.someFloat",
				"complex.someNumberWithÜnicödeNäme",
				"complex.valueType",
				"complex.word.f0",
				"complex.word.f1",
				"complex.word.f2"};
		int[] positions = {9,
				1,
				0,
				2,
				3,
				4,
				5,
				6,
				7,
				8};
		Assert.assertEquals(fields.length, positions.length);
		for(int i = 0; i < fields.length; i++) {
			pojoType.getFlatFields(fields[i], 0, ffd);
			Assert.assertEquals("Too many keys returned", 1, ffd.size());
			Assert.assertEquals("position of field "+fields[i]+" wrong", positions[i], ffd.get(0).getPosition());
			ffd.clear();
		}

		pojoType.getFlatFields("complex.word.*", 0, ffd);
		Assert.assertEquals(3, ffd.size());
		// check if it returns 5,6,7
		for(FlatFieldDescriptor ffdE : ffd) {
			final int pos = ffdE.getPosition();
			Assert.assertTrue(pos <= 8 );
			Assert.assertTrue(6 <= pos );
			if(pos == 6) {
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 7) {
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 8) {
				Assert.assertEquals(String.class, ffdE.getType().getTypeClass());
			}
		}
		ffd.clear();

		// scala style full tuple selection for pojos
		pojoType.getFlatFields("complex.word._", 0, ffd);
		Assert.assertEquals(3, ffd.size());
		ffd.clear();

		pojoType.getFlatFields("complex.*", 0, ffd);
		Assert.assertEquals(9, ffd.size());
		// check if it returns 0-7
		for(FlatFieldDescriptor ffdE : ffd) {
			final int pos = ffdE.getPosition();
			Assert.assertTrue(ffdE.getPosition() <= 8 );
			Assert.assertTrue(0 <= ffdE.getPosition() );

			if(pos == 0) {
				Assert.assertEquals(List.class, ffdE.getType().getTypeClass());
			}
			if(pos == 1) {
				Assert.assertEquals(Date.class, ffdE.getType().getTypeClass());
			}
			if(pos == 2) {
				Assert.assertEquals(Object.class, ffdE.getType().getTypeClass());
			}
			if(pos == 3) {
				Assert.assertEquals(Float.class, ffdE.getType().getTypeClass());
			}
			if(pos == 4) {
				Assert.assertEquals(Integer.class, ffdE.getType().getTypeClass());
			}
			if(pos == 5) {
				Assert.assertEquals(MyValue.class, ffdE.getType().getTypeClass());
			}
			if(pos == 6) {
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 7) {
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 8) {
				Assert.assertEquals(String.class, ffdE.getType().getTypeClass());
			}
			if(pos == 9) {
				Assert.assertEquals(Integer.class, ffdE.getType().getTypeClass());
			}
		}
		ffd.clear();

		pojoType.getFlatFields("*", 0, ffd);
		Assert.assertEquals(10, ffd.size());
		// check if it returns 0-8
		for(FlatFieldDescriptor ffdE : ffd) {
			Assert.assertTrue(ffdE.getPosition() <= 9 );
			Assert.assertTrue(0 <= ffdE.getPosition() );
			if(ffdE.getPosition() == 9) {
				Assert.assertEquals(Integer.class, ffdE.getType().getTypeClass());
			}
		}
		ffd.clear();

		TypeInformation<?> typeComplexNested = pojoType.getTypeAt(0); // ComplexNestedClass complex
		Assert.assertTrue(typeComplexNested instanceof PojoTypeInfo);

		Assert.assertEquals(7, typeComplexNested.getArity());
		Assert.assertEquals(9, typeComplexNested.getTotalFields());
		PojoTypeInfo<?> pojoTypeComplexNested = (PojoTypeInfo<?>) typeComplexNested;

		boolean dateSeen = false, intSeen = false, floatSeen = false,
				tupleSeen = false, objectSeen = false, writableSeen = false, collectionSeen = false;
		for(int i = 0; i < pojoTypeComplexNested.getArity(); i++) {
			PojoField field = pojoTypeComplexNested.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("date")) {
				if(dateSeen) {
					Assert.fail("already seen");
				}
				dateSeen = true;
				Assert.assertEquals(BasicTypeInfo.DATE_TYPE_INFO, field.getTypeInformation());
				Assert.assertEquals(Date.class, field.getTypeInformation().getTypeClass());
			} else if(name.equals("someNumberWithÜnicödeNäme")) {
				if(intSeen) {
					Assert.fail("already seen");
				}
				intSeen = true;
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.getTypeInformation());
				Assert.assertEquals(Integer.class, field.getTypeInformation().getTypeClass());
			} else if(name.equals("someFloat")) {
				if(floatSeen) {
					Assert.fail("already seen");
				}
				floatSeen = true;
				Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, field.getTypeInformation());
				Assert.assertEquals(Float.class, field.getTypeInformation().getTypeClass());
			} else if(name.equals("word")) {
				if(tupleSeen) {
					Assert.fail("already seen");
				}
				tupleSeen = true;
				Assert.assertTrue(field.getTypeInformation() instanceof TupleTypeInfo<?>);
				Assert.assertEquals(Tuple3.class, field.getTypeInformation().getTypeClass());
				// do some more advanced checks on the tuple
				TupleTypeInfo<?> tupleTypeFromComplexNested = (TupleTypeInfo<?>) field.getTypeInformation();
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tupleTypeFromComplexNested.getTypeAt(0));
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tupleTypeFromComplexNested.getTypeAt(1));
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tupleTypeFromComplexNested.getTypeAt(2));
			} else if(name.equals("nothing")) {
				if(objectSeen) {
					Assert.fail("already seen");
				}
				objectSeen = true;
				Assert.assertEquals(new GenericTypeInfo<Object>(Object.class), field.getTypeInformation());
				Assert.assertEquals(Object.class, field.getTypeInformation().getTypeClass());
			} else if(name.equals("valueType")) {
				if(writableSeen) {
					Assert.fail("already seen");
				}
				writableSeen = true;
				Assert.assertEquals(new ValueTypeInfo<>(MyValue.class), field.getTypeInformation());
				Assert.assertEquals(MyValue.class, field.getTypeInformation().getTypeClass());
			} else if(name.equals("collection")) {
				if(collectionSeen) {
					Assert.fail("already seen");
				}
				collectionSeen = true;
				Assert.assertEquals(new GenericTypeInfo(List.class), field.getTypeInformation());

			} else {
				Assert.fail("field "+field+" is not expected");
			}
		}
		Assert.assertTrue("Field was not present", dateSeen);
		Assert.assertTrue("Field was not present", intSeen);
		Assert.assertTrue("Field was not present", floatSeen);
		Assert.assertTrue("Field was not present", tupleSeen);
		Assert.assertTrue("Field was not present", objectSeen);
		Assert.assertTrue("Field was not present", writableSeen);
		Assert.assertTrue("Field was not present", collectionSeen);

		TypeInformation<?> typeAtOne = pojoType.getTypeAt(1); // int count
		Assert.assertTrue(typeAtOne instanceof BasicTypeInfo);

		Assert.assertEquals(typeInfo.getTypeClass(), WC.class);
		Assert.assertEquals(typeInfo.getArity(), 2);
	}

	@Test
	public void testPojoAllPublic() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(AllPublic.class);
		checkAllPublicAsserts(typeForClass);

		TypeInformation<?> typeForObject = TypeExtractor.getForObject(new AllPublic() );
		checkAllPublicAsserts(typeForObject);
	}

	private void checkAllPublicAsserts(TypeInformation<?> typeInformation) {
		Assert.assertTrue(typeInformation instanceof PojoTypeInfo);
		Assert.assertEquals(10, typeInformation.getArity());
		Assert.assertEquals(12, typeInformation.getTotalFields());
		// check if the three additional fields are identified correctly
		boolean arrayListSeen = false, multisetSeen = false, strArraySeen = false;
		PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeInformation;
		for(int i = 0; i < pojoTypeForClass.getArity(); i++) {
			PojoField field = pojoTypeForClass.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("somethingFancy")) {
				if(arrayListSeen) {
					Assert.fail("already seen");
				}
				arrayListSeen = true;
				Assert.assertTrue(field.getTypeInformation() instanceof GenericTypeInfo);
				Assert.assertEquals(ArrayList.class, field.getTypeInformation().getTypeClass());
			} else if(name.equals("fancyIds")) {
				if(multisetSeen) {
					Assert.fail("already seen");
				}
				multisetSeen = true;
				Assert.assertTrue(field.getTypeInformation() instanceof GenericTypeInfo);
				Assert.assertEquals(FancyCollectionSubtype.class, field.getTypeInformation().getTypeClass());
			} else if(name.equals("fancyArray")) {
				if(strArraySeen) {
					Assert.fail("already seen");
				}
				strArraySeen = true;
				Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, field.getTypeInformation());
				Assert.assertEquals(String[].class, field.getTypeInformation().getTypeClass());
			} else if(Arrays.asList("date", "someNumberWithÜnicödeNäme", "someFloat", "word", "nothing", "valueType", "collection").contains(name)) {
				// ignore these, they are inherited from the ComplexNestedClass
			}
			else {
				Assert.fail("field "+field+" is not expected");
			}
		}
		Assert.assertTrue("Field was not present", arrayListSeen);
		Assert.assertTrue("Field was not present", multisetSeen);
		Assert.assertTrue("Field was not present", strArraySeen);
	}

	@Test
	public void testPojoExtendingTuple() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(FromTuple.class);
		checkFromTuplePojo(typeForClass);

		FromTuple ft = new FromTuple();
		ft.f0 = ""; ft.f1 = ""; ft.f2 = 0L;
		TypeInformation<?> typeForObject = TypeExtractor.getForObject(ft);
		checkFromTuplePojo(typeForObject);
	}

	private void checkFromTuplePojo(TypeInformation<?> typeInformation) {
		Assert.assertTrue(typeInformation instanceof PojoTypeInfo<?>);
		Assert.assertEquals(4, typeInformation.getTotalFields());
		PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeInformation;
		for(int i = 0; i < pojoTypeForClass.getArity(); i++) {
			PojoField field = pojoTypeForClass.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("special")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.getTypeInformation());
			} else if(name.equals("f0") || name.equals("f1")) {
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, field.getTypeInformation());
			} else if(name.equals("f2")) {
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, field.getTypeInformation());
			} else {
				Assert.fail("unexpected field");
			}
		}
	}

	@Test
	public void testPojoWithGenerics() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(ParentSettingGenerics.class);
		Assert.assertTrue(typeForClass instanceof PojoTypeInfo<?>);
		PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeForClass;
		for(int i = 0; i < pojoTypeForClass.getArity(); i++) {
			PojoField field = pojoTypeForClass.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("field1")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("field2")) {
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("field3")) {
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("key")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.getTypeInformation());
			} else {
				Assert.fail("Unexpected field "+field);
			}
		}
	}

	/**
	 * Test if the TypeExtractor is accepting untyped generics,
	 * making them GenericTypes
	 */
	@Test
	public void testPojoWithGenericsSomeFieldsGeneric() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(PojoWithGenerics.class);
		Assert.assertTrue(typeForClass instanceof PojoTypeInfo<?>);
		PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeForClass;
		for(int i = 0; i < pojoTypeForClass.getArity(); i++) {
			PojoField field = pojoTypeForClass.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("field1")) {
				Assert.assertEquals(new GenericTypeInfo<Object>(Object.class), field.getTypeInformation());
			} else if (name.equals("field2")) {
				Assert.assertEquals(new GenericTypeInfo<Object>(Object.class), field.getTypeInformation());
			} else if (name.equals("key")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.getTypeInformation());
			} else {
				Assert.fail("Unexpected field "+field);
			}
		}
	}


	@Test
	public void testPojoWithComplexHierarchy() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(ComplexHierarchyTop.class);
		Assert.assertTrue(typeForClass instanceof PojoTypeInfo<?>);
		PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeForClass;
		for(int i = 0; i < pojoTypeForClass.getArity(); i++) {
			PojoField field = pojoTypeForClass.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("field1")) {
				Assert.assertTrue(field.getTypeInformation() instanceof PojoTypeInfo<?>); // From tuple is pojo (not tuple type!)
			} else if (name.equals("field2")) {
				Assert.assertTrue(field.getTypeInformation() instanceof TupleTypeInfo<?>);
				Assert.assertTrue( ((TupleTypeInfo<?>)field.getTypeInformation()).getTypeAt(0).equals(BasicTypeInfo.STRING_TYPE_INFO) );
			} else if (name.equals("key")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.getTypeInformation());
			} else {
				Assert.fail("Unexpected field "+field);
			}
		}
	}
	
	public static class MyMapper<T> implements MapFunction<PojoWithGenerics<Long, T>, PojoWithGenerics<T,T>> {
		private static final long serialVersionUID = 1L;

		@Override
		public PojoWithGenerics<T, T> map(PojoWithGenerics<Long, T> value)
				throws Exception {
			return null;
		}
	}

	@Test
	public void testGenericPojoTypeInference1() {
		MyMapper<String> function = new MyMapper<>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(
				function,
				TypeInformation.of(new TypeHint<PojoWithGenerics<Long, String>>(){}));
		
		Assert.assertTrue(ti instanceof PojoTypeInfo<?>);
		PojoTypeInfo<?> pti = (PojoTypeInfo<?>) ti;
		for(int i = 0; i < pti.getArity(); i++) {
			PojoField field = pti.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("field1")) {
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("field2")) {
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("key")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.getTypeInformation());
			} else {
				Assert.fail("Unexpected field "+field);
			}
		}
	}

	public static class PojoTuple<A, B, C> extends Tuple3<B, C, Long> {
		private static final long serialVersionUID = 1L;

		public A extraField;
	}

	public static class MyMapper2<D, E> implements MapFunction<Tuple2<E, D>, PojoTuple<E, D, D>> {
		private static final long serialVersionUID = 1L;

		@Override
		public PojoTuple<E, D, D> map(Tuple2<E, D> value) throws Exception {
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericPojoTypeInference2() {
		MyMapper2<Boolean, Character> function = new MyMapper2<>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(
				function,
				TypeInformation.of(new TypeHint<Tuple2<Character,Boolean>>(){}));

		Assert.assertTrue(ti instanceof PojoTypeInfo<?>);
		PojoTypeInfo<?> pti = (PojoTypeInfo<?>) ti;
		for(int i = 0; i < pti.getArity(); i++) {
			PojoField field = pti.getPojoFieldAt(i);
			String name = field.getField().getName();
			if(name.equals("extraField")) {
				Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("f0")) {
				Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("f1")) {
				Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, field.getTypeInformation());
			} else if (name.equals("f2")) {
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, field.getTypeInformation());
			} else {
				Assert.fail("Unexpected field "+field);
			}
		}
	}

	public static class MyMapper3<D, E> implements MapFunction<PojoTuple<E, D, D>, Tuple2<E, D>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<E, D> map(PojoTuple<E, D, D> value) throws Exception {
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericPojoTypeInference3() {
		MyMapper3<Boolean, Character> function = new MyMapper3<>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(
				function,
				TypeInformation.of(new TypeHint<PojoTuple<Character, Boolean, Boolean>>(){}));
		
		Assert.assertTrue(ti instanceof TupleTypeInfo<?>);
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(1));
	}

	public static class PojoWithParameterizedFields1<Z> {
		public Tuple2<Z, Z> field;
	}

	public static class MyMapper4<A> implements MapFunction<PojoWithParameterizedFields1<A>, A> {
		private static final long serialVersionUID = 1L;
		@Override
		public A map(PojoWithParameterizedFields1<A> value) throws Exception {
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericPojoTypeInference4() {
		MyMapper4<Byte> function = new MyMapper4<>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(
				function,
				TypeInformation.of(new TypeHint<PojoWithParameterizedFields1<Byte>>(){}));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, ti);
	}

	public static class PojoWithParameterizedFields2<Z> {
		public PojoWithGenerics<Z, Z> field;
	}

	public static class MyMapper5<A> implements MapFunction<PojoWithParameterizedFields2<A>, A> {
		private static final long serialVersionUID = 1L;
		@Override
		public A map(PojoWithParameterizedFields2<A> value) throws Exception {
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericPojoTypeInference5() {
		MyMapper5<Byte> function = new MyMapper5<>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(
				function,
				TypeInformation.of(new TypeHint<PojoWithParameterizedFields2<Byte>>(){}));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, ti);
	}
	
	public static class PojoWithParameterizedFields3<Z> {
		public Z[] field;
	}

	public static class MyMapper6<A> implements MapFunction<PojoWithParameterizedFields3<A>, A> {
		private static final long serialVersionUID = 1L;
		@Override
		public A map(PojoWithParameterizedFields3<A> value) throws Exception {
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericPojoTypeInference6() {
		MyMapper6<Integer> function = new MyMapper6<>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(
				function,
				TypeInformation.of(new TypeHint<PojoWithParameterizedFields3<Integer>>(){}));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ti);
	}

	public static class MyMapper7<A> implements MapFunction<PojoWithParameterizedFields4<A>, A> {
		private static final long serialVersionUID = 1L;
		@Override
		public A map(PojoWithParameterizedFields4<A> value) throws Exception {
			return null;
		}
	}

	public static class PojoWithParameterizedFields4<Z> {
		public Tuple1<Z>[] field;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericPojoTypeInference7() {
		MyMapper7<Integer> function = new MyMapper7<>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(
				function,
				TypeInformation.of(new TypeHint<PojoWithParameterizedFields4<Integer>>(){}));

		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ti);
	}

	public static class RecursivePojo1 {
		public RecursivePojo1 field;
	}

	public static class RecursivePojo2 {
		public Tuple1<RecursivePojo2> field;
	}

	public static class RecursivePojo3 {
		public NestedPojo field;
	}

	public static class NestedPojo {
		public RecursivePojo3 field;
	}

	@Test
	public void testRecursivePojo1() {
		TypeInformation<?> ti = TypeExtractor.createTypeInfo(RecursivePojo1.class);
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		Assert.assertEquals(GenericTypeInfo.class, ((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation().getClass());
	}

	@Test
	public void testRecursivePojo2() {
		TypeInformation<?> ti = TypeExtractor.createTypeInfo(RecursivePojo2.class);
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		PojoField pf = ((PojoTypeInfo) ti).getPojoFieldAt(0);
		Assert.assertTrue(pf.getTypeInformation() instanceof TupleTypeInfo);
		Assert.assertEquals(GenericTypeInfo.class, ((TupleTypeInfo) pf.getTypeInformation()).getTypeAt(0).getClass());
	}

	@Test
	public void testRecursivePojo3() {
		TypeInformation<?> ti = TypeExtractor.createTypeInfo(RecursivePojo3.class);
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		PojoField pf = ((PojoTypeInfo) ti).getPojoFieldAt(0);
		Assert.assertTrue(pf.getTypeInformation() instanceof PojoTypeInfo);
		Assert.assertEquals(GenericTypeInfo.class, ((PojoTypeInfo) pf.getTypeInformation()).getPojoFieldAt(0).getTypeInformation().getClass());
	}

	public static class FooBarPojo {
		public int foo, bar;
		public FooBarPojo() {}
	}

	public static class DuplicateMapper implements MapFunction<FooBarPojo, Tuple2<FooBarPojo, FooBarPojo>> {
		@Override
		public Tuple2<FooBarPojo, FooBarPojo> map(FooBarPojo value) throws Exception {
			return null;
		}
	}

	@Test
	public void testDualUseOfPojo() {
		MapFunction<?, ?> function = new DuplicateMapper();
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeExtractor.createTypeInfo(FooBarPojo.class));
		Assert.assertTrue(ti instanceof TupleTypeInfo);
		TupleTypeInfo<?> tti = ((TupleTypeInfo) ti);
		Assert.assertTrue(tti.getTypeAt(0) instanceof PojoTypeInfo);
		Assert.assertTrue(tti.getTypeAt(1) instanceof PojoTypeInfo);
	}

	public static class PojoWithRecursiveGenericField<K,V> {
		public PojoWithRecursiveGenericField<K,V> parent;
		public PojoWithRecursiveGenericField(){}
	}

	@Test
	public void testPojoWithRecursiveGenericField() {
		TypeInformation<?> ti = TypeExtractor.createTypeInfo(PojoWithRecursiveGenericField.class);
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		Assert.assertEquals(GenericTypeInfo.class, ((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation().getClass());
	}

	public static class MutualPojoA {
		public MutualPojoB field;
	}

	public static class MutualPojoB {
		public MutualPojoA field;
	}

	@Test
	public void testPojosWithMutualRecursion() {
		TypeInformation<?> ti = TypeExtractor.createTypeInfo(MutualPojoB.class);
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		TypeInformation<?> pti = ((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation();
		Assert.assertTrue(pti instanceof PojoTypeInfo);
		Assert.assertEquals(GenericTypeInfo.class, ((PojoTypeInfo) pti).getPojoFieldAt(0).getTypeInformation().getClass());
	}

	public static class Container<T> {
		public T field;
	}

	public static class MyType extends Container<Container<Object>> {}

	@Test
	public void testRecursivePojoWithTypeVariable() {
		TypeInformation<?> ti = TypeExtractor.createTypeInfo(MyType.class);
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		TypeInformation<?> pti = ((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation();
		Assert.assertTrue(pti instanceof PojoTypeInfo);
		Assert.assertEquals(GenericTypeInfo.class, ((PojoTypeInfo) pti).getPojoFieldAt(0).getTypeInformation().getClass());
	}

	/**
	 * POJO generated using Lombok.
	 */
	@Getter
	@Setter
	@NoArgsConstructor
	public static class TestLombok{
		private int age = 10;
		private String name;
	}

	@Test
	public void testLombokPojo() {
		TypeInformation<TestLombok> ti = TypeExtractor.getForClass(TestLombok.class);
		Assert.assertTrue(ti instanceof PojoTypeInfo);

		PojoTypeInfo<TestLombok> pti = (PojoTypeInfo<TestLombok>) ti;
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, pti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, pti.getTypeAt(1));
	}
}
