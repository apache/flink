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
package org.apache.flink.api.java.type.extractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParserTest.MyWritable;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.HashMultiset;

/**
 *  Pojo Type tests
 *  
 *  A Pojo is a bean-style class with getters, setters and empty ctor
 *   OR a class with all fields public (or for every private field, there has to be a public getter/setter)
 *   everything else is a generic type (that can't be used for field selection)
 */
public class PojoTypeExtractionTest {

	public static class HasDuplicateField extends WC {
		private int count; // duplicate
	}
	
	@Test(expected=RuntimeException.class)
	public void testDuplicateFieldException() {
		TypeExtractor.createTypeInfo(HasDuplicateField.class);
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
		public Integer someNumber; // BasicType
		public float someFloat; // BasicType
		public Tuple3<Long, Long, String> word; //Tuple Type with three basic types
		public Object nothing; // generic type
		public MyWritable hadoopCitizen;  // writableType
	}

	// all public test
	public static class AllPublic extends ComplexNestedClass {
		public ArrayList<String> somethingFancy; // generic type
		public HashMultiset<Integer> fancyIds; // generic type
		public String[]	fancyArray;			 // generic type
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
	
	private void checkWCPojoAsserts(TypeInformation<?> typeInfo) {
		Assert.assertFalse(typeInfo.isBasicType());
		Assert.assertFalse(typeInfo.isTupleType());
		Assert.assertEquals(9, typeInfo.getTotalFields());
		Assert.assertTrue(typeInfo instanceof PojoTypeInfo);
		PojoTypeInfo<?> pojoType = (PojoTypeInfo<?>) typeInfo;
		
		List<FlatFieldDescriptor> ffd = new ArrayList<FlatFieldDescriptor>();
		String[] fields = {"count","complex.date", "complex.hadoopCitizen", "complex.nothing",
				"complex.someFloat", "complex.someNumber", "complex.word.f0",
				"complex.word.f1", "complex.word.f2"};
		int[] positions = {8,0,1,2,
				3,4,5,
				6,7};
		Assert.assertEquals(fields.length, positions.length);
		for(int i = 0; i < fields.length; i++) {
			pojoType.getKey(fields[i], 0, ffd);
			Assert.assertEquals("Too many keys returned", 1, ffd.size());
			Assert.assertEquals("position of field "+fields[i]+" wrong", positions[i], ffd.get(0).getPosition());
			ffd.clear();
		}
		
		pojoType.getKey("complex.word.*", 0, ffd);
		Assert.assertEquals(3, ffd.size());
		// check if it returns 5,6,7
		for(FlatFieldDescriptor ffdE : ffd) {
			final int pos = ffdE.getPosition();
			Assert.assertTrue(pos <= 7 );
			Assert.assertTrue(5 <= pos );
			if(pos == 5) {
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 6) {
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 7) {
				Assert.assertEquals(String.class, ffdE.getType().getTypeClass());
			}
		}
		ffd.clear();
		
		// scala style full tuple selection for pojos
		pojoType.getKey("complex.word._", 0, ffd);
		Assert.assertEquals(3, ffd.size());
		ffd.clear();
		
		pojoType.getKey("complex.*", 0, ffd);
		Assert.assertEquals(8, ffd.size());
		// check if it returns 0-7
		for(FlatFieldDescriptor ffdE : ffd) {
			final int pos = ffdE.getPosition();
			Assert.assertTrue(ffdE.getPosition() <= 7 );
			Assert.assertTrue(0 <= ffdE.getPosition() );
			if(pos == 0) {
				Assert.assertEquals(Date.class, ffdE.getType().getTypeClass());
			}
			if(pos == 1) {
				Assert.assertEquals(MyWritable.class, ffdE.getType().getTypeClass());
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
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 6) {
				Assert.assertEquals(Long.class, ffdE.getType().getTypeClass());
			}
			if(pos == 7) {
				Assert.assertEquals(String.class, ffdE.getType().getTypeClass());
			}
		}
		ffd.clear();
		
		pojoType.getKey("*", 0, ffd);
		Assert.assertEquals(9, ffd.size());
		// check if it returns 0-8
		for(FlatFieldDescriptor ffdE : ffd) {
			Assert.assertTrue(ffdE.getPosition() <= 8 );
			Assert.assertTrue(0 <= ffdE.getPosition() );
			if(ffdE.getPosition() == 8) {
				Assert.assertEquals(Integer.class, ffdE.getType().getTypeClass());
			}
		}
		ffd.clear();
		
		TypeInformation<?> typeComplexNested = pojoType.getTypeAt(0); // ComplexNestedClass complex
		Assert.assertTrue(typeComplexNested instanceof PojoTypeInfo);
		
		Assert.assertEquals(6, typeComplexNested.getArity());
		Assert.assertEquals(8, typeComplexNested.getTotalFields());
		PojoTypeInfo<?> pojoTypeComplexNested = (PojoTypeInfo<?>) typeComplexNested;
		
		boolean dateSeen = false, intSeen = false, floatSeen = false,
				tupleSeen = false, objectSeen = false, writableSeen = false;
		for(int i = 0; i < pojoTypeComplexNested.getArity(); i++) {
			PojoField field = pojoTypeComplexNested.getPojoFieldAt(i);
			String name = field.field.getName();
			if(name.equals("date")) {
				if(dateSeen) {
					Assert.fail("already seen");
				}
				dateSeen = true;
				Assert.assertEquals(new GenericTypeInfo<Date>(Date.class), field.type);
				Assert.assertEquals(Date.class, field.type.getTypeClass());
			} else if(name.equals("someNumber")) {
				if(intSeen) {
					Assert.fail("already seen");
				}
				intSeen = true;
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.type);
				Assert.assertEquals(Integer.class, field.type.getTypeClass());
			} else if(name.equals("someFloat")) {
				if(floatSeen) {
					Assert.fail("already seen");
				}
				floatSeen = true;
				Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, field.type);
				Assert.assertEquals(Float.class, field.type.getTypeClass());
			} else if(name.equals("word")) {
				if(tupleSeen) {
					Assert.fail("already seen");
				}
				tupleSeen = true;
				Assert.assertTrue(field.type instanceof TupleTypeInfo<?>);
				Assert.assertEquals(Tuple3.class, field.type.getTypeClass());
				// do some more advanced checks on the tuple
				TupleTypeInfo<?> tupleTypeFromComplexNested = (TupleTypeInfo<?>) field.type;
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tupleTypeFromComplexNested.getTypeAt(0));
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tupleTypeFromComplexNested.getTypeAt(1));
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tupleTypeFromComplexNested.getTypeAt(2));
			} else if(name.equals("nothing")) {
				if(objectSeen) {
					Assert.fail("already seen");
				}
				objectSeen = true;
				Assert.assertEquals(new GenericTypeInfo<Object>(Object.class), field.type);
				Assert.assertEquals(Object.class, field.type.getTypeClass());
			} else if(name.equals("hadoopCitizen")) {
				if(writableSeen) {
					Assert.fail("already seen");
				}
				writableSeen = true;
				Assert.assertEquals(new WritableTypeInfo<MyWritable>(MyWritable.class), field.type);
				Assert.assertEquals(MyWritable.class, field.type.getTypeClass());
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
		
		TypeInformation<?> typeAtOne = pojoType.getTypeAt(1); // int count
		Assert.assertTrue(typeAtOne instanceof BasicTypeInfo);
		
		Assert.assertEquals(typeInfo.getTypeClass(), WC.class);
		Assert.assertEquals(typeInfo.getArity(), 2);
	}

	// Kryo is required for this, so disable for now.
	@Ignore
	@Test
	public void testPojoAllPublic() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(AllPublic.class);
		checkAllPublicAsserts(typeForClass);
		
		TypeInformation<?> typeForObject = TypeExtractor.getForObject(new AllPublic() );
		checkAllPublicAsserts(typeForObject);
	}
	
	private void checkAllPublicAsserts(TypeInformation<?> typeInformation) {
		Assert.assertTrue(typeInformation instanceof PojoTypeInfo);
		Assert.assertEquals(9, typeInformation.getArity());
		Assert.assertEquals(11, typeInformation.getTotalFields());
		// check if the three additional fields are identified correctly
		boolean arrayListSeen = false, multisetSeen = false, strArraySeen = false;
		PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeInformation;
		for(int i = 0; i < pojoTypeForClass.getArity(); i++) {
			PojoField field = pojoTypeForClass.getPojoFieldAt(i);
			String name = field.field.getName();
			if(name.equals("somethingFancy")) {
				if(arrayListSeen) {
					Assert.fail("already seen");
				}
				arrayListSeen = true;
				Assert.assertTrue(field.type instanceof GenericTypeInfo);
				Assert.assertEquals(ArrayList.class, field.type.getTypeClass());
			} else if(name.equals("fancyIds")) {
				if(multisetSeen) {
					Assert.fail("already seen");
				}
				multisetSeen = true;
				Assert.assertTrue(field.type instanceof GenericTypeInfo);
				Assert.assertEquals(HashMultiset.class, field.type.getTypeClass());
			} else if(name.equals("fancyArray")) {
				if(strArraySeen) {
					Assert.fail("already seen");
				}
				strArraySeen = true;
				Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, field.type);
				Assert.assertEquals(String[].class, field.type.getTypeClass());
			} else if(Arrays.asList("date", "someNumber", "someFloat", "word", "nothing", "hadoopCitizen").contains(name)) {
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
			String name = field.field.getName();
			if(name.equals("special")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.type);
			} else if(name.equals("f0") || name.equals("f1")) {
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, field.type);
			} else if(name.equals("f2")) {
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, field.type);
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
			String name = field.field.getName();
			if(name.equals("field1")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.type);
			} else if (name.equals("field2")) {
				Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, field.type);
			} else if (name.equals("field3")) {
				Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, field.type);
			} else if (name.equals("key")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.type);
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
	@Ignore // kryo needed.
	public void testPojoWithGenericsSomeFieldsGeneric() {
		TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(PojoWithGenerics.class);
		Assert.assertTrue(typeForClass instanceof PojoTypeInfo<?>);
		PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeForClass;
		for(int i = 0; i < pojoTypeForClass.getArity(); i++) {
			PojoField field = pojoTypeForClass.getPojoFieldAt(i);
			String name = field.field.getName();
			if(name.equals("field1")) {
				Assert.assertEquals(new GenericTypeInfo<Object>(Object.class), field.type);
			} else if (name.equals("field2")) {
				Assert.assertEquals(new GenericTypeInfo<Object>(Object.class), field.type);
			} else if (name.equals("key")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.type);
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
			String name = field.field.getName();
			if(name.equals("field1")) {
				Assert.assertTrue(field.type instanceof PojoTypeInfo<?>); // From tuple is pojo (not tuple type!)
			} else if (name.equals("field2")) {
				Assert.assertTrue(field.type instanceof TupleTypeInfo<?>);
				Assert.assertTrue( ((TupleTypeInfo<?>)field.type).getTypeAt(0).equals(BasicTypeInfo.STRING_TYPE_INFO) );
			} else if (name.equals("key")) {
				Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, field.type);
			} else {
				Assert.fail("Unexpected field "+field);
			}
		}
	}


	public static class Vertex<K, V> {

		private K key1;
		private K key2;
		private V value;

		public Vertex() {}

		public Vertex(K key, V value) {
			this.key1 = key;
			this.key2 = key;
			this.value = value;
		}

		public Vertex(K key1, K key2, V value) {
			this.key1 = key1;
			this.key2 = key2;
			this.value = value;
		}

		public void setKey1(K key1) {
			this.key1 = key1;
		}

		public void setKey2(K key2) {
			this.key2 = key2;
		}

		public K getKey1() {
			return key1;
		}

		public K getKey2() {
			return key2;
		}

		public void setValue(V value) {
			this.value = value;
		}

		public V getValue() {
			return value;
		}
	}

	public static class VertexTyped extends Vertex<Long, Double>{
		public VertexTyped(Long l, Double d) {
			super(l, d);
		}
		public VertexTyped() {
		}
	}
	
	@Test
	public void testGetterSetterWithVertex() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<VertexTyped> set = env.fromElements(new VertexTyped(0L, 3.0), new VertexTyped(1L, 1.0));
	}
}
