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
package eu.stratosphere.api.java.typeutils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.io.Writable;
import org.junit.Test;

import eu.stratosphere.api.java.typeutils.BasicArrayTypeInfo;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.GenericTypeInfo;
import eu.stratosphere.api.java.typeutils.ObjectArrayTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.ValueTypeInfo;
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.ByteValue;
import eu.stratosphere.types.CharValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.ListValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.MapValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.ShortValue;
import eu.stratosphere.types.StringValue;

public class TypeInfoParserTest {
	
	@Test
	public void testBasicTypes() {
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, TypeInformation.parse("Integer"));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, TypeInformation.parse("Double"));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, TypeInformation.parse("Byte"));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, TypeInformation.parse("Float"));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, TypeInformation.parse("Short"));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.parse("Long"));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, TypeInformation.parse("Character"));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.parse("String"));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeInformation.parse("Boolean"));
		
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, TypeInformation.parse("java.lang.Integer"));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, TypeInformation.parse("java.lang.Double"));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, TypeInformation.parse("java.lang.Byte"));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, TypeInformation.parse("java.lang.Float"));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, TypeInformation.parse("java.lang.Short"));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.parse("java.lang.Long"));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, TypeInformation.parse("java.lang.Character"));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.parse("java.lang.String"));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeInformation.parse("java.lang.Boolean"));
		
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, TypeInformation.parse("int"));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, TypeInformation.parse("double"));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, TypeInformation.parse("byte"));		
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, TypeInformation.parse("float"));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, TypeInformation.parse("short"));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.parse("long"));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, TypeInformation.parse("char"));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeInformation.parse("boolean"));
	}
	
	@Test
	public void testValueTypes() {
		helperValueType("StringValue", StringValue.class);
		helperValueType("IntValue", IntValue.class);
		helperValueType("ByteValue", ByteValue.class);
		helperValueType("ShortValue", ShortValue.class);
		helperValueType("CharValue", CharValue.class);
		helperValueType("DoubleValue", DoubleValue.class);
		helperValueType("FloatValue", FloatValue.class);
		helperValueType("LongValue", LongValue.class);
		helperValueType("BooleanValue", BooleanValue.class);
		helperValueType("ListValue", ListValue.class);
		helperValueType("MapValue", MapValue.class);
		helperValueType("NullValue", NullValue.class);
	}
	
	private static void helperValueType(String str, Class<?> clazz) {
		TypeInformation<?> ti = TypeInformation.parse(str);
		Assert.assertTrue(ti instanceof ValueTypeInfo);
		ValueTypeInfo<?> vti = (ValueTypeInfo<?>) ti;
		Assert.assertEquals(clazz, vti.getTypeClass());
	}
	
	@Test
	public void testBasicArrays() {
		Assert.assertEquals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, TypeInformation.parse("Integer[]"));
		Assert.assertEquals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO, TypeInformation.parse("Double[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, TypeInformation.parse("Byte[]"));
		Assert.assertEquals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO, TypeInformation.parse("Float[]"));
		Assert.assertEquals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO, TypeInformation.parse("Short[]"));
		Assert.assertEquals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO, TypeInformation.parse("Long[]"));
		Assert.assertEquals(BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO, TypeInformation.parse("Character[]"));
		Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, TypeInformation.parse("String[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO, TypeInformation.parse("Boolean[]"));
		
		Assert.assertEquals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Integer[]"));
		Assert.assertEquals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Double[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Byte[]"));
		Assert.assertEquals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Float[]"));
		Assert.assertEquals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Short[]"));
		Assert.assertEquals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Long[]"));
		Assert.assertEquals(BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Character[]"));
		Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.String[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO, TypeInformation.parse("java.lang.Boolean[]"));
		
		Assert.assertEquals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, TypeInformation.parse("int[]"));
		Assert.assertEquals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO, TypeInformation.parse("double[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, TypeInformation.parse("byte[]"));		
		Assert.assertEquals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO, TypeInformation.parse("float[]"));
		Assert.assertEquals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO, TypeInformation.parse("short[]"));
		Assert.assertEquals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO, TypeInformation.parse("long[]"));
		Assert.assertEquals(BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO, TypeInformation.parse("char[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO, TypeInformation.parse("boolean[]"));
	}
	
	@Test
	public void testTuples() {
		TypeInformation<?> ti = TypeInformation.parse("Tuple2<Integer, Long>");
		Assert.assertEquals(2, ti.getArity());
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>)ti).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>)ti).getTypeAt(1));
		
		ti = TypeInformation.parse("Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>");
		Assert.assertEquals("Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>", ti.toString());
	}
	
	@Test
	public void testCustomType() {
		TypeInformation<?> ti = TypeInformation.parse("java.lang.Class");
		Assert.assertTrue(ti instanceof GenericTypeInfo);
		Assert.assertEquals(Class.class, ((GenericTypeInfo<?>) ti).getTypeClass());
	}
	
	public static class MyWritable implements Writable {

		@Override
		public void write(DataOutput out) throws IOException {
			
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			
		}
		
	}
	
	@Test
	public void testWritableType() {
		TypeInformation<?> ti = TypeInformation.parse("Writable<eu.stratosphere.api.java.typeutils.TypeInfoParserTest$MyWritable>");
		Assert.assertTrue(ti instanceof WritableTypeInfo<?>);
		Assert.assertEquals(MyWritable.class, ((WritableTypeInfo<?>) ti).getTypeClass());
	}
	
	@Test
	public void testObjectArrays() {
		TypeInformation<?> ti = TypeInformation.parse("java.lang.Class[]");

		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?, ?>);
		Assert.assertEquals(Class.class, ((ObjectArrayTypeInfo<?, ?>) ti).getComponentType());
		
		TypeInformation<?> ti2 = TypeInformation.parse("Tuple2<Integer,Double>[]");

		Assert.assertTrue(ti2 instanceof ObjectArrayTypeInfo<?, ?>);
		Assert.assertTrue(((ObjectArrayTypeInfo<?, ?>) ti2).getComponentInfo() instanceof TupleTypeInfo);
		
		TypeInformation<?> ti3 = TypeInformation.parse("Tuple2<Integer[],Double>[]");
		Assert.assertEquals("ObjectArrayTypeInfo<Tuple2<BasicArrayTypeInfo<Integer>, Double>>", ti3.toString());
	}
	
	@Test
	public void testLargeMixedTuple() {
		TypeInformation<?> ti = TypeInformation.parse("eu.stratosphere.api.java.tuple.Tuple4<Double,java.lang.Class[],StringValue,Tuple1<int>>[]");
		Assert.assertEquals("ObjectArrayTypeInfo<Tuple4<Double, ObjectArrayTypeInfo<GenericType<java.lang.Class>>, ValueType<eu.stratosphere.types.StringValue>, Tuple1<Integer>>>", ti.toString());
	}
	
	@Test
	public void testException() {
		try {
	    	TypeInformation.parse("THIS_CLASS_DOES_NOT_EXIST");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
		
		try {
	    	TypeInformation.parse("Tuple2<Integer>");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
		
		try {
	    	TypeInformation.parse("Tuple3<Integer,,>");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
		
		try {
	    	TypeInformation.parse("Tuple1<Integer,Double>");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
	}
}
