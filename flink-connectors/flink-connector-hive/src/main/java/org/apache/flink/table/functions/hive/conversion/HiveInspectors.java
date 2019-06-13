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

package org.apache.flink.table.functions.hive.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * Util for any ObjectInspector related inspection and conversion of Hive data to/from Flink data.
 *
 * <p>Hive ObjectInspector is a group of flexible APIs to inspect value in different data representation,
 * and developers can extend those API as needed, so technically, object inspector supports arbitrary data type in java.
 */
@Internal
public class HiveInspectors {

	/**
	 * Get conversion for converting Flink object to Hive object from an ObjectInspector.
	 */
	public static HiveObjectConversion getConversion(ObjectInspector inspector) {
		if (inspector instanceof PrimitiveObjectInspector) {
			if (inspector instanceof JavaBooleanObjectInspector) {
				if (((JavaBooleanObjectInspector) inspector).preferWritable()) {
					return o -> new BooleanWritable((Boolean) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaStringObjectInspector) {
				if (((StringObjectInspector) inspector).preferWritable()) {
					return o -> new Text((String) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaByteObjectInspector) {
				if (((JavaByteObjectInspector) inspector).preferWritable()) {
					return o -> new ByteWritable((Byte) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaShortObjectInspector) {
				if (((JavaShortObjectInspector) inspector).preferWritable()) {
					return o -> new ShortWritable((Short) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaIntObjectInspector) {
				if (((JavaIntObjectInspector) inspector).preferWritable()) {
					return o -> new IntWritable((Integer) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaLongObjectInspector) {
				if (((JavaLongObjectInspector) inspector).preferWritable()) {
					return o -> new LongWritable((Long) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaFloatObjectInspector) {
				if (((JavaFloatObjectInspector) inspector).preferWritable()) {
					return o -> new FloatWritable((Float) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaDoubleObjectInspector) {
				if (((JavaDoubleObjectInspector) inspector).preferWritable()) {
					return o -> new DoubleWritable((Double) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			}
		}

		throw new FlinkHiveUDFException(
			String.format("Flink doesn't support convert object conversion for %s yet", inspector));
	}

	/**
	 * Converts a Hive object to Flink object with an ObjectInspector.
	 */
	public static Object toFlinkObject(ObjectInspector inspector, Object data) {
		if (data == null) {
			return null;
		}

		if (inspector instanceof VoidObjectInspector) {
			return null;
		}

		if (inspector instanceof PrimitiveObjectInspector) {
			if (inspector instanceof BooleanObjectInspector) {
				BooleanObjectInspector oi = (BooleanObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof StringObjectInspector) {
				StringObjectInspector oi = (StringObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.getPrimitiveWritableObject(data).toString() :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof ByteObjectInspector) {
				ByteObjectInspector oi = (ByteObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof ShortObjectInspector) {
				ShortObjectInspector oi = (ShortObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveWritableObject(data);
			} else if (inspector instanceof IntObjectInspector) {
				IntObjectInspector oi = (IntObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveWritableObject(data);
			} else if (inspector instanceof LongObjectInspector) {
				LongObjectInspector oi = (LongObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveWritableObject(data);
			} else if (inspector instanceof FloatObjectInspector) {
				FloatObjectInspector oi = (FloatObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveWritableObject(data);
			} else if (inspector instanceof DoubleObjectInspector) {
				DoubleObjectInspector oi = (DoubleObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveWritableObject(data);
			}

			// TODO: handle more primitive types like char, varchar, timestamp, date, decimal
		}

		// TODO: handle complex types like struct, list, and map

		throw new FlinkHiveUDFException(
			String.format("Unwrap does not support ObjectInspector '%s' yet", inspector));
	}

	public static ObjectInspector getObjectInspector(Class clazz) {
		TypeInfo typeInfo;

		if (clazz.equals(String.class) || clazz.equals(Text.class)) {

			typeInfo = TypeInfoFactory.stringTypeInfo;
		} else if (clazz.equals(Boolean.class) || clazz.equals(BooleanWritable.class)) {

			typeInfo = TypeInfoFactory.booleanTypeInfo;
		} else if (clazz.equals(Byte.class) || clazz.equals(ByteWritable.class)) {

			typeInfo = TypeInfoFactory.byteTypeInfo;
		} else if (clazz.equals(Short.class) || clazz.equals(ShortWritable.class)) {

			typeInfo = TypeInfoFactory.shortTypeInfo;
		} else if (clazz.equals(Integer.class) || clazz.equals(IntWritable.class)) {

			typeInfo = TypeInfoFactory.intTypeInfo;
		} else if (clazz.equals(Long.class) || clazz.equals(LongWritable.class)) {

			typeInfo = TypeInfoFactory.longTypeInfo;
		} else if (clazz.equals(Float.class) || clazz.equals(FloatWritable.class)) {

			typeInfo = TypeInfoFactory.floatTypeInfo;
		} else if (clazz.equals(Double.class) || clazz.equals(DoubleWritable.class)) {

			typeInfo = TypeInfoFactory.doubleTypeInfo;
		} else {
			throw new FlinkHiveUDFException(
				String.format("Class %s is not supported yet", clazz.getName()));
		}

		return getObjectInspector(typeInfo);
	}

	private static ObjectInspector getObjectInspector(TypeInfo type) {
		switch (type.getCategory()) {
			case PRIMITIVE:
				PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
				return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType);
			default:
				throw new FlinkHiveUDFException(
					String.format("TypeInfo %s is not supported yet", type));
		}
	}

	public static DataType toFlinkType(ObjectInspector inspector) {
		return HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(inspector.getTypeName()));
	}
}
