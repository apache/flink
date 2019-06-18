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
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Timestamp;


/**
 * Util for any ObjectInspector related inspection and conversion of Hive data to/from Flink data.
 *
 * <p>Hive ObjectInspector is a group of flexible APIs to inspect value in different data representation,
 * and developers can extend those API as needed, so technically, object inspector supports arbitrary data type in java.
 */
@Internal
public class HiveInspectors {

	/**
	 * Get an array of ObjectInspector from the give array of args and their types.
	 */
	public static ObjectInspector[] toInspectors(Object[] args, DataType[] argTypes) {
		assert args.length == argTypes.length;

		ObjectInspector[] argumentInspectors = new ObjectInspector[argTypes.length];

		for (int i = 0; i < argTypes.length; i++) {
			Object constant = args[i];

			if (constant == null) {
				argumentInspectors[i] =
					TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
						HiveTypeUtil.toHiveTypeInfo(argTypes[i]));
			} else {
				argumentInspectors[i] =
					HiveInspectors.getPrimitiveJavaConstantObjectInspector(
						(PrimitiveTypeInfo) HiveTypeUtil.toHiveTypeInfo(argTypes[i]),
						constant
					);
			}
		}

		return argumentInspectors;
	}

	private static ConstantObjectInspector getPrimitiveJavaConstantObjectInspector(PrimitiveTypeInfo typeInfo, Object value) {
		switch (typeInfo.getPrimitiveCategory()) {
			case BOOLEAN:
				return new JavaConstantBooleanObjectInspector((Boolean) value);
			case BYTE:
				return new JavaConstantByteObjectInspector((Byte) value);
			case SHORT:
				return new JavaConstantShortObjectInspector((Short) value);
			case INT:
				return new JavaConstantIntObjectInspector((Integer) value);
			case LONG:
				return new JavaConstantLongObjectInspector((Long) value);
			case FLOAT:
				return new JavaConstantFloatObjectInspector((Float) value);
			case DOUBLE:
				return new JavaConstantDoubleObjectInspector((Double) value);
			case STRING:
				return new JavaConstantStringObjectInspector((String) value);
			case CHAR:
				return new JavaConstantHiveCharObjectInspector((HiveChar) value);
			case VARCHAR:
				return new JavaConstantHiveVarcharObjectInspector((HiveVarchar) value);
			case DATE:
				return new JavaConstantDateObjectInspector((Date) value);
			case TIMESTAMP:
				return new JavaConstantTimestampObjectInspector((Timestamp) value);
			case DECIMAL:
				// TODO: Needs more testing
				return new JavaConstantHiveDecimalObjectInspector((HiveDecimal) value);
			case BINARY:
				return new JavaConstantBinaryObjectInspector((byte[]) value);
			case UNKNOWN:
			case VOID:
				// If type is null, we use the Java Constant String to replace
				return new JavaConstantStringObjectInspector((String) value);
			default:
				throw new FlinkHiveUDFException(
					String.format("Cannot find ConstantObjectInspector for %s", typeInfo));
		}
	}

	/**
	 * Get conversion for converting Flink object to Hive object from an ObjectInspector and the corresponding Flink DataType.
	 */
	public static HiveObjectConversion getConversion(ObjectInspector inspector, DataType dataType) {
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
			} else if (inspector instanceof JavaDateObjectInspector) {
				if (((JavaDateObjectInspector) inspector).preferWritable()) {
					return o -> new DateWritable((Date) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaTimestampObjectInspector) {
				if (((JavaTimestampObjectInspector) inspector).preferWritable()) {
					return o -> new TimestampWritable((Timestamp) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaBinaryObjectInspector) {
				if (((JavaBinaryObjectInspector) inspector).preferWritable()) {
					return o -> new BytesWritable((byte[]) o);
				} else {
					return IdentityConversion.INSTANCE;
				}
			} else if (inspector instanceof JavaHiveCharObjectInspector) {
				if (((JavaHiveCharObjectInspector) inspector).preferWritable()) {
					return o -> new HiveCharWritable(
						new HiveChar((String) o, ((CharType) dataType.getLogicalType()).getLength()));
				} else {
					return o -> new HiveChar((String) o, ((CharType) dataType.getLogicalType()).getLength());
				}
			} else if (inspector instanceof JavaHiveVarcharObjectInspector) {
				if (((JavaHiveVarcharObjectInspector) inspector).preferWritable()) {
					return o -> new HiveVarcharWritable(
						new HiveVarchar((String) o, ((VarCharType) dataType.getLogicalType()).getLength()));
				} else {
					return o -> new HiveVarchar((String) o, ((VarCharType) dataType.getLogicalType()).getLength());
				}
			}

			// TODO: handle decimal type
		}

		// TODO: handle complex types like struct, list, and map

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
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof IntObjectInspector) {
				IntObjectInspector oi = (IntObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof LongObjectInspector) {
				LongObjectInspector oi = (LongObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof FloatObjectInspector) {
				FloatObjectInspector oi = (FloatObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof DoubleObjectInspector) {
				DoubleObjectInspector oi = (DoubleObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.get(data) :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof DateObjectInspector) {
				DateObjectInspector oi = (DateObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.getPrimitiveWritableObject(data).get() :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof TimestampObjectInspector) {
				TimestampObjectInspector oi = (TimestampObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.getPrimitiveWritableObject(data).getTimestamp() :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof BinaryObjectInspector) {
				BinaryObjectInspector oi = (BinaryObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.getPrimitiveWritableObject(data).getBytes() :
					oi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof HiveCharObjectInspector) {
				HiveCharObjectInspector oi = (HiveCharObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.getPrimitiveWritableObject(data).getHiveChar().getValue() :
					oi.getPrimitiveJavaObject(data).getValue();
			} else if (inspector instanceof HiveVarcharObjectInspector) {
				HiveVarcharObjectInspector oi = (HiveVarcharObjectInspector) inspector;

				return oi.preferWritable() ?
					oi.getPrimitiveWritableObject(data).getHiveVarchar().getValue() :
					oi.getPrimitiveJavaObject(data).getValue();
			}

			// TODO: handle decimal type
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
		} else if (clazz.equals(Date.class) || clazz.equals(DateWritable.class)) {

			typeInfo = TypeInfoFactory.dateTypeInfo;
		} else if (clazz.equals(Timestamp.class) || clazz.equals(TimestampWritable.class)) {

			typeInfo = TypeInfoFactory.timestampTypeInfo;
		} else if (clazz.equals(byte[].class) || clazz.equals(BytesWritable.class)) {

			typeInfo = TypeInfoFactory.binaryTypeInfo;
		} else if (clazz.equals(HiveChar.class)) {

			typeInfo = TypeInfoFactory.charTypeInfo;
		} else if (clazz.equals(HiveVarchar.class)) {

			typeInfo = TypeInfoFactory.varcharTypeInfo;
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
