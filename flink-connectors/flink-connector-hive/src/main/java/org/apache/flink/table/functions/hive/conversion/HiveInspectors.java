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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
	public static HiveObjectConversion getConversion(ObjectInspector inspector, LogicalType dataType) {
		if (inspector instanceof PrimitiveObjectInspector) {
			if (inspector instanceof BooleanObjectInspector ||
					inspector instanceof StringObjectInspector ||
					inspector instanceof ByteObjectInspector ||
					inspector instanceof ShortObjectInspector ||
					inspector instanceof IntObjectInspector ||
					inspector instanceof LongObjectInspector ||
					inspector instanceof FloatObjectInspector ||
					inspector instanceof DoubleObjectInspector ||
					inspector instanceof DateObjectInspector ||
					inspector instanceof TimestampObjectInspector ||
					inspector instanceof BinaryObjectInspector) {
				return IdentityConversion.INSTANCE;
			} else if (inspector instanceof HiveCharObjectInspector) {
				return o -> new HiveChar((String) o, ((CharType) dataType).getLength());
			} else if (inspector instanceof HiveVarcharObjectInspector) {
				return o -> new HiveVarchar((String) o, ((VarCharType) dataType).getLength());
			} else if (inspector instanceof HiveDecimalObjectInspector) {
				return o -> o == null ? null : HiveDecimal.create((BigDecimal) o);
			}
		}

		if (inspector instanceof ListObjectInspector) {
			HiveObjectConversion eleConvert = getConversion(
				((ListObjectInspector) inspector).getListElementObjectInspector(),
				((ArrayType) dataType).getElementType());
			return o -> {
				Object[] array = (Object[]) o;
				List<Object> result = new ArrayList<>();

				for (Object ele : array) {
					result.add(eleConvert.toHiveObject(ele));
				}
				return result;
			};
		}

		if (inspector instanceof MapObjectInspector) {
			MapObjectInspector mapInspector = (MapObjectInspector) inspector;
			MapType kvType = (MapType) dataType;

			HiveObjectConversion keyConversion =
				getConversion(mapInspector.getMapKeyObjectInspector(), kvType.getKeyType());
			HiveObjectConversion valueConversion =
				getConversion(mapInspector.getMapValueObjectInspector(), kvType.getValueType());

			return o -> {
				Map<Object, Object> map = (Map) o;
				Map<Object, Object> result = new HashMap<>(map.size());

				for (Map.Entry<Object, Object> entry : map.entrySet()){
					result.put(
						keyConversion.toHiveObject(entry.getKey()),
						valueConversion.toHiveObject(entry.getValue()));
				}
				return result;
			};
		}

		if (inspector instanceof StructObjectInspector) {
			StructObjectInspector structInspector = (StructObjectInspector) inspector;

			List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();

			List<RowType.RowField> rowFields = ((RowType) dataType).getFields();

			HiveObjectConversion[] conversions = new HiveObjectConversion[structFields.size()];
			for (int i = 0; i < structFields.size(); i++) {
				conversions[i] = getConversion(structFields.get(i).getFieldObjectInspector(), rowFields.get(i).getType());
			}

			return o -> {
				Row row = (Row) o;
				List<Object> result = new ArrayList<>(row.getArity());
				for (int i = 0; i < row.getArity(); i++) {
					result.add(conversions[i].toHiveObject(row.getField(i)));
				}
				return result;
			};
		}

		throw new FlinkHiveUDFException(
			String.format("Flink doesn't support convert object conversion for %s yet", inspector));
	}

	/**
	 * Converts a Hive object to Flink object with an ObjectInspector.
	 */
	public static Object toFlinkObject(ObjectInspector inspector, Object data) {
		if (data == null || inspector instanceof VoidObjectInspector) {
			return null;
		}

		if (inspector instanceof PrimitiveObjectInspector) {
			if (inspector instanceof BooleanObjectInspector ||
					inspector instanceof StringObjectInspector ||
					inspector instanceof ByteObjectInspector ||
					inspector instanceof ShortObjectInspector ||
					inspector instanceof IntObjectInspector ||
					inspector instanceof LongObjectInspector ||
					inspector instanceof FloatObjectInspector ||
					inspector instanceof DoubleObjectInspector ||
					inspector instanceof DateObjectInspector ||
					inspector instanceof TimestampObjectInspector ||
					inspector instanceof BinaryObjectInspector) {

				PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
				return poi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof HiveCharObjectInspector) {
				HiveCharObjectInspector oi = (HiveCharObjectInspector) inspector;

				return oi.getPrimitiveJavaObject(data).getValue();
			} else if (inspector instanceof HiveVarcharObjectInspector) {
				HiveVarcharObjectInspector oi = (HiveVarcharObjectInspector) inspector;

				return oi.getPrimitiveJavaObject(data).getValue();
			} else if (inspector instanceof HiveDecimalObjectInspector) {
				HiveDecimalObjectInspector oi = (HiveDecimalObjectInspector) inspector;

				return oi.getPrimitiveJavaObject(data).bigDecimalValue();
			}
		}

		if (inspector instanceof ListObjectInspector) {
			ListObjectInspector listInspector = (ListObjectInspector) inspector;
			List<?> list = listInspector.getList(data);

			Object[] result = new Object[list.size()];
			for (int i = 0; i < list.size(); i++) {
				result[i] = toFlinkObject(listInspector.getListElementObjectInspector(), list.get(i));
			}
			return result;
		}

		if (inspector instanceof MapObjectInspector) {
			MapObjectInspector mapInspector = (MapObjectInspector) inspector;
			Map<?, ?> map = mapInspector.getMap(data);

			Map<Object, Object> result = new HashMap<>(map.size());
			for (Map.Entry<?, ?> entry : map.entrySet()){
				result.put(
					toFlinkObject(mapInspector.getMapKeyObjectInspector(), entry.getKey()),
					toFlinkObject(mapInspector.getMapValueObjectInspector(), entry.getValue()));
			}
			return result;
		}

		if (inspector instanceof StructObjectInspector) {
			StructObjectInspector structInspector = (StructObjectInspector) inspector;

			List<? extends StructField> fields = structInspector.getAllStructFieldRefs();

			Row row = new Row(fields.size());
			// StandardStructObjectInspector.getStructFieldData in Hive-1.2.1 only accepts array or list as data
			if (!data.getClass().isArray() && !(data instanceof List) && (inspector instanceof StandardStructObjectInspector)) {
				data = new Object[]{data};
			}
			for (int i = 0; i < row.getArity(); i++) {
				row.setField(
					i,
					toFlinkObject(
							fields.get(i).getFieldObjectInspector(),
							structInspector.getStructFieldData(data, fields.get(i)))
				);
			}
			return row;
		}

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
		} else if (clazz.equals(HiveChar.class) || clazz.equals(HiveCharWritable.class)) {

			typeInfo = TypeInfoFactory.charTypeInfo;
		} else if (clazz.equals(HiveVarchar.class) || clazz.equals(HiveVarcharWritable.class)) {

			typeInfo = TypeInfoFactory.varcharTypeInfo;
		} else if (clazz.equals(HiveDecimal.class) || clazz.equals(HiveDecimalWritable.class)) {

			typeInfo = TypeInfoFactory.decimalTypeInfo;
		} else {
			throw new FlinkHiveUDFException(
				String.format("Class %s is not supported yet", clazz.getName()));
		}

		return getObjectInspector(typeInfo);
	}

	/**
	 * Get Hive {@link ObjectInspector} for a Flink {@link TypeInformation}.
	 */
	public static ObjectInspector getObjectInspector(DataType flinkType) {
		return getObjectInspector(HiveTypeUtil.toHiveTypeInfo(flinkType));
	}

	private static ObjectInspector getObjectInspector(TypeInfo type) {
		switch (type.getCategory()) {

			case PRIMITIVE:
				PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
				return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType);
			case LIST:
				ListTypeInfo listType = (ListTypeInfo) type;
				return ObjectInspectorFactory.getStandardListObjectInspector(
						getObjectInspector(listType.getListElementTypeInfo()));
			case MAP:
				MapTypeInfo mapType = (MapTypeInfo) type;
				return ObjectInspectorFactory.getStandardMapObjectInspector(
						getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
			case STRUCT:
				StructTypeInfo structType = (StructTypeInfo) type;
				List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

				List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
				for (TypeInfo fieldType : fieldTypes) {
					fieldInspectors.add(getObjectInspector(fieldType));
				}

				return ObjectInspectorFactory.getStandardStructObjectInspector(
						structType.getAllStructFieldNames(), fieldInspectors);
			default:
				throw new CatalogException("Unsupported Hive type category " + type.getCategory());
		}
	}

	public static DataType toFlinkType(ObjectInspector inspector) {
		return HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(inspector.getTypeName()));
	}
}
