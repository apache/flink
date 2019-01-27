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

package org.apache.flink.table.hive.functions;

import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteArrayType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive Inspectors of the Converter, which converts the underlying ObjectInspectors
 * to DataType.
 * It also converts the DataType to ObjectInspectors.
 */
public class HiveInspectors {

	public static InternalType inspectorToDataType(ObjectInspector inspector) {
		if (inspector instanceof WritableStringObjectInspector) {
			return StringType.INSTANCE;
		} else if (inspector instanceof JavaStringObjectInspector) {
			return StringType.INSTANCE;
		} else if (inspector instanceof WritableHiveVarcharObjectInspector) {
			return StringType.INSTANCE;
		} else if (inspector instanceof JavaHiveVarcharObjectInspector) {
			return StringType.INSTANCE;
		} else if (inspector instanceof WritableHiveCharObjectInspector) {
			return StringType.INSTANCE;
		} else if (inspector instanceof JavaHiveCharObjectInspector) {
			return StringType.INSTANCE;
		} else if (inspector instanceof WritableIntObjectInspector) {
			return IntType.INSTANCE;
		} else if (inspector instanceof JavaIntObjectInspector) {
			return IntType.INSTANCE;
		} else if (inspector instanceof WritableDoubleObjectInspector) {
			return DoubleType.INSTANCE;
		} else if (inspector instanceof JavaDoubleObjectInspector) {
			return DoubleType.INSTANCE;
		} else if (inspector instanceof WritableBooleanObjectInspector) {
			return BooleanType.INSTANCE;
		} else if (inspector instanceof JavaBooleanObjectInspector) {
			return BooleanType.INSTANCE;
		} else if (inspector instanceof WritableLongObjectInspector) {
			return LongType.INSTANCE;
		} else if (inspector instanceof JavaLongObjectInspector) {
			return LongType.INSTANCE;
		} else if (inspector instanceof WritableShortObjectInspector) {
			return ShortType.INSTANCE;
		} else if (inspector instanceof JavaShortObjectInspector) {
			return ShortType.INSTANCE;
		} else if (inspector instanceof WritableByteObjectInspector) {
			return ByteType.INSTANCE;
		} else if (inspector instanceof JavaByteObjectInspector) {
			return ByteType.INSTANCE;
		} else if (inspector instanceof WritableFloatObjectInspector) {
			return FloatType.INSTANCE;
		} else if (inspector instanceof JavaFloatObjectInspector) {
			return FloatType.INSTANCE;
		} else if (inspector instanceof WritableBinaryObjectInspector) {
			return ByteArrayType.INSTANCE;
		} else if (inspector instanceof JavaBinaryObjectInspector) {
			return ByteArrayType.INSTANCE;
		} else if (inspector instanceof StructObjectInspector) {
			List<? extends StructField> fields = ((StructObjectInspector) inspector).getAllStructFieldRefs();
			InternalType[] types = new InternalType[fields.size()];
			String[] fieldNames = new String[fields.size()];
			int i = 0;
			for (StructField field : fields) {
				InternalType type = inspectorToDataType(field.getFieldObjectInspector());
				String fieldName = field.getFieldName();
				types[i] = type;
				fieldNames[i] = fieldName;
				i++;
			}
			return new RowType(types, fieldNames);
		} else {
			throw new UnsupportedOperationException("Unsupported inspectors: " + inspector);
		}
	}

	public static ObjectInspector[] toInspectors(
			Object[] args,
			List<Boolean> constants) {
		assert args.length == constants.size();
		List<TypeInfo> typeInfos = new ArrayList<>();
		for (Object arg: args) {
			if (arg == null) {
				// If arg is null, we use String Type Info to replace.
				// This may only be called at the default case of the client side.
				typeInfos.add(TypeInfoFactory.stringTypeInfo);
			} else {
				typeInfos.add(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(arg.getClass()));
			}
		}
		ObjectInspector[] argumentInspectors = new ObjectInspector[typeInfos.size()];
		for (int i = 0; i < typeInfos.size(); i++) {
			if (constants.get(i)) {
				argumentInspectors[i] = getPrimitiveJavaConstantObjectInspector(
						(PrimitiveTypeInfo) typeInfos.get(i), args[i]);
			} else {
				argumentInspectors[i] = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
						(PrimitiveTypeInfo) typeInfos.get(i));
			}
		}
		return argumentInspectors;
	}

	private static ConstantObjectInspector getPrimitiveJavaConstantObjectInspector(
			PrimitiveTypeInfo typeInfo, Object value) {
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
				// TODO Test this for blink
				return new JavaConstantHiveDecimalObjectInspector((HiveDecimal) value);
			case BINARY:
				// TODO Test this for blink
				return new JavaConstantBinaryObjectInspector((byte[]) value);
			case UNKNOWN:
				// If type is null, we use the Java Constant String to replace
				return new JavaConstantStringObjectInspector((String) value);
			default:
				throw new RuntimeException("Internal error: Cannot find "
						+ "ConstantObjectInspector for " + typeInfo);
		}
	}

	public static Object unwrap(Object hiveObj, ObjectInspector inspector) {
		if (hiveObj == null) {
			return null;
		}
		if (inspector instanceof VoidObjectInspector) {
			return null;
		} else if (inspector instanceof HiveVarcharObjectInspector) {
			if (((HiveVarcharObjectInspector) inspector).preferWritable()) {
				return ((HiveVarcharObjectInspector) inspector).getPrimitiveWritableObject(hiveObj)
						.getHiveVarchar().getValue();
			} else {
				return ((HiveVarcharObjectInspector) inspector).getPrimitiveJavaObject(hiveObj)
						.getValue();
			}
		} else if (inspector instanceof HiveCharObjectInspector) {
			if (((HiveCharObjectInspector) inspector).preferWritable()) {
				return ((HiveCharObjectInspector) inspector).getPrimitiveWritableObject(hiveObj)
						.getHiveChar().getValue();
			} else {
				return ((HiveCharObjectInspector) inspector).getPrimitiveJavaObject(hiveObj)
						.getValue();
			}
		} else if (inspector instanceof StringObjectInspector) {
			if (((StringObjectInspector) inspector).preferWritable()) {
				return ((StringObjectInspector) inspector).getPrimitiveWritableObject(hiveObj)
						.toString();
			} else {
				return ((StringObjectInspector) inspector).getPrimitiveJavaObject(hiveObj);
			}
		} else if (inspector instanceof IntObjectInspector) {
			return ((IntObjectInspector) inspector).get(hiveObj);
		} else if (inspector instanceof BooleanObjectInspector) {
			return ((BooleanObjectInspector) inspector).get(hiveObj);
		} else if (inspector instanceof FloatObjectInspector) {
			return ((FloatObjectInspector) inspector).get(hiveObj);
		} else if (inspector instanceof DoubleObjectInspector) {
			return ((DoubleObjectInspector) inspector).get(hiveObj);
		} else if (inspector instanceof LongObjectInspector) {
			return ((LongObjectInspector) inspector).get(hiveObj);
		} else if (inspector instanceof ShortObjectInspector) {
			return ((ShortObjectInspector) inspector).get(hiveObj);
		} else if (inspector instanceof ByteObjectInspector) {
			return ((ByteObjectInspector) inspector).get(hiveObj);
		} else if (inspector instanceof HiveDecimalObjectInspector) {
			if (((HiveDecimalObjectInspector) inspector).preferWritable()) {
				return Decimal.fromBigDecimal(
					((HiveDecimalObjectInspector) inspector).getPrimitiveWritableObject(hiveObj)
						.getHiveDecimal().bigDecimalValue(),
					((HiveDecimalObjectInspector) inspector).precision(),
					((HiveDecimalObjectInspector) inspector).scale());
			} else {
				return Decimal.fromBigDecimal(
					((HiveDecimalObjectInspector) inspector).getPrimitiveJavaObject(hiveObj)
						.bigDecimalValue(),
					((HiveDecimalObjectInspector) inspector).precision(),
					((HiveDecimalObjectInspector) inspector).scale());
			}
		} else if (inspector instanceof BinaryObjectInspector) {
			if (((BinaryObjectInspector) inspector).preferWritable()) {
				return ((BinaryObjectInspector) inspector).getPrimitiveWritableObject(hiveObj).getBytes();
			} else {
				return ((BinaryObjectInspector) inspector).getPrimitiveJavaObject(hiveObj);
			}
		} else if (inspector instanceof DateObjectInspector) {
			return ((DateObjectInspector) inspector).getPrimitiveJavaObject(hiveObj);
		} else if (inspector instanceof PrimitiveObjectInspector) {
			return ((PrimitiveObjectInspector) inspector).getPrimitiveJavaObject(hiveObj);
		} else if (inspector instanceof StructObjectInspector) {
			// This is a hive row
			List<? extends StructField> fields = ((StructObjectInspector) inspector).getAllStructFieldRefs();
			GenericRow row = new GenericRow(fields.size());
			for (int i = 0; i < fields.size(); i++) {
				StructField field = fields.get(i);
				Object fieldData = ((StructObjectInspector) inspector).getStructFieldData(hiveObj, field);
				Object rowData = HiveInspectors.unwrap(fieldData, field.getFieldObjectInspector());
				row.update(i, rowData);
			}
			return row;
		}
		throw new RuntimeException("Unsupported exception: " + inspector.toString());
	}
}
