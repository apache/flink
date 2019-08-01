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

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utils to convert data types between Flink and Hive.
 */
@Internal
public class HiveTypeUtil {

	private HiveTypeUtil() {
	}

	/**
	 * Convert Flink data type to Hive data type name.
	 *
	 * @param type a Flink data type
	 * @return the corresponding Hive data type name
	 */
	public static String toHiveTypeName(DataType type) {
		checkNotNull(type, "type cannot be null");

		return toHiveTypeInfo(type).getTypeName();
	}

	/**
	 * Convert Flink data type to Hive data type.
	 *
	 * @param dataType a Flink data type
	 * @return the corresponding Hive data type
	 */
	public static TypeInfo toHiveTypeInfo(DataType dataType) {
		checkNotNull(dataType, "type cannot be null");

//		LogicalTypeRoot type = dataType.getLogicalType().getTypeRoot();
		LogicalType logicalType = dataType.getLogicalType();
		TypeInfo typeInfo = logicalType.accept(new TypeInfoLogicalTypeVisitor());
		if (null == typeInfo) {
			throw new UnsupportedOperationException(
					String.format("Flink doesn't support converting type %s to Hive type yet.", dataType.toString()));
		} else {
			return typeInfo;
		}
	}

	/**
	 * Convert a Hive ObjectInspector to a Flink data type.
	 *
	 * @param inspector a Hive inspector
	 * @return the corresponding Flink data type
	 */
	public static DataType toFlinkType(ObjectInspector inspector) {
		return toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(inspector.getTypeName()));
	}

	/**
	 * Convert Hive data type to a Flink data type.
	 *
	 * @param hiveType a Hive data type
	 * @return the corresponding Flink data type
	 */
	public static DataType toFlinkType(TypeInfo hiveType) {
		checkNotNull(hiveType, "hiveType cannot be null");

		switch (hiveType.getCategory()) {
			case PRIMITIVE:
				return toFlinkPrimitiveType((PrimitiveTypeInfo) hiveType);
			case LIST:
				ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType;
				return DataTypes.ARRAY(toFlinkType(listTypeInfo.getListElementTypeInfo()));
			case MAP:
				MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveType;
				return DataTypes.MAP(toFlinkType(mapTypeInfo.getMapKeyTypeInfo()), toFlinkType(mapTypeInfo.getMapValueTypeInfo()));
			case STRUCT:
				StructTypeInfo structTypeInfo = (StructTypeInfo) hiveType;

				List<String> names = structTypeInfo.getAllStructFieldNames();
				List<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();

				DataTypes.Field[] fields = new DataTypes.Field[names.size()];

				for (int i = 0; i < fields.length; i++) {
					fields[i] = DataTypes.FIELD(names.get(i), toFlinkType(typeInfos.get(i)));
				}

				return DataTypes.ROW(fields);
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive data type %s yet.", hiveType));
		}
	}

	private static DataType toFlinkPrimitiveType(PrimitiveTypeInfo hiveType) {
		checkNotNull(hiveType, "hiveType cannot be null");

		switch (hiveType.getPrimitiveCategory()) {
			case CHAR:
				return DataTypes.CHAR(((CharTypeInfo) hiveType).getLength());
			case VARCHAR:
				return DataTypes.VARCHAR(((VarcharTypeInfo) hiveType).getLength());
			case STRING:
				return DataTypes.STRING();
			case BOOLEAN:
				return DataTypes.BOOLEAN();
			case BYTE:
				return DataTypes.TINYINT();
			case SHORT:
				return DataTypes.SMALLINT();
			case INT:
				return DataTypes.INT();
			case LONG:
				return DataTypes.BIGINT();
			case FLOAT:
				return DataTypes.FLOAT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case DATE:
				return DataTypes.DATE();
			case TIMESTAMP:
				return DataTypes.TIMESTAMP();
			case BINARY:
				return DataTypes.BYTES();
			case DECIMAL:
				DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveType;
				return DataTypes.DECIMAL(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive primitive type %s yet", hiveType));
		}
	}

	private static class TypeInfoLogicalTypeVisitor implements LogicalTypeVisitor<TypeInfo> {
		@Override
		public TypeInfo visit(CharType charType) {
			if (charType.getLength() > HiveChar.MAX_CHAR_LENGTH) {
				throw new CatalogException(
						String.format("HiveCatalog doesn't support char type with length of '%d'. " +
									"The maximum length is %d",
									charType.getLength(), HiveChar.MAX_CHAR_LENGTH));
			}
			return TypeInfoFactory.getCharTypeInfo(charType.getLength());
		}

		@Override
		public TypeInfo visit(VarCharType varCharType) {
			// Flink's StringType is defined as VARCHAR(Integer.MAX_VALUE)
			// We don't have more information in LogicalTypeRoot to distinguish StringType and a VARCHAR(Integer.MAX_VALUE) instance
			// Thus always treat VARCHAR(Integer.MAX_VALUE) as StringType
			if (varCharType.getLength() == Integer.MAX_VALUE) {
				return TypeInfoFactory.stringTypeInfo;
			}
			if (varCharType.getLength() > HiveVarchar.MAX_VARCHAR_LENGTH) {
				throw new CatalogException(
						String.format("HiveCatalog doesn't support varchar type with length of '%d'. " +
									"The maximum length is %d",
									varCharType.getLength(), HiveVarchar.MAX_VARCHAR_LENGTH));
			}
			return TypeInfoFactory.getVarcharTypeInfo(varCharType.getLength());
		}

		@Override
		public TypeInfo visit(BooleanType booleanType) {
			return TypeInfoFactory.booleanTypeInfo;
		}

		@Override
		public TypeInfo visit(BinaryType binaryType) {
			return null;
		}

		@Override
		public TypeInfo visit(VarBinaryType varBinaryType) {
			// Flink's BytesType is defined as VARBINARY(Integer.MAX_VALUE)
			// We don't have more information in LogicalTypeRoot to distinguish BytesType and a VARBINARY(Integer.MAX_VALUE) instance
			// Thus always treat VARBINARY(Integer.MAX_VALUE) as BytesType
			if (varBinaryType.getLength() == VarBinaryType.MAX_LENGTH) {
				return TypeInfoFactory.binaryTypeInfo;
			}
			return null;
		}

		@Override
		public TypeInfo visit(DecimalType decimalType) {
			// Flink and Hive share the same precision and scale range
			// Flink already validates the type so we don't need to validate again here
			return TypeInfoFactory.getDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
		}

		@Override
		public TypeInfo visit(TinyIntType tinyIntType) {
			return TypeInfoFactory.byteTypeInfo;
		}

		@Override
		public TypeInfo visit(SmallIntType smallIntType) {
			return TypeInfoFactory.shortTypeInfo;
		}

		@Override
		public TypeInfo visit(IntType intType) {
			return TypeInfoFactory.intTypeInfo;
		}

		@Override
		public TypeInfo visit(BigIntType bigIntType) {
			return TypeInfoFactory.longTypeInfo;
		}

		@Override
		public TypeInfo visit(FloatType floatType) {
			return TypeInfoFactory.floatTypeInfo;
		}

		@Override
		public TypeInfo visit(DoubleType doubleType) {
			return TypeInfoFactory.doubleTypeInfo;
		}

		@Override
		public TypeInfo visit(DateType dateType) {
			return TypeInfoFactory.dateTypeInfo;
		}

		@Override
		public TypeInfo visit(TimeType timeType) {
			return null;
		}

		@Override
		public TypeInfo visit(TimestampType timestampType) {
			return TypeInfoFactory.timestampTypeInfo;
		}

		@Override
		public TypeInfo visit(ZonedTimestampType zonedTimestampType) {
			return null;
		}

		@Override
		public TypeInfo visit(LocalZonedTimestampType localZonedTimestampType) {
			return null;
		}

		@Override
		public TypeInfo visit(YearMonthIntervalType yearMonthIntervalType) {
			return null;
		}

		@Override
		public TypeInfo visit(DayTimeIntervalType dayTimeIntervalType) {
			return null;
		}

		@Override
		public TypeInfo visit(ArrayType arrayType) {
			LogicalType elementType = arrayType.getElementType();
			TypeInfo elementTypeInfo = elementType.accept(new TypeInfoLogicalTypeVisitor());
			if (null != elementTypeInfo) {
				return TypeInfoFactory.getListTypeInfo(elementTypeInfo);
			} else {
				return null;
			}
		}

		@Override
		public TypeInfo visit(MultisetType multisetType) {
			return null;
		}

		@Override
		public TypeInfo visit(MapType mapType) {
			LogicalType keyType  = mapType.getKeyType();
			LogicalType valueType = mapType.getValueType();
			TypeInfo keyTypeInfo = keyType.accept(new TypeInfoLogicalTypeVisitor());
			TypeInfo valueTypeInfo = valueType.accept(new TypeInfoLogicalTypeVisitor());
			if (null == keyTypeInfo || null == valueTypeInfo) {
				return null;
			} else {
				return TypeInfoFactory.getMapTypeInfo(keyTypeInfo, valueTypeInfo);
			}
		}

		@Override
		public TypeInfo visit(RowType rowType) {
			List<String> names = rowType.getFieldNames();
			List<TypeInfo> typeInfos = new ArrayList<>(names.size());
			for (String name : names) {
				TypeInfo typeInfo =
						rowType.getTypeAt(rowType.getFieldIndex(name)).accept(new TypeInfoLogicalTypeVisitor());
				if (null != typeInfo) {
					typeInfos.add(typeInfo);
				} else {
					return null;
				}
			}
			return TypeInfoFactory.getStructTypeInfo(names, typeInfos);
		}

		@Override
		public TypeInfo visit(DistinctType distinctType) {
			return null;
		}

		@Override
		public TypeInfo visit(StructuredType structuredType) {
			return null;
		}

		@Override
		public TypeInfo visit(NullType nullType) {
			return null;
		}

		@Override
		public TypeInfo visit(AnyType<?> anyType) {
			return null;
		}

		@Override
		public TypeInfo visit(SymbolType<?> symbolType) {
			return null;
		}

		@Override
		public TypeInfo visit(LogicalType other) {
			return null;
		}
	}
}
