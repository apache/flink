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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** Counterpart of hive's org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter. */
public class HiveParserTypeConverter {

    public static RelDataType getType(
            RelOptCluster cluster, HiveParserRowResolver rr, List<String> neededCols)
            throws SemanticException {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
        RowSchema rs = rr.getRowSchema();
        List<RelDataType> fieldTypes = new LinkedList<>();
        List<String> fieldNames = new LinkedList<>();

        for (ColumnInfo ci : rs.getSignature()) {
            if (neededCols == null || neededCols.contains(ci.getInternalName())) {
                fieldTypes.add(convert(ci.getType(), dtFactory));
                fieldNames.add(ci.getInternalName());
            }
        }
        return dtFactory.createStructType(fieldTypes, fieldNames);
    }

    public static RelDataType convert(TypeInfo type, RelDataTypeFactory dtFactory)
            throws SemanticException {
        RelDataType convertedType = null;

        switch (type.getCategory()) {
            case PRIMITIVE:
                convertedType = convert((PrimitiveTypeInfo) type, dtFactory);
                break;
            case LIST:
                convertedType = convert((ListTypeInfo) type, dtFactory);
                break;
            case MAP:
                convertedType = convert((MapTypeInfo) type, dtFactory);
                break;
            case STRUCT:
                convertedType = convert((StructTypeInfo) type, dtFactory);
                break;
            case UNION:
                convertedType = convert((UnionTypeInfo) type, dtFactory);
                break;
        }
        return convertedType;
    }

    public static RelDataType convert(PrimitiveTypeInfo type, RelDataTypeFactory dtFactory) {
        RelDataType convertedType = null;
        HiveShim hiveShim = HiveParserUtils.getSessionHiveShim();

        switch (type.getPrimitiveCategory()) {
            case VOID:
                convertedType = dtFactory.createSqlType(SqlTypeName.NULL);
                break;
            case BOOLEAN:
                convertedType = dtFactory.createSqlType(SqlTypeName.BOOLEAN);
                break;
            case BYTE:
                convertedType = dtFactory.createSqlType(SqlTypeName.TINYINT);
                break;
            case SHORT:
                convertedType = dtFactory.createSqlType(SqlTypeName.SMALLINT);
                break;
            case INT:
                convertedType = dtFactory.createSqlType(SqlTypeName.INTEGER);
                break;
            case LONG:
                convertedType = dtFactory.createSqlType(SqlTypeName.BIGINT);
                break;
            case FLOAT:
                convertedType = dtFactory.createSqlType(SqlTypeName.FLOAT);
                break;
            case DOUBLE:
                convertedType = dtFactory.createSqlType(SqlTypeName.DOUBLE);
                break;
            case STRING:
                convertedType =
                        dtFactory.createTypeWithCharsetAndCollation(
                                dtFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
                                Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME),
                                SqlCollation.IMPLICIT);
                break;
            case DATE:
                convertedType = dtFactory.createSqlType(SqlTypeName.DATE);
                break;
            case TIMESTAMP:
                convertedType = dtFactory.createSqlType(SqlTypeName.TIMESTAMP, 9);
                break;
            case BINARY:
                convertedType = dtFactory.createSqlType(SqlTypeName.BINARY);
                break;
            case DECIMAL:
                DecimalTypeInfo dtInf = (DecimalTypeInfo) type;
                convertedType =
                        dtFactory.createSqlType(
                                SqlTypeName.DECIMAL, dtInf.precision(), dtInf.scale());
                break;
            case VARCHAR:
                convertedType =
                        dtFactory.createTypeWithCharsetAndCollation(
                                dtFactory.createSqlType(
                                        SqlTypeName.VARCHAR, ((BaseCharTypeInfo) type).getLength()),
                                Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME),
                                SqlCollation.IMPLICIT);
                break;
            case CHAR:
                convertedType =
                        dtFactory.createTypeWithCharsetAndCollation(
                                dtFactory.createSqlType(
                                        SqlTypeName.CHAR, ((BaseCharTypeInfo) type).getLength()),
                                Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME),
                                SqlCollation.IMPLICIT);
                break;
            case UNKNOWN:
                convertedType = dtFactory.createSqlType(SqlTypeName.OTHER);
                break;
            default:
                if (hiveShim.isIntervalYearMonthType(type.getPrimitiveCategory())) {
                    convertedType =
                            dtFactory.createSqlIntervalType(
                                    new SqlIntervalQualifier(
                                            TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
                } else if (hiveShim.isIntervalDayTimeType(type.getPrimitiveCategory())) {
                    convertedType =
                            dtFactory.createSqlIntervalType(
                                    new SqlIntervalQualifier(
                                            TimeUnit.DAY, TimeUnit.SECOND, new SqlParserPos(1, 1)));
                }
        }

        if (null == convertedType) {
            throw new RuntimeException("Unsupported Type : " + type.getTypeName());
        }

        return dtFactory.createTypeWithNullability(convertedType, true);
    }

    private static RelDataType convert(ListTypeInfo lstType, RelDataTypeFactory dtFactory)
            throws SemanticException {
        RelDataType elemType = convert(lstType.getListElementTypeInfo(), dtFactory);
        return dtFactory.createArrayType(elemType, -1);
    }

    private static RelDataType convert(MapTypeInfo mapType, RelDataTypeFactory dtFactory)
            throws SemanticException {
        RelDataType keyType = convert(mapType.getMapKeyTypeInfo(), dtFactory);
        RelDataType valueType = convert(mapType.getMapValueTypeInfo(), dtFactory);
        return dtFactory.createMapType(keyType, valueType);
    }

    private static RelDataType convert(
            StructTypeInfo structType, final RelDataTypeFactory dtFactory)
            throws SemanticException {
        List<RelDataType> fTypes = new ArrayList<>(structType.getAllStructFieldTypeInfos().size());
        for (TypeInfo ti : structType.getAllStructFieldTypeInfos()) {
            fTypes.add(convert(ti, dtFactory));
        }
        return dtFactory.createStructType(fTypes, structType.getAllStructFieldNames());
    }

    private static RelDataType convert(UnionTypeInfo unionType, RelDataTypeFactory dtFactory)
            throws SemanticException {
        // Union type is not supported in Calcite.
        throw new SemanticException("Union type is not supported");
    }

    public static TypeInfo convert(RelDataType rType) {
        if (rType.isStruct()) {
            return convertStructType(rType);
        } else if (rType.getComponentType() != null) {
            return convertListType(rType);
        } else if (rType.getKeyType() != null) {
            return convertMapType(rType);
        } else {
            return convertPrimitiveType(rType);
        }
    }

    private static TypeInfo convertStructType(RelDataType rType) {
        List<TypeInfo> fTypes =
                rType.getFieldList().stream()
                        .map(f -> convert(f.getType()))
                        .collect(Collectors.toList());
        List<String> fNames =
                rType.getFieldList().stream()
                        .map(RelDataTypeField::getName)
                        .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fNames, fTypes);
    }

    private static TypeInfo convertMapType(RelDataType rType) {
        return TypeInfoFactory.getMapTypeInfo(
                convert(rType.getKeyType()), convert(rType.getValueType()));
    }

    private static TypeInfo convertListType(RelDataType rType) {
        return TypeInfoFactory.getListTypeInfo(convert(rType.getComponentType()));
    }

    private static TypeInfo convertPrimitiveType(RelDataType rType) {
        HiveShim hiveShim = HiveParserUtils.getSessionHiveShim();
        switch (rType.getSqlTypeName()) {
            case BOOLEAN:
                return TypeInfoFactory.booleanTypeInfo;
            case TINYINT:
                return TypeInfoFactory.byteTypeInfo;
            case SMALLINT:
                return TypeInfoFactory.shortTypeInfo;
            case INTEGER:
                return TypeInfoFactory.intTypeInfo;
            case BIGINT:
                return TypeInfoFactory.longTypeInfo;
            case FLOAT:
                return TypeInfoFactory.floatTypeInfo;
            case DOUBLE:
                return TypeInfoFactory.doubleTypeInfo;
            case DATE:
                return TypeInfoFactory.dateTypeInfo;
            case TIMESTAMP:
                return TypeInfoFactory.timestampTypeInfo;
            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
                return hiveShim.getIntervalYearMonthTypeInfo();
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return hiveShim.getIntervalDayTimeTypeInfo();
            case BINARY:
                return TypeInfoFactory.binaryTypeInfo;
            case DECIMAL:
                return TypeInfoFactory.getDecimalTypeInfo(rType.getPrecision(), rType.getScale());
            case VARCHAR:
                int varcharLength = rType.getPrecision();
                if (varcharLength < 1 || varcharLength > HiveVarchar.MAX_VARCHAR_LENGTH) {
                    return TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
                } else {
                    return TypeInfoFactory.getVarcharTypeInfo(varcharLength);
                }
            case CHAR:
                int charLength = rType.getPrecision();
                if (charLength < 1 || charLength > HiveChar.MAX_CHAR_LENGTH) {
                    return TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
                } else {
                    return TypeInfoFactory.getCharTypeInfo(charLength);
                }
            case OTHER:
            default:
                return TypeInfoFactory.voidTypeInfo;
        }
    }
}
