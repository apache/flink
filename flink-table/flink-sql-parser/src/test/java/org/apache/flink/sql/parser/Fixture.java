/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/** Type used during tests. */
public class Fixture {
    private final TestRelDataTypeFactory typeFactory;

    static final String RAW_TYPE_INT_CLASS = "java.lang.Integer";
    static final String RAW_TYPE_INT_SERIALIZER_STRING = "<Serializer Snapshot>";

    final RelDataType char1Type;
    final RelDataType char33Type;
    final RelDataType varcharType;
    final RelDataType varchar33Type;
    final RelDataType booleanType;
    final RelDataType binaryType;
    final RelDataType binary33Type;
    final RelDataType varbinaryType;
    final RelDataType varbinary33Type;
    final RelDataType decimalType;
    final RelDataType decimalP10S0Type;
    final RelDataType decimalP10S3Type;
    final RelDataType tinyintType;
    final RelDataType smallintType;
    final RelDataType intType;
    final RelDataType bigintType;
    final RelDataType floatType;
    final RelDataType doubleType;
    final RelDataType dateType;
    final RelDataType timeType;
    final RelDataType time3Type;
    final RelDataType timestampType;
    final RelDataType timestamp3Type;
    final RelDataType timestampWithLocalTimeZoneType;
    final RelDataType timestamp3WithLocalTimeZoneType;
    final RelDataType nullType;
    final RelDataType rawTypeOfInteger;

    Fixture(TestRelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
        this.char1Type = typeFactory.createSqlType(SqlTypeName.CHAR);
        this.char33Type = typeFactory.createSqlType(SqlTypeName.CHAR, 33);
        this.varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        this.varchar33Type = typeFactory.createSqlType(SqlTypeName.VARCHAR, 33);
        this.booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        this.binaryType = typeFactory.createSqlType(SqlTypeName.BINARY);
        this.binary33Type = typeFactory.createSqlType(SqlTypeName.BINARY, 33);
        this.varbinaryType = typeFactory.createSqlType(SqlTypeName.VARBINARY);
        this.varbinary33Type = typeFactory.createSqlType(SqlTypeName.VARBINARY, 33);
        this.decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
        this.decimalP10S0Type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10);
        this.decimalP10S3Type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 3);
        this.tinyintType = typeFactory.createSqlType(SqlTypeName.TINYINT);
        this.smallintType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
        this.intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        this.bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        this.floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        this.doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        this.dateType = typeFactory.createSqlType(SqlTypeName.DATE);
        this.timeType = typeFactory.createSqlType(SqlTypeName.TIME);
        this.time3Type = typeFactory.createSqlType(SqlTypeName.TIME, 3);
        this.timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        this.timestamp3Type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
        this.timestampWithLocalTimeZoneType =
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        this.timestamp3WithLocalTimeZoneType =
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
        this.nullType = typeFactory.createSqlType(SqlTypeName.NULL);
        this.rawTypeOfInteger =
                typeFactory.createRawType(RAW_TYPE_INT_CLASS, RAW_TYPE_INT_SERIALIZER_STRING);
    }

    public RelDataType createSqlType(SqlTypeName sqlTypeName, int precision) {
        return typeFactory.createSqlType(sqlTypeName, precision);
    }

    public RelDataType createArrayType(RelDataType elementType) {
        return typeFactory.createArrayType(elementType, -1);
    }

    public RelDataType createMultisetType(RelDataType elementType) {
        return typeFactory.createMultisetType(elementType, -1);
    }

    public RelDataType createMapType(RelDataType keyType, RelDataType valType) {
        return typeFactory.createMapType(keyType, valType);
    }

    public RelDataType createStructType(List<RelDataType> keyTypes, List<String> names) {
        return typeFactory.createStructType(keyTypes, names);
    }

    public RelDataType createRawType(String className, String serializerString) {
        return typeFactory.createRawType(className, serializerString);
    }

    public RelDataType nullable(RelDataType type) {
        return typeFactory.createTypeWithNullability(type, true);
    }
}
