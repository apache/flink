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
package org.apache.calcite.sql.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * SqlTypeFamily provides SQL type categorization.
 *
 * <p>The <em>primary</em> family categorization is a complete disjoint partitioning of SQL types
 * into families, where two types are members of the same primary family iff instances of the two
 * types can be the operands of an SQL equality predicate such as <code>WHERE v1 = v2</code>.
 * Primary families are returned by RelDataType.getFamily().
 *
 * <p>There is also a <em>secondary</em> family categorization which overlaps with the primary
 * categorization. It is used in type strategies for more specific or more general categorization
 * than the primary families. Secondary families are never returned by RelDataType.getFamily().
 *
 * <p>This class was copied over from Calcite to support variant type(CALCITE-4918). When upgrading
 * to Calcite 1.39.0 version, please remove the entire class.
 */
public enum SqlTypeFamily implements RelDataTypeFamily {
    // Primary families.
    CHARACTER,
    BINARY,
    NUMERIC,
    DATE,
    TIME,
    TIMESTAMP,
    BOOLEAN,
    INTERVAL_YEAR_MONTH,
    INTERVAL_DAY_TIME,

    // Secondary families.

    STRING,
    APPROXIMATE_NUMERIC,
    EXACT_NUMERIC,
    DECIMAL,
    INTEGER,
    DATETIME,
    DATETIME_INTERVAL,
    MULTISET,
    ARRAY,
    MAP,
    NULL,
    ANY,
    CURSOR,
    COLUMN_LIST,
    GEO,
    VARIANT,
    /** Like ANY, but do not even validate the operand. It may not be an expression. */
    IGNORE;

    private static final Map<Integer, SqlTypeFamily> JDBC_TYPE_TO_FAMILY =
            ImmutableMap.<Integer, SqlTypeFamily>builder()
                    // Not present:
                    // SqlTypeName.MULTISET shares Types.ARRAY with SqlTypeName.ARRAY;
                    // SqlTypeName.MAP has no corresponding JDBC type
                    // SqlTypeName.COLUMN_LIST has no corresponding JDBC type
                    .put(Types.BIT, NUMERIC)
                    .put(Types.TINYINT, NUMERIC)
                    .put(Types.SMALLINT, NUMERIC)
                    .put(Types.BIGINT, NUMERIC)
                    .put(Types.INTEGER, NUMERIC)
                    .put(Types.NUMERIC, NUMERIC)
                    .put(Types.DECIMAL, NUMERIC)
                    .put(Types.FLOAT, NUMERIC)
                    .put(Types.REAL, NUMERIC)
                    .put(Types.DOUBLE, NUMERIC)
                    .put(Types.CHAR, CHARACTER)
                    .put(Types.VARCHAR, CHARACTER)
                    .put(Types.LONGVARCHAR, CHARACTER)
                    .put(Types.CLOB, CHARACTER)
                    .put(Types.BINARY, BINARY)
                    .put(Types.VARBINARY, BINARY)
                    .put(Types.LONGVARBINARY, BINARY)
                    .put(Types.BLOB, BINARY)
                    .put(Types.DATE, DATE)
                    .put(Types.TIME, TIME)
                    .put(ExtraSqlTypes.TIME_WITH_TIMEZONE, TIME)
                    .put(Types.TIMESTAMP, TIMESTAMP)
                    .put(ExtraSqlTypes.TIMESTAMP_WITH_TIMEZONE, TIMESTAMP)
                    .put(Types.BOOLEAN, BOOLEAN)
                    .put(ExtraSqlTypes.REF_CURSOR, CURSOR)
                    .put(Types.ARRAY, ARRAY)
                    .put(Types.JAVA_OBJECT, VARIANT)
                    .build();

    /**
     * Gets the primary family containing a JDBC type.
     *
     * @param jdbcType the JDBC type of interest
     * @return containing family
     */
    public static @Nullable SqlTypeFamily getFamilyForJdbcType(int jdbcType) {
        return JDBC_TYPE_TO_FAMILY.get(jdbcType);
    }

    /**
     * For this type family, returns the allow types of the difference between two values of this
     * family.
     *
     * <p>Equivalently, given an {@code ORDER BY} expression with one key, returns the allowable
     * type families of the difference between two keys.
     *
     * <p>Example 1. For {@code ORDER BY empno}, a NUMERIC, the difference between two {@code empno}
     * values is also NUMERIC.
     *
     * <p>Example 2. For {@code ORDER BY hireDate}, a DATE, the difference between two {@code
     * hireDate} values might be an INTERVAL_DAY_TIME or INTERVAL_YEAR_MONTH.
     *
     * <p>The result determines whether a {@link SqlWindow} with a {@code RANGE} is valid (for
     * example, {@code OVER (ORDER BY empno RANGE 10} is valid because {@code 10} is numeric); and
     * whether a call to {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#PERCENTILE_CONT
     * PERCENTILE_CONT} is valid (for example, {@code PERCENTILE_CONT(0.25)} ORDER BY (hireDate)} is
     * valid because {@code hireDate} values may be interpolated by adding values of type {@code
     * INTERVAL_DAY_TIME}.
     */
    public List<SqlTypeFamily> allowableDifferenceTypes() {
        switch (this) {
            case NUMERIC:
                return ImmutableList.of(NUMERIC);
            case DATE:
            case TIME:
            case TIMESTAMP:
                return ImmutableList.of(INTERVAL_DAY_TIME, INTERVAL_YEAR_MONTH);
            default:
                return ImmutableList.of();
        }
    }

    /** Returns the collection of {@link SqlTypeName}s included in this family. */
    public Collection<SqlTypeName> getTypeNames() {
        switch (this) {
            case CHARACTER:
                return SqlTypeName.CHAR_TYPES;
            case BINARY:
                return SqlTypeName.BINARY_TYPES;
            case NUMERIC:
                return SqlTypeName.NUMERIC_TYPES;
            case DECIMAL:
                return ImmutableList.of(SqlTypeName.DECIMAL);
            case DATE:
                return ImmutableList.of(SqlTypeName.DATE);
            case TIME:
                return ImmutableList.of(SqlTypeName.TIME, SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
            case TIMESTAMP:
                return ImmutableList.of(
                        SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
            case BOOLEAN:
                return SqlTypeName.BOOLEAN_TYPES;
            case INTERVAL_YEAR_MONTH:
                return SqlTypeName.YEAR_INTERVAL_TYPES;
            case INTERVAL_DAY_TIME:
                return SqlTypeName.DAY_INTERVAL_TYPES;
            case STRING:
                return SqlTypeName.STRING_TYPES;
            case APPROXIMATE_NUMERIC:
                return SqlTypeName.APPROX_TYPES;
            case EXACT_NUMERIC:
                return SqlTypeName.EXACT_TYPES;
            case INTEGER:
                return SqlTypeName.INT_TYPES;
            case DATETIME:
                return SqlTypeName.DATETIME_TYPES;
            case DATETIME_INTERVAL:
                return SqlTypeName.INTERVAL_TYPES;
            case GEO:
                return SqlTypeName.GEOMETRY_TYPES;
            case MULTISET:
                return ImmutableList.of(SqlTypeName.MULTISET);
            case ARRAY:
                return ImmutableList.of(SqlTypeName.ARRAY);
            case MAP:
                return ImmutableList.of(SqlTypeName.MAP);
            case NULL:
                return ImmutableList.of(SqlTypeName.NULL);
            case ANY:
                return SqlTypeName.ALL_TYPES;
            case CURSOR:
                return ImmutableList.of(SqlTypeName.CURSOR);
            case COLUMN_LIST:
                return ImmutableList.of(SqlTypeName.COLUMN_LIST);
            case VARIANT:
                return ImmutableList.of(SqlTypeName.VARIANT);
            default:
                throw new IllegalArgumentException();
        }
    }

    /** Return the default {@link RelDataType} that belongs to this family. */
    public @Nullable RelDataType getDefaultConcreteType(RelDataTypeFactory factory) {
        switch (this) {
            case CHARACTER:
                return factory.createSqlType(SqlTypeName.VARCHAR);
            case BINARY:
                return factory.createSqlType(SqlTypeName.VARBINARY);
            case NUMERIC:
                return SqlTypeUtil.getMaxPrecisionScaleDecimal(factory);
            case DATE:
                return factory.createSqlType(SqlTypeName.DATE);
            case TIME:
                return factory.createSqlType(SqlTypeName.TIME);
            case TIMESTAMP:
                return factory.createSqlType(SqlTypeName.TIMESTAMP);
            case BOOLEAN:
                return factory.createSqlType(SqlTypeName.BOOLEAN);
            case STRING:
                return factory.createSqlType(SqlTypeName.VARCHAR);
            case APPROXIMATE_NUMERIC:
                return factory.createSqlType(SqlTypeName.DOUBLE);
            case EXACT_NUMERIC:
                return SqlTypeUtil.getMaxPrecisionScaleDecimal(factory);
            case INTEGER:
                return factory.createSqlType(SqlTypeName.BIGINT);
            case DECIMAL:
                return factory.createSqlType(SqlTypeName.DECIMAL);
            case DATETIME:
                return factory.createSqlType(SqlTypeName.TIMESTAMP);
            case INTERVAL_DAY_TIME:
                return factory.createSqlIntervalType(
                        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO));
            case INTERVAL_YEAR_MONTH:
                return factory.createSqlIntervalType(
                        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO));
            case GEO:
                return factory.createSqlType(SqlTypeName.GEOMETRY);
            case MULTISET:
                return factory.createMultisetType(factory.createSqlType(SqlTypeName.ANY), -1);
            case ARRAY:
                return factory.createArrayType(factory.createSqlType(SqlTypeName.ANY), -1);
            case MAP:
                return factory.createMapType(
                        factory.createSqlType(SqlTypeName.ANY),
                        factory.createSqlType(SqlTypeName.ANY));
            case NULL:
                return factory.createSqlType(SqlTypeName.NULL);
            case CURSOR:
                return factory.createSqlType(SqlTypeName.CURSOR);
            case COLUMN_LIST:
                return factory.createSqlType(SqlTypeName.COLUMN_LIST);
            default:
                return null;
        }
    }

    public boolean contains(RelDataType type) {
        return SqlTypeUtil.isOfSameTypeName(getTypeNames(), type);
    }
}
