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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enumeration of the type names which can be used to construct a SQL type. Rationale for this
 * class's existence (instead of just using the standard java.sql.Type ordinals):
 *
 * <ul>
 *   <li>{@link Types} does not include all SQL2003 data-types;
 *   <li>SqlTypeName provides a type-safe enumeration;
 *   <li>SqlTypeName provides a place to hang extra information such as whether the type carries
 *       precision and scale.
 * </ul>
 *
 * <p>This class was copied over from Calcite to support variant type(CALCITE-4918). When upgrading
 * to Calcite 1.39.0 version, please remove the entire class.
 */
public enum SqlTypeName {
    BOOLEAN(PrecScale.NO_NO, false, Types.BOOLEAN, SqlTypeFamily.BOOLEAN),
    TINYINT(PrecScale.NO_NO, false, Types.TINYINT, SqlTypeFamily.NUMERIC),
    SMALLINT(PrecScale.NO_NO, false, Types.SMALLINT, SqlTypeFamily.NUMERIC),
    INTEGER(PrecScale.NO_NO, false, Types.INTEGER, SqlTypeFamily.NUMERIC),
    BIGINT(PrecScale.NO_NO, false, Types.BIGINT, SqlTypeFamily.NUMERIC),
    DECIMAL(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.DECIMAL,
            SqlTypeFamily.NUMERIC),
    FLOAT(PrecScale.NO_NO, false, Types.FLOAT, SqlTypeFamily.NUMERIC),
    REAL(PrecScale.NO_NO, false, Types.REAL, SqlTypeFamily.NUMERIC),
    DOUBLE(PrecScale.NO_NO, false, Types.DOUBLE, SqlTypeFamily.NUMERIC),
    DATE(PrecScale.NO_NO, false, Types.DATE, SqlTypeFamily.DATE),
    TIME(PrecScale.NO_NO | PrecScale.YES_NO, false, Types.TIME, SqlTypeFamily.TIME),
    TIME_WITH_LOCAL_TIME_ZONE(
            PrecScale.NO_NO | PrecScale.YES_NO, false, Types.OTHER, SqlTypeFamily.TIME),
    TIMESTAMP(PrecScale.NO_NO | PrecScale.YES_NO, false, Types.TIMESTAMP, SqlTypeFamily.TIMESTAMP),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            PrecScale.NO_NO | PrecScale.YES_NO, false, Types.TIMESTAMP, SqlTypeFamily.TIMESTAMP),
    INTERVAL_YEAR(PrecScale.NO_NO, false, Types.OTHER, SqlTypeFamily.INTERVAL_YEAR_MONTH),
    INTERVAL_YEAR_MONTH(PrecScale.NO_NO, false, Types.OTHER, SqlTypeFamily.INTERVAL_YEAR_MONTH),
    INTERVAL_MONTH(PrecScale.NO_NO, false, Types.OTHER, SqlTypeFamily.INTERVAL_YEAR_MONTH),
    INTERVAL_DAY(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_DAY_HOUR(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_DAY_MINUTE(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_DAY_SECOND(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_HOUR(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_HOUR_MINUTE(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_HOUR_SECOND(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_MINUTE(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_MINUTE_SECOND(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    INTERVAL_SECOND(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            false,
            Types.OTHER,
            SqlTypeFamily.INTERVAL_DAY_TIME),
    CHAR(PrecScale.NO_NO | PrecScale.YES_NO, false, Types.CHAR, SqlTypeFamily.CHARACTER),
    VARCHAR(PrecScale.NO_NO | PrecScale.YES_NO, false, Types.VARCHAR, SqlTypeFamily.CHARACTER),
    BINARY(PrecScale.NO_NO | PrecScale.YES_NO, false, Types.BINARY, SqlTypeFamily.BINARY),
    VARBINARY(PrecScale.NO_NO | PrecScale.YES_NO, false, Types.VARBINARY, SqlTypeFamily.BINARY),
    NULL(PrecScale.NO_NO, true, Types.NULL, SqlTypeFamily.NULL),
    UNKNOWN(PrecScale.NO_NO, true, Types.NULL, SqlTypeFamily.NULL),
    ANY(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            true,
            Types.JAVA_OBJECT,
            SqlTypeFamily.ANY),
    SYMBOL(PrecScale.NO_NO, true, Types.OTHER, null),
    MULTISET(PrecScale.NO_NO, false, Types.ARRAY, SqlTypeFamily.MULTISET),
    ARRAY(PrecScale.NO_NO, false, Types.ARRAY, SqlTypeFamily.ARRAY),
    MAP(PrecScale.NO_NO, false, Types.OTHER, SqlTypeFamily.MAP),
    DISTINCT(PrecScale.NO_NO, false, Types.DISTINCT, null),
    STRUCTURED(PrecScale.NO_NO, false, Types.STRUCT, null),
    ROW(PrecScale.NO_NO, false, Types.STRUCT, null),
    OTHER(PrecScale.NO_NO, false, Types.OTHER, null),
    CURSOR(PrecScale.NO_NO, false, ExtraSqlTypes.REF_CURSOR, SqlTypeFamily.CURSOR),
    COLUMN_LIST(PrecScale.NO_NO, false, Types.OTHER + 2, SqlTypeFamily.COLUMN_LIST),
    DYNAMIC_STAR(
            PrecScale.NO_NO | PrecScale.YES_NO | PrecScale.YES_YES,
            true,
            Types.JAVA_OBJECT,
            SqlTypeFamily.ANY),
    /**
     * Spatial type. Though not standard, it is common to several DBs, so we do not flag it
     * 'special' (internal).
     */
    GEOMETRY(PrecScale.NO_NO, false, ExtraSqlTypes.GEOMETRY, SqlTypeFamily.GEO),
    MEASURE(PrecScale.NO_NO, true, Types.OTHER, SqlTypeFamily.ANY),
    SARG(PrecScale.NO_NO, true, Types.OTHER, SqlTypeFamily.ANY),
    /**
     * VARIANT data type, a dynamically-typed value that can have at runtime any of the other data
     * types in this table.
     */
    VARIANT(PrecScale.NO_NO, false, Types.OTHER, SqlTypeFamily.VARIANT);

    public static final int MAX_DATETIME_PRECISION = 3;

    // Minimum and default interval precisions are  defined by SQL2003
    // Maximum interval precisions are implementation dependent,
    //  but must be at least the default value
    public static final int DEFAULT_INTERVAL_START_PRECISION = 2;
    public static final int DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION = 6;
    public static final int MIN_INTERVAL_START_PRECISION = 1;
    public static final int MIN_INTERVAL_FRACTIONAL_SECOND_PRECISION = 1;
    public static final int MAX_INTERVAL_START_PRECISION = 10;
    public static final int MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION = 9;

    // Cached map of enum values
    private static final Map<String, SqlTypeName> VALUES_MAP =
            Util.enumConstants(SqlTypeName.class);

    // categorizations used by SqlTypeFamily definitions

    // you probably want to use JDK 1.5 support for treating enumeration
    // as collection instead; this is only here to support
    // SqlTypeFamily.ANY
    public static final List<SqlTypeName> ALL_TYPES =
            ImmutableList.of(
                    BOOLEAN,
                    INTEGER,
                    VARCHAR,
                    DATE,
                    TIME,
                    TIMESTAMP,
                    NULL,
                    DECIMAL,
                    ANY,
                    CHAR,
                    BINARY,
                    VARBINARY,
                    TINYINT,
                    SMALLINT,
                    BIGINT,
                    REAL,
                    DOUBLE,
                    SYMBOL,
                    INTERVAL_YEAR,
                    INTERVAL_YEAR_MONTH,
                    INTERVAL_MONTH,
                    INTERVAL_DAY,
                    INTERVAL_DAY_HOUR,
                    INTERVAL_DAY_MINUTE,
                    INTERVAL_DAY_SECOND,
                    INTERVAL_HOUR,
                    INTERVAL_HOUR_MINUTE,
                    INTERVAL_HOUR_SECOND,
                    INTERVAL_MINUTE,
                    INTERVAL_MINUTE_SECOND,
                    INTERVAL_SECOND,
                    TIME_WITH_LOCAL_TIME_ZONE,
                    TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                    FLOAT,
                    MULTISET,
                    DISTINCT,
                    STRUCTURED,
                    ROW,
                    CURSOR,
                    COLUMN_LIST,
                    VARIANT);

    public static final List<SqlTypeName> BOOLEAN_TYPES = ImmutableList.of(BOOLEAN);

    public static final List<SqlTypeName> BINARY_TYPES = ImmutableList.of(BINARY, VARBINARY);

    public static final List<SqlTypeName> INT_TYPES =
            ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT);

    public static final List<SqlTypeName> EXACT_TYPES =
            combine(INT_TYPES, ImmutableList.of(DECIMAL));

    public static final List<SqlTypeName> APPROX_TYPES = ImmutableList.of(FLOAT, REAL, DOUBLE);

    public static final List<SqlTypeName> NUMERIC_TYPES = combine(EXACT_TYPES, APPROX_TYPES);

    public static final List<SqlTypeName> FRACTIONAL_TYPES =
            combine(APPROX_TYPES, ImmutableList.of(DECIMAL));

    public static final List<SqlTypeName> CHAR_TYPES = ImmutableList.of(CHAR, VARCHAR);

    public static final List<SqlTypeName> STRING_TYPES = combine(CHAR_TYPES, BINARY_TYPES);

    public static final List<SqlTypeName> GEOMETRY_TYPES = ImmutableList.of(GEOMETRY);

    public static final List<SqlTypeName> DATETIME_TYPES =
            ImmutableList.of(
                    DATE,
                    TIME,
                    TIME_WITH_LOCAL_TIME_ZONE,
                    TIMESTAMP,
                    TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    public static final Set<SqlTypeName> YEAR_INTERVAL_TYPES =
            Sets.immutableEnumSet(
                    SqlTypeName.INTERVAL_YEAR,
                    SqlTypeName.INTERVAL_YEAR_MONTH,
                    SqlTypeName.INTERVAL_MONTH);

    public static final Set<SqlTypeName> DAY_INTERVAL_TYPES =
            Sets.immutableEnumSet(
                    SqlTypeName.INTERVAL_DAY,
                    SqlTypeName.INTERVAL_DAY_HOUR,
                    SqlTypeName.INTERVAL_DAY_MINUTE,
                    SqlTypeName.INTERVAL_DAY_SECOND,
                    SqlTypeName.INTERVAL_HOUR,
                    SqlTypeName.INTERVAL_HOUR_MINUTE,
                    SqlTypeName.INTERVAL_HOUR_SECOND,
                    SqlTypeName.INTERVAL_MINUTE,
                    SqlTypeName.INTERVAL_MINUTE_SECOND,
                    SqlTypeName.INTERVAL_SECOND);

    public static final Set<SqlTypeName> INTERVAL_TYPES =
            Sets.immutableEnumSet(Iterables.concat(YEAR_INTERVAL_TYPES, DAY_INTERVAL_TYPES));

    /** The possible types of a time frame argument to a function such as {@code TIMESTAMP_DIFF}. */
    public static final Set<SqlTypeName> TIME_FRAME_TYPES =
            Sets.immutableEnumSet(Iterables.concat(INTERVAL_TYPES, ImmutableList.of(SYMBOL)));

    private static final Map<Integer, SqlTypeName> JDBC_TYPE_TO_NAME =
            ImmutableMap.<Integer, SqlTypeName>builder()
                    .put(Types.TINYINT, TINYINT)
                    .put(Types.SMALLINT, SMALLINT)
                    .put(Types.BIGINT, BIGINT)
                    .put(Types.INTEGER, INTEGER)
                    .put(Types.NUMERIC, DECIMAL) // REVIEW
                    .put(Types.DECIMAL, DECIMAL)
                    .put(Types.FLOAT, FLOAT)
                    .put(Types.REAL, REAL)
                    .put(Types.DOUBLE, DOUBLE)
                    .put(Types.CHAR, CHAR)
                    .put(Types.VARCHAR, VARCHAR)

                    // TODO: provide real support for these eventually
                    .put(ExtraSqlTypes.NCHAR, CHAR)
                    .put(ExtraSqlTypes.NVARCHAR, VARCHAR)

                    // TODO: additional types not yet supported. See ExtraSqlTypes.
                    // .put(Types.LONGVARCHAR, Longvarchar)
                    // .put(Types.CLOB, Clob)
                    // .put(Types.LONGVARBINARY, Longvarbinary)
                    // .put(Types.BLOB, Blob)
                    // .put(Types.LONGNVARCHAR, Longnvarchar)
                    // .put(Types.NCLOB, Nclob)
                    // .put(Types.ROWID, Rowid)
                    // .put(Types.SQLXML, Sqlxml)

                    .put(Types.BINARY, BINARY)
                    .put(Types.VARBINARY, VARBINARY)
                    .put(Types.DATE, DATE)
                    .put(Types.TIME, TIME)
                    .put(Types.TIMESTAMP, TIMESTAMP)
                    .put(Types.BIT, BOOLEAN)
                    .put(Types.BOOLEAN, BOOLEAN)
                    .put(Types.DISTINCT, DISTINCT)
                    .put(Types.STRUCT, STRUCTURED)
                    .put(Types.ARRAY, ARRAY)
                    .build();

    /** Bitwise-or of flags indicating allowable precision/scale combinations. */
    private final int signatures;

    /**
     * Returns true if not of a "pure" standard sql type. "Inpure" types are {@link #ANY}, {@link
     * #NULL} and {@link #SYMBOL}
     */
    private final boolean special;

    private final int jdbcOrdinal;
    private final @Nullable SqlTypeFamily family;

    SqlTypeName(int signatures, boolean special, int jdbcType, @Nullable SqlTypeFamily family) {
        this.signatures = signatures;
        this.special = special;
        this.jdbcOrdinal = jdbcType;
        this.family = family;
    }

    /**
     * Looks up a type name from its name.
     *
     * @return Type name, or null if not found
     */
    public static @Nullable SqlTypeName get(String name) {
        if (false) {
            // The following code works OK, but the spurious exceptions are
            // annoying.
            try {
                return SqlTypeName.valueOf(name);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return VALUES_MAP.get(name);
    }

    /**
     * Returns the SqlTypeName value whose name or {@link #getSpaceName()} matches the given name,
     * or throws {@link IllegalArgumentException}; never returns null.
     */
    public static SqlTypeName lookup(String tag) {
        String tag2 = tag.replace(' ', '_');
        return valueOf(tag2);
    }

    public boolean allowsNoPrecNoScale() {
        return (signatures & PrecScale.NO_NO) != 0;
    }

    public boolean allowsPrecNoScale() {
        return (signatures & PrecScale.YES_NO) != 0;
    }

    public boolean allowsPrec() {
        return allowsPrecScale(true, true) || allowsPrecScale(true, false);
    }

    public boolean allowsScale() {
        return allowsPrecScale(true, true);
    }

    /**
     * Returns whether this type can be specified with a given combination of precision and scale.
     * For example,
     *
     * <ul>
     *   <li><code>Varchar.allowsPrecScale(true, false)</code> returns <code>
     * true</code>, because the VARCHAR type allows a precision parameter, as in <code>VARCHAR(10)
     *       </code>.
     *   <li><code>Varchar.allowsPrecScale(true, true)</code> returns <code>
     * true</code>, because the VARCHAR type does not allow a precision and a scale parameter, as in
     *       <code>VARCHAR(10, 4)</code>.
     *   <li><code>allowsPrecScale(false, true)</code> returns <code>false</code> for every type.
     * </ul>
     *
     * @param precision Whether the precision/length field is part of the type specification
     * @param scale Whether the scale field is part of the type specification
     * @return Whether this combination of precision/scale is valid
     */
    public boolean allowsPrecScale(boolean precision, boolean scale) {
        int mask =
                precision
                        ? (scale ? PrecScale.YES_YES : PrecScale.YES_NO)
                        : (scale ? 0 : PrecScale.NO_NO);
        return (signatures & mask) != 0;
    }

    public boolean isSpecial() {
        return special;
    }

    /** Returns the ordinal from {@link Types} corresponding to this SqlTypeName. */
    public int getJdbcOrdinal() {
        return jdbcOrdinal;
    }

    private static List<SqlTypeName> combine(List<SqlTypeName> list0, List<SqlTypeName> list1) {
        return ImmutableList.<SqlTypeName>builder().addAll(list0).addAll(list1).build();
    }

    /**
     * Returns the default scale for this type if supported, otherwise -1 if scale is either
     * unsupported or must be specified explicitly.
     */
    public int getDefaultScale() {
        switch (this) {
            case DECIMAL:
                return 0;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
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
                return DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION;
            default:
                return -1;
        }
    }

    /**
     * Gets the SqlTypeFamily containing this SqlTypeName.
     *
     * @return containing family, or null for none (SYMBOL, DISTINCT, STRUCTURED, ROW, OTHER)
     */
    public @Nullable SqlTypeFamily getFamily() {
        return family;
    }

    /**
     * Gets the SqlTypeName corresponding to a JDBC type.
     *
     * @param jdbcType the JDBC type of interest
     * @return corresponding SqlTypeName, or null if the type is not known
     */
    public static @Nullable SqlTypeName getNameForJdbcType(int jdbcType) {
        return JDBC_TYPE_TO_NAME.get(jdbcType);
    }

    /**
     * Returns the limit of this datatype. For example,
     *
     * <table border="1">
     * <caption>Datatype limits</caption>
     * <tr>
     * <th>Datatype</th>
     * <th>sign</th>
     * <th>limit</th>
     * <th>beyond</th>
     * <th>precision</th>
     * <th>scale</th>
     * <th>Returns</th>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>true</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>false</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>-2147483648 (-2 ^ 31 = MININT)</td>
     * </tr>
     * <tr>
     * <td>Boolean</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>TRUE</td>
     * </tr>
     * <tr>
     * <td>Varchar</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>10</td>
     * <td>-1</td>
     * <td>'ZZZZZZZZZZ'</td>
     * </tr>
     * </table>
     *
     * @param sign If true, returns upper limit, otherwise lower limit
     * @param limit If true, returns value at or near to overflow; otherwise value at or near to
     *     underflow
     * @param beyond If true, returns the value just beyond the limit, otherwise the value at the
     *     limit
     * @param precision Precision, or -1 if not applicable
     * @param scale Scale, or -1 if not applicable
     * @return Limit value
     */
    public @Nullable Object getLimit(
            boolean sign, Limit limit, boolean beyond, int precision, int scale) {
        assert allowsPrecScale(precision != -1, scale != -1) : this;
        if (limit == Limit.ZERO) {
            if (beyond) {
                return null;
            }
            sign = true;
        }
        Calendar calendar;

        switch (this) {
            case BOOLEAN:
                switch (limit) {
                    case ZERO:
                        return false;
                    case UNDERFLOW:
                        return null;
                    case OVERFLOW:
                        if (beyond || !sign) {
                            return null;
                        } else {
                            return true;
                        }
                    default:
                        throw Util.unexpected(limit);
                }

            case TINYINT:
                return getNumericLimit(2, 8, sign, limit, beyond);

            case SMALLINT:
                return getNumericLimit(2, 16, sign, limit, beyond);

            case INTEGER:
                return getNumericLimit(2, 32, sign, limit, beyond);

            case BIGINT:
                return getNumericLimit(2, 64, sign, limit, beyond);

            case DECIMAL:
                BigDecimal decimal = getNumericLimit(10, precision, sign, limit, beyond);
                if (decimal == null) {
                    return null;
                }

                // Decimal values must fit into 64 bits. So, the maximum value of
                // a DECIMAL(19, 0) is 2^63 - 1, not 10^19 - 1.
                switch (limit) {
                    case OVERFLOW:
                        final BigDecimal other =
                                (BigDecimal) BIGINT.getLimit(sign, limit, beyond, -1, -1);
                        if (other != null && decimal.compareTo(other) == (sign ? 1 : -1)) {
                            decimal = other;
                        }
                        break;
                    default:
                        break;
                }

                // Apply scale.
                if (scale == 0) {
                    // do nothing
                } else if (scale > 0) {
                    decimal = decimal.divide(BigDecimal.TEN.pow(scale));
                } else {
                    decimal = decimal.multiply(BigDecimal.TEN.pow(-scale));
                }
                return decimal;

            case CHAR:
            case VARCHAR:
                if (!sign) {
                    return null; // this type does not have negative values
                }
                StringBuilder buf = new StringBuilder();
                switch (limit) {
                    case ZERO:
                        break;
                    case UNDERFLOW:
                        if (beyond) {
                            // There is no value between the empty string and the
                            // smallest non-empty string.
                            return null;
                        }
                        buf.append("a");
                        break;
                    case OVERFLOW:
                        for (int i = 0; i < precision; ++i) {
                            buf.append("Z");
                        }
                        if (beyond) {
                            buf.append("Z");
                        }
                        break;
                    default:
                        break;
                }
                return buf.toString();

            case BINARY:
            case VARBINARY:
                if (!sign) {
                    return null; // this type does not have negative values
                }
                byte[] bytes;
                switch (limit) {
                    case ZERO:
                        bytes = new byte[0];
                        break;
                    case UNDERFLOW:
                        if (beyond) {
                            // There is no value between the empty string and the
                            // smallest value.
                            return null;
                        }
                        bytes = new byte[] {0x00};
                        break;
                    case OVERFLOW:
                        bytes = new byte[precision + (beyond ? 1 : 0)];
                        Arrays.fill(bytes, (byte) 0xff);
                        break;
                    default:
                        throw Util.unexpected(limit);
                }
                return bytes;

            case DATE:
                calendar = Util.calendar();
                switch (limit) {
                    case ZERO:
                        // The epoch.
                        calendar.set(Calendar.YEAR, 1970);
                        calendar.set(Calendar.MONTH, 0);
                        calendar.set(Calendar.DAY_OF_MONTH, 1);
                        break;
                    case UNDERFLOW:
                        return null;
                    case OVERFLOW:
                        if (beyond) {
                            // It is impossible to represent an invalid year as a date
                            // literal. SQL dates are represented as 'yyyy-mm-dd', and
                            // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                            // before 1AD is 1BC, so SimpleDateFormat renders the day
                            // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                            // like a valid date.
                            return null;
                        }

                        // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                        // between 1 and 9999, and days/months are the valid Gregorian
                        // calendar values for these years.
                        if (sign) {
                            calendar.set(Calendar.YEAR, 9999);
                            calendar.set(Calendar.MONTH, 11);
                            calendar.set(Calendar.DAY_OF_MONTH, 31);
                        } else {
                            calendar.set(Calendar.YEAR, 1);
                            calendar.set(Calendar.MONTH, 0);
                            calendar.set(Calendar.DAY_OF_MONTH, 1);
                        }
                        break;
                    default:
                        break;
                }
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                return calendar;

            case TIME:
                if (!sign) {
                    return null; // this type does not have negative values
                }
                if (beyond) {
                    return null; // invalid values are impossible to represent
                }
                calendar = Util.calendar();
                switch (limit) {
                    case ZERO:

                        // The epoch.
                        calendar.set(Calendar.HOUR_OF_DAY, 0);
                        calendar.set(Calendar.MINUTE, 0);
                        calendar.set(Calendar.SECOND, 0);
                        calendar.set(Calendar.MILLISECOND, 0);
                        break;
                    case UNDERFLOW:
                        return null;
                    case OVERFLOW:
                        calendar.set(Calendar.HOUR_OF_DAY, 23);
                        calendar.set(Calendar.MINUTE, 59);
                        calendar.set(Calendar.SECOND, 59);
                        int millis =
                                (precision >= 3)
                                        ? 999
                                        : ((precision == 2) ? 990 : ((precision == 1) ? 900 : 0));
                        calendar.set(Calendar.MILLISECOND, millis);
                        break;
                    default:
                        break;
                }
                return calendar;

            case TIMESTAMP:
                calendar = Util.calendar();
                switch (limit) {
                    case ZERO:
                        // The epoch.
                        calendar.set(Calendar.YEAR, 1970);
                        calendar.set(Calendar.MONTH, 0);
                        calendar.set(Calendar.DAY_OF_MONTH, 1);
                        calendar.set(Calendar.HOUR_OF_DAY, 0);
                        calendar.set(Calendar.MINUTE, 0);
                        calendar.set(Calendar.SECOND, 0);
                        calendar.set(Calendar.MILLISECOND, 0);
                        break;
                    case UNDERFLOW:
                        return null;
                    case OVERFLOW:
                        if (beyond) {
                            // It is impossible to represent an invalid year as a date
                            // literal. SQL dates are represented as 'yyyy-mm-dd', and
                            // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                            // before 1AD is 1BC, so SimpleDateFormat renders the day
                            // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                            // like a valid date.
                            return null;
                        }

                        // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                        // between 1 and 9999, and days/months are the valid Gregorian
                        // calendar values for these years.
                        if (sign) {
                            calendar.set(Calendar.YEAR, 9999);
                            calendar.set(Calendar.MONTH, 11);
                            calendar.set(Calendar.DAY_OF_MONTH, 31);
                            calendar.set(Calendar.HOUR_OF_DAY, 23);
                            calendar.set(Calendar.MINUTE, 59);
                            calendar.set(Calendar.SECOND, 59);
                            int millis =
                                    (precision >= 3)
                                            ? 999
                                            : ((precision == 2)
                                                    ? 990
                                                    : ((precision == 1) ? 900 : 0));
                            calendar.set(Calendar.MILLISECOND, millis);
                        } else {
                            calendar.set(Calendar.YEAR, 1);
                            calendar.set(Calendar.MONTH, 0);
                            calendar.set(Calendar.DAY_OF_MONTH, 1);
                            calendar.set(Calendar.HOUR_OF_DAY, 0);
                            calendar.set(Calendar.MINUTE, 0);
                            calendar.set(Calendar.SECOND, 0);
                            calendar.set(Calendar.MILLISECOND, 0);
                        }
                        break;
                    default:
                        break;
                }
                return calendar;

            default:
                throw Util.unexpected(this);
        }
    }

    /**
     * Returns the minimum precision (or length) allowed for this type, or -1 if precision/length
     * are not applicable for this type.
     *
     * @return Minimum allowed precision
     */
    public int getMinPrecision() {
        switch (this) {
            case DECIMAL:
            case VARCHAR:
            case CHAR:
            case VARBINARY:
            case BINARY:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 1;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
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
                return MIN_INTERVAL_START_PRECISION;
            default:
                return -1;
        }
    }

    /**
     * Returns the minimum scale (or fractional second precision in the case of intervals) allowed
     * for this type, or -1 if precision/length are not applicable for this type.
     *
     * @return Minimum allowed scale
     */
    public int getMinScale() {
        switch (this) {
            // TODO: Minimum numeric scale for decimal
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
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
                return MIN_INTERVAL_FRACTIONAL_SECOND_PRECISION;
            default:
                return -1;
        }
    }

    /**
     * Returns {@code HOUR} for {@code HOUR TO SECOND} and {@code HOUR}, {@code SECOND} for {@code
     * SECOND}.
     */
    public TimeUnit getStartUnit() {
        switch (this) {
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
                return TimeUnit.YEAR;
            case INTERVAL_MONTH:
                return TimeUnit.MONTH;
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
                return TimeUnit.DAY;
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
                return TimeUnit.HOUR;
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
                return TimeUnit.MINUTE;
            case INTERVAL_SECOND:
                return TimeUnit.SECOND;
            default:
                throw new AssertionError(this);
        }
    }

    /** Returns {@code SECOND} for both {@code HOUR TO SECOND} and {@code SECOND}. */
    public TimeUnit getEndUnit() {
        switch (this) {
            case INTERVAL_YEAR:
                return TimeUnit.YEAR;
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return TimeUnit.MONTH;
            case INTERVAL_DAY:
                return TimeUnit.DAY;
            case INTERVAL_DAY_HOUR:
            case INTERVAL_HOUR:
                return TimeUnit.HOUR;
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_MINUTE:
                return TimeUnit.MINUTE;
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return TimeUnit.SECOND;
            default:
                throw new AssertionError(this);
        }
    }

    public boolean isYearMonth() {
        switch (this) {
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return true;
            default:
                return false;
        }
    }

    /** Limit. */
    public enum Limit {
        ZERO,
        UNDERFLOW,
        OVERFLOW
    }

    private static @Nullable BigDecimal getNumericLimit(
            int radix, int exponent, boolean sign, Limit limit, boolean beyond) {
        switch (limit) {
            case OVERFLOW:

                // 2-based schemes run from -2^(N-1) to 2^(N-1)-1 e.g. -128 to +127
                // 10-based schemas run from -(10^N-1) to 10^N-1 e.g. -99 to +99
                final BigDecimal bigRadix = BigDecimal.valueOf(radix);
                if (radix == 2) {
                    --exponent;
                }
                BigDecimal decimal = bigRadix.pow(exponent);
                if (sign || (radix != 2)) {
                    decimal = decimal.subtract(BigDecimal.ONE);
                }
                if (beyond) {
                    decimal = decimal.add(BigDecimal.ONE);
                }
                if (!sign) {
                    decimal = decimal.negate();
                }
                return decimal;
            case UNDERFLOW:
                return beyond ? null : (sign ? BigDecimal.ONE : BigDecimal.ONE.negate());
            case ZERO:
                return BigDecimal.ZERO;
            default:
                throw Util.unexpected(limit);
        }
    }

    public SqlLiteral createLiteral(Object o, SqlParserPos pos) {
        switch (this) {
            case BOOLEAN:
                return SqlLiteral.createBoolean((Boolean) o, pos);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return SqlLiteral.createExactNumeric(o.toString(), pos);
            case VARCHAR:
            case CHAR:
                return SqlLiteral.createCharString((String) o, pos);
            case VARBINARY:
            case BINARY:
                return SqlLiteral.createBinaryString((byte[]) o, pos);
            case DATE:
                return SqlLiteral.createDate(
                        o instanceof Calendar
                                ? DateString.fromCalendarFields((Calendar) o)
                                : (DateString) o,
                        pos);
            case TIME:
                return SqlLiteral.createTime(
                        o instanceof Calendar
                                ? TimeString.fromCalendarFields((Calendar) o)
                                : (TimeString) o,
                        0 /* todo */,
                        pos);
            case TIMESTAMP:
                return SqlLiteral.createTimestamp(
                        this,
                        o instanceof Calendar
                                ? TimestampString.fromCalendarFields((Calendar) o)
                                : (TimestampString) o,
                        0 /* todo */,
                        pos);
            default:
                throw Util.unexpected(this);
        }
    }

    /** Returns the name of this type. */
    public String getName() {
        return name();
    }

    /**
     * Returns the name of this type, with underscores converted to spaces, for example "TIMESTAMP
     * WITH LOCAL TIME ZONE", "DATE".
     */
    public String getSpaceName() {
        return name().replace('_', ' ');
    }

    /**
     * Flags indicating precision/scale combinations.
     *
     * <p>Note: for intervals:
     *
     * <ul>
     *   <li>precision = start (leading field) precision
     *   <li>scale = fractional second precision
     * </ul>
     */
    private interface PrecScale {
        int NO_NO = 1;
        int YES_NO = 2;
        int YES_YES = 4;
    }
}
