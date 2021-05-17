/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rex;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Constant value in a row-expression.
 *
 * <p>There are several methods for creating literals in {@link RexBuilder}: {@link
 * RexBuilder#makeLiteral(boolean)} and so forth.
 *
 * <p>How is the value stored? In that respect, the class is somewhat of a black box. There is a
 * {@link #getValue} method which returns the value as an object, but the type of that value is
 * implementation detail, and it is best that your code does not depend upon that knowledge. It is
 * better to use task-oriented methods such as {@link #getValue2} and {@link #toJavaString}.
 *
 * <p>The allowable types and combinations are:
 *
 * <table>
 * <caption>Allowable types for RexLiteral instances</caption>
 * <tr>
 * <th>TypeName</th>
 * <th>Meaning</th>
 * <th>Value type</th>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#NULL}</td>
 * <td>The null value. It has its own special type.</td>
 * <td>null</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#BOOLEAN}</td>
 * <td>Boolean, namely <code>TRUE</code>, <code>FALSE</code> or <code>
 * UNKNOWN</code>.</td>
 * <td>{@link Boolean}, or null represents the UNKNOWN value</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DECIMAL}</td>
 * <td>Exact number, for example <code>0</code>, <code>-.5</code>, <code>
 * 12345</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DOUBLE}</td>
 * <td>Approximate number, for example <code>6.023E-23</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DATE}</td>
 * <td>Date, for example <code>DATE '1969-04'29'</code></td>
 * <td>{@link Calendar};
 *     also {@link Calendar} (UTC time zone)
 *     and {@link Integer} (days since POSIX epoch)</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#TIME}</td>
 * <td>Time, for example <code>TIME '18:37:42.567'</code></td>
 * <td>{@link Calendar};
 *     also {@link Calendar} (UTC time zone)
 *     and {@link Integer} (milliseconds since midnight)</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#TIMESTAMP}</td>
 * <td>Timestamp, for example <code>TIMESTAMP '1969-04-29
 * 18:37:42.567'</code></td>
 * <td>{@link TimestampString};
 *     also {@link Calendar} (UTC time zone)
 *     and {@link Long} (milliseconds since POSIX epoch)</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#INTERVAL_DAY},
 *     {@link SqlTypeName#INTERVAL_DAY_HOUR},
 *     {@link SqlTypeName#INTERVAL_DAY_MINUTE},
 *     {@link SqlTypeName#INTERVAL_DAY_SECOND},
 *     {@link SqlTypeName#INTERVAL_HOUR},
 *     {@link SqlTypeName#INTERVAL_HOUR_MINUTE},
 *     {@link SqlTypeName#INTERVAL_HOUR_SECOND},
 *     {@link SqlTypeName#INTERVAL_MINUTE},
 *     {@link SqlTypeName#INTERVAL_MINUTE_SECOND},
 *     {@link SqlTypeName#INTERVAL_SECOND}</td>
 * <td>Interval, for example <code>INTERVAL '4:3:2' HOUR TO SECOND</code></td>
 * <td>{@link BigDecimal};
 *     also {@link Long} (milliseconds)</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#INTERVAL_YEAR},
 *     {@link SqlTypeName#INTERVAL_YEAR_MONTH},
 *     {@link SqlTypeName#INTERVAL_MONTH}</td>
 * <td>Interval, for example <code>INTERVAL '2-3' YEAR TO MONTH</code></td>
 * <td>{@link BigDecimal};
 *     also {@link Integer} (months)</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#CHAR}</td>
 * <td>Character constant, for example <code>'Hello, world!'</code>, <code>
 * ''</code>, <code>_N'Bonjour'</code>, <code>_ISO-8859-1'It''s superman!'
 * COLLATE SHIFT_JIS$ja_JP$2</code>. These are always CHAR, never VARCHAR.</td>
 * <td>{@link NlsString};
 *     also {@link String}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#BINARY}</td>
 * <td>Binary constant, for example <code>X'7F34'</code>. (The number of hexits
 * must be even; see above.) These constants are always BINARY, never
 * VARBINARY.</td>
 * <td>{@link ByteBuffer};
 *     also {@code byte[]}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#SYMBOL}</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of
 * the SQL standard, and is not exposed to end-users. It is used to hold a flag,
 * such as the LEADING flag in a call to the function <code>
 * TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.</td>
 * <td>An enum class</td>
 * </tr>
 * </table>
 *
 * <p>Line 374 ~ 376: fix FLINK-20942, the file should be removed when upgrade to CALCITE 1.27.0
 * release.
 */
public class RexLiteral extends RexNode {
    // ~ Instance fields --------------------------------------------------------

    /**
     * The value of this literal. Must be consistent with its type, as per {@link
     * #valueMatchesType}. For example, you can't store an {@link Integer} value here just because
     * you feel like it -- all numbers are represented by a {@link BigDecimal}. But since this field
     * is private, it doesn't really matter how the values are stored.
     */
    private final Comparable value;

    /** The real type of this literal, as reported by {@link #getType}. */
    private final RelDataType type;

    // TODO jvs 26-May-2006:  Use SqlTypeFamily instead; it exists
    // for exactly this purpose (to avoid the confusion which results
    // from overloading SqlTypeName).
    /**
     * An indication of the broad type of this literal -- even if its type isn't a SQL type.
     * Sometimes this will be different than the SQL type; for example, all exact numbers, including
     * integers have typeName {@link SqlTypeName#DECIMAL}. See {@link #valueMatchesType} for the
     * definitive story.
     */
    private final SqlTypeName typeName;

    private static final ImmutableList<TimeUnit> TIME_UNITS =
            ImmutableList.copyOf(TimeUnit.values());

    // ~ Constructors -----------------------------------------------------------

    /** Creates a <code>RexLiteral</code>. */
    RexLiteral(Comparable value, RelDataType type, SqlTypeName typeName) {
        this.value = value;
        this.type = Objects.requireNonNull(type);
        this.typeName = Objects.requireNonNull(typeName);
        Preconditions.checkArgument(valueMatchesType(value, typeName, true));
        Preconditions.checkArgument((value == null) == type.isNullable());
        Preconditions.checkArgument(typeName != SqlTypeName.ANY);
        this.digest = computeDigest(RexDigestIncludeType.OPTIONAL);
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * Returns a string which concisely describes the definition of this rex literal. Two literals
     * are equivalent if and only if their digests are the same.
     *
     * <p>The digest does not contain the expression's identity, but does include the identity of
     * children.
     *
     * <p>Technically speaking 1:INT differs from 1:FLOAT, so we need data type in the literal's
     * digest, however we want to avoid extra verbosity of the {@link RelNode#getDigest()} for
     * readability purposes, so we omit type info in certain cases. For instance, 1:INT becomes 1
     * (INT is implied by default), however 1:BIGINT always holds the type
     *
     * <p>Here's a non-exhaustive list of the "well known cases":
     *
     * <ul>
     *   <li>Hide "NOT NULL" for not null literals
     *   <li>Hide INTEGER, BOOLEAN, SYMBOL, TIME(0), TIMESTAMP(0), DATE(0) types
     *   <li>Hide collation when it matches IMPLICIT/COERCIBLE
     *   <li>Hide charset when it matches default
     *   <li>Hide CHAR(xx) when literal length is equal to the precision of the type. In other
     *       words, use 'Bob' instead of 'Bob':CHAR(3)
     *   <li>Hide BOOL for AND/OR arguments. In other words, AND(true, null) means null is BOOL.
     *   <li>Hide types for literals in simple binary operations (e.g. +, -, *, /, comparison) when
     *       type of the other argument is clear. See {@link RexCall#computeDigest(boolean)} For
     *       instance: =(true. null) means null is BOOL. =($0, null) means the type of null matches
     *       the type of $0.
     * </ul>
     *
     * @param includeType whether the digest should include type or not
     * @return digest
     */
    public final String computeDigest(RexDigestIncludeType includeType) {
        if (includeType == RexDigestIncludeType.OPTIONAL) {
            if (digest != null) {
                // digest is initialized with OPTIONAL, so cached value matches for
                // includeType=OPTIONAL as well
                return digest;
            }
            // Compute we should include the type or not
            includeType = digestIncludesType();
        } else if (digest != null && includeType == digestIncludesType()) {
            // The digest is always computed with includeType=OPTIONAL
            // If it happened to omit the type, we want to optimize computeDigest(NO_TYPE) as well
            // If the digest includes the type, we want to optimize computeDigest(ALWAYS)
            return digest;
        }

        return toJavaString(value, typeName, type, includeType);
    }

    /**
     * Returns true if {@link RexDigestIncludeType#OPTIONAL} digest would include data type.
     *
     * @see RexCall#computeDigest(boolean)
     * @return true if {@link RexDigestIncludeType#OPTIONAL} digest would include data type
     */
    RexDigestIncludeType digestIncludesType() {
        return shouldIncludeType(value, type);
    }

    /** Returns whether a value is appropriate for its type. (We have rules about these things!) */
    public static boolean valueMatchesType(Comparable value, SqlTypeName typeName, boolean strict) {
        if (value == null) {
            return true;
        }
        switch (typeName) {
            case BOOLEAN:
                // Unlike SqlLiteral, we do not allow boolean null.
                return value instanceof Boolean;
            case NULL:
                return false; // value should have been null
            case INTEGER: // not allowed -- use Decimal
            case TINYINT:
            case SMALLINT:
                if (strict) {
                    throw Util.unexpected(typeName);
                }
                // fall through
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case REAL:
            case BIGINT:
                return value instanceof BigDecimal;
            case DATE:
                return value instanceof DateString;
            case TIME:
                return value instanceof TimeString;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return value instanceof TimeString;
            case TIMESTAMP:
                return value instanceof TimestampString;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return value instanceof TimestampString;
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
                // The value of a DAY-TIME interval (whatever the start and end units,
                // even say HOUR TO MINUTE) is in milliseconds (perhaps fractional
                // milliseconds). The value of a YEAR-MONTH interval is in months.
                return value instanceof BigDecimal;
            case VARBINARY: // not allowed -- use Binary
                if (strict) {
                    throw Util.unexpected(typeName);
                }
                // fall through
            case BINARY:
                return value instanceof ByteString;
            case VARCHAR: // not allowed -- use Char
                if (strict) {
                    throw Util.unexpected(typeName);
                }
                // fall through
            case CHAR:
                // A SqlLiteral's charset and collation are optional; not so a
                // RexLiteral.
                return (value instanceof NlsString)
                        && (((NlsString) value).getCharset() != null)
                        && (((NlsString) value).getCollation() != null);
            case SARG:
                return value instanceof Sarg;
            case SYMBOL:
                return value instanceof Enum;
            case ROW:
            case MULTISET:
                return value instanceof List;
            case GEOMETRY:
                return value instanceof Geometries.Geom;
            case ANY:
                // Literal of type ANY is not legal. "CAST(2 AS ANY)" remains
                // an integer literal surrounded by a cast function.
                return false;
            default:
                throw Util.unexpected(typeName);
        }
    }

    /** Returns the strict literal type for a given type. */
    public static SqlTypeName strictTypeName(RelDataType type) {
        final SqlTypeName typeName = type.getSqlTypeName();
        switch (typeName) {
            case INTEGER:
            case TINYINT:
            case SMALLINT:
                return SqlTypeName.DECIMAL;
            case REAL:
            case FLOAT:
                return SqlTypeName.DOUBLE;
            case VARBINARY:
                return SqlTypeName.BINARY;
            case VARCHAR:
                return SqlTypeName.CHAR;
            default:
                return typeName;
        }
    }

    private static String toJavaString(
            Comparable value,
            SqlTypeName typeName,
            RelDataType type,
            RexDigestIncludeType includeType) {
        assert includeType != RexDigestIncludeType.OPTIONAL
                : "toJavaString must not be called with includeType=OPTIONAL";
        if (value == null) {
            return includeType == RexDigestIncludeType.NO_TYPE
                    ? "null"
                    : "null:" + type.getFullTypeString();
        }
        StringBuilder sb = new StringBuilder();
        appendAsJava(value, sb, typeName, type, false, includeType);

        if (includeType != RexDigestIncludeType.NO_TYPE) {
            sb.append(':');
            final String fullTypeString = type.getFullTypeString();
            if (!fullTypeString.endsWith("NOT NULL")) {
                sb.append(fullTypeString);
            } else {
                // Trim " NOT NULL". Apparently, the literal is not null, so we just print the data
                // type.
                sb.append(fullTypeString, 0, fullTypeString.length() - 9);
            }
        }
        return sb.toString();
    }

    /**
     * Computes if data type can be omitted from the digset.
     *
     * <p>For instance, {@code 1:BIGINT} has to keep data type while {@code 1:INT} should be
     * represented as just {@code 1}.
     *
     * <p>Implementation assumption: this method should be fast. In fact might call {@link
     * NlsString#getValue()} which could decode the string, however we rely on the cache there.
     *
     * @see RexLiteral#computeDigest(RexDigestIncludeType)
     * @param value value of the literal
     * @param type type of the literal
     * @return NO_TYPE when type can be omitted, ALWAYS otherwise
     */
    private static RexDigestIncludeType shouldIncludeType(Comparable value, RelDataType type) {
        if (type.isNullable()) {
            // This means "null literal", so we require a type for it
            // There might be exceptions like AND(null, true) which are handled by
            // RexCall#computeDigest
            return RexDigestIncludeType.ALWAYS;
        }
        // The variable here simplifies debugging (one can set a breakpoint at return)
        // final ensures we set the value in all the branches, and it ensures the value is set just
        // once
        final RexDigestIncludeType includeType;
        if (type.getSqlTypeName() == SqlTypeName.BOOLEAN
                || type.getSqlTypeName() == SqlTypeName.INTEGER
                || type.getSqlTypeName() == SqlTypeName.SYMBOL) {
            // We don't want false:BOOLEAN NOT NULL, so we don't print type information for
            // non-nullable BOOLEAN and INTEGER
            includeType = RexDigestIncludeType.NO_TYPE;
        } else if (type.getSqlTypeName() == SqlTypeName.CHAR && value instanceof NlsString) {
            NlsString nlsString = (NlsString) value;

            // Ignore type information for 'Bar':CHAR(3)
            if (((nlsString.getCharset() != null
                                    && type.getCharset().equals(nlsString.getCharset()))
                            || (nlsString.getCharset() == null
                                    && SqlCollation.IMPLICIT
                                            .getCharset()
                                            .equals(type.getCharset())))
                    && nlsString.getCollation().equals(type.getCollation())
                    && ((NlsString) value).getValue().length() == type.getPrecision()) {
                includeType = RexDigestIncludeType.NO_TYPE;
            } else {
                includeType = RexDigestIncludeType.ALWAYS;
            }
        } else if (type.getPrecision() == 0
                && (type.getSqlTypeName() == SqlTypeName.TIME
                        || type.getSqlTypeName() == SqlTypeName.TIMESTAMP
                        || type.getSqlTypeName() == SqlTypeName.DATE)) {
            // Ignore type information for '12:23:20':TIME(0)
            // Note that '12:23:20':TIME WITH LOCAL TIME ZONE
            includeType = RexDigestIncludeType.NO_TYPE;
        } else {
            includeType = RexDigestIncludeType.ALWAYS;
        }
        return includeType;
    }

    /**
     * Returns whether a value is valid as a constant value, using the same criteria as {@link
     * #valueMatchesType}.
     */
    public static boolean validConstant(Object o, Litmus litmus) {
        if (o == null
                || o instanceof BigDecimal
                || o instanceof NlsString
                || o instanceof ByteString
                || o instanceof Boolean) {
            return litmus.succeed();
        } else if (o instanceof List) {
            List list = (List) o;
            for (Object o1 : list) {
                if (!validConstant(o1, litmus)) {
                    return litmus.fail("not a constant: {}", o1);
                }
            }
            return litmus.succeed();
        } else if (o instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<Object, Object> map = (Map) o;
            for (Map.Entry entry : map.entrySet()) {
                if (!validConstant(entry.getKey(), litmus)) {
                    return litmus.fail("not a constant: {}", entry.getKey());
                }
                if (!validConstant(entry.getValue(), litmus)) {
                    return litmus.fail("not a constant: {}", entry.getValue());
                }
            }
            return litmus.succeed();
        } else {
            return litmus.fail("not a constant: {}", o);
        }
    }

    /**
     * Returns a list of the time units covered by an interval type such as HOUR TO SECOND. Adds
     * MILLISECOND if the end is SECOND, to deal with fractional seconds.
     */
    private static List<TimeUnit> getTimeUnits(SqlTypeName typeName) {
        final TimeUnit start = typeName.getStartUnit();
        final TimeUnit end = typeName.getEndUnit();
        final ImmutableList<TimeUnit> list = TIME_UNITS.subList(start.ordinal(), end.ordinal() + 1);
        if (end == TimeUnit.SECOND) {
            return CompositeList.of(list, ImmutableList.of(TimeUnit.MILLISECOND));
        }
        return list;
    }

    private String intervalString(BigDecimal v) {
        final List<TimeUnit> timeUnits = getTimeUnits(type.getSqlTypeName());
        final StringBuilder b = new StringBuilder();
        for (TimeUnit timeUnit : timeUnits) {
            final BigDecimal[] result = v.divideAndRemainder(timeUnit.multiplier);
            if (b.length() > 0) {
                b.append(timeUnit.separator);
            }
            final int width = b.length() == 0 ? -1 : width(timeUnit); // don't pad 1st
            pad(b, result[0].toString(), width);
            v = result[1];
        }
        if (Util.last(timeUnits) == TimeUnit.MILLISECOND) {
            while (b.toString().matches(".*\\.[0-9]*0")) {
                if (b.toString().endsWith(".0")) {
                    b.setLength(b.length() - 2); // remove ".0"
                } else {
                    b.setLength(b.length() - 1); // remove "0"
                }
            }
        }
        return b.toString();
    }

    private static void pad(StringBuilder b, String s, int width) {
        if (width >= 0) {
            for (int i = s.length(); i < width; i++) {
                b.append('0');
            }
        }
        b.append(s);
    }

    private static int width(TimeUnit timeUnit) {
        switch (timeUnit) {
            case MILLISECOND:
                return 3;
            case HOUR:
            case MINUTE:
            case SECOND:
                return 2;
            default:
                return -1;
        }
    }

    /** Prints the value this literal as a Java string constant. */
    public void printAsJava(PrintWriter pw) {
        Util.asStringBuilder(
                pw,
                sb -> appendAsJava(value, sb, typeName, type, true, RexDigestIncludeType.NO_TYPE));
    }

    /**
     * Appends the specified value in the provided destination as a Java string. The value must be
     * consistent with the type, as per {@link #valueMatchesType}.
     *
     * <p>Typical return values:
     *
     * <ul>
     *   <li>true
     *   <li>null
     *   <li>"Hello, world!"
     *   <li>1.25
     *   <li>1234ABCD
     * </ul>
     *
     * @param value Value to be appended to the provided destination as a Java string
     * @param sb Destination to which to append the specified value
     * @param typeName Type name to be used for the transformation of the value to a Java string
     * @param type Type to be used for the transformation of the value to a Java string
     * @param includeType Whether to include the data type in the Java representation
     */
    private static void appendAsJava(
            Comparable value,
            StringBuilder sb,
            SqlTypeName typeName,
            RelDataType type,
            boolean java,
            RexDigestIncludeType includeType) {
        switch (typeName) {
            case CHAR:
                NlsString nlsString = (NlsString) value;
                if (java) {
                    Util.printJavaString(sb, nlsString.getValue(), true);
                } else {
                    boolean includeCharset =
                            (nlsString.getCharsetName() != null)
                                    && !nlsString
                                            .getCharsetName()
                                            .equals(CalciteSystemProperty.DEFAULT_CHARSET.value());
                    sb.append(nlsString.asSql(includeCharset, false));
                }
                break;
            case BOOLEAN:
                assert value instanceof Boolean;
                sb.append(value.toString());
                break;
            case DECIMAL:
                assert value instanceof BigDecimal;
                sb.append(value.toString());
                break;
            case DOUBLE:
                assert value instanceof BigDecimal;
                sb.append(Util.toScientificNotation((BigDecimal) value));
                break;
            case BIGINT:
                assert value instanceof BigDecimal;
                long narrowLong = ((BigDecimal) value).longValue();
                sb.append(String.valueOf(narrowLong));
                sb.append('L');
                break;
            case BINARY:
                assert value instanceof ByteString;
                sb.append("X'");
                sb.append(((ByteString) value).toString(16));
                sb.append("'");
                break;
            case NULL:
                assert value == null;
                sb.append("null");
                break;
            case SARG:
                assert value instanceof Sarg;
                //noinspection unchecked,rawtypes
                Util.asStringBuilder(sb, sb2 -> printSarg(sb2, (Sarg) value, type));
                break;
            case SYMBOL:
                assert value instanceof Enum;
                sb.append("FLAG(");
                sb.append(value.toString());
                sb.append(")");
                break;
            case DATE:
                assert value instanceof DateString;
                sb.append(value.toString());
                break;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                assert value instanceof TimeString;
                sb.append(value.toString());
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                assert value instanceof TimestampString;
                sb.append(value.toString());
                break;
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
                assert value instanceof BigDecimal;
                sb.append(value.toString());
                break;
            case MULTISET:
            case ROW:
                final List<RexLiteral> list = (List) value;
                Util.asStringBuilder(
                        sb,
                        sb2 ->
                                Util.printList(
                                        sb,
                                        list.size(),
                                        (sb3, i) ->
                                                sb3.append(
                                                        list.get(i).computeDigest(includeType))));
                break;
            case GEOMETRY:
                final String wkt = GeoFunctions.ST_AsWKT((Geometries.Geom) value);
                sb.append(wkt);
                break;
            default:
                assert valueMatchesType(value, typeName, true);
                throw Util.needToImplement(typeName);
        }
    }

    private static <C extends Comparable<C>> void printSarg(
            StringBuilder sb, Sarg<C> sarg, RelDataType type) {
        sarg.printTo(sb, (sb2, value) -> sb2.append(toLiteral(type, value)));
    }

    /**
     * Converts a value to a temporary literal, for the purposes of generating a digest. Literals of
     * type ROW and MULTISET require that their components are also literals.
     */
    private static RexLiteral toLiteral(RelDataType type, Comparable<?> value) {
        final SqlTypeName typeName = strictTypeName(type);
        switch (typeName) {
            case ROW:
                final List<Comparable<?>> fieldValues = (List) value;
                final List<RelDataTypeField> fields = type.getFieldList();
                final List<RexLiteral> fieldLiterals =
                        FlatLists.of(
                                Functions.generate(
                                        fieldValues.size(),
                                        i ->
                                                toLiteral(
                                                        fields.get(i).getType(),
                                                        fieldValues.get(i))));
                return new RexLiteral((Comparable) fieldLiterals, type, typeName);

            case MULTISET:
                final List<Comparable<?>> elementValues = (List) value;
                final List<RexLiteral> elementLiterals =
                        FlatLists.of(
                                Functions.generate(
                                        elementValues.size(),
                                        i ->
                                                toLiteral(
                                                        type.getComponentType(),
                                                        elementValues.get(i))));
                return new RexLiteral((Comparable) elementLiterals, type, typeName);

            default:
                return new RexLiteral(value, type, typeName);
        }
    }

    /**
     * Converts a Jdbc string into a RexLiteral. This method accepts a string, as returned by the
     * Jdbc method ResultSet.getString(), and restores the string into an equivalent RexLiteral. It
     * allows one to use Jdbc strings as a common format for data.
     *
     * <p>If a null literal is provided, then a null pointer will be returned.
     *
     * @param type data type of literal to be read
     * @param typeName type family of literal
     * @param literal the (non-SQL encoded) string representation, as returned by the Jdbc call to
     *     return a column as a string
     * @return a typed RexLiteral, or null
     */
    public static RexLiteral fromJdbcString(
            RelDataType type, SqlTypeName typeName, String literal) {
        if (literal == null) {
            return null;
        }

        switch (typeName) {
            case CHAR:
                Charset charset = type.getCharset();
                SqlCollation collation = type.getCollation();
                NlsString str = new NlsString(literal, charset.name(), collation);
                return new RexLiteral(str, type, typeName);
            case BOOLEAN:
                boolean b = ConversionUtil.toBoolean(literal);
                return new RexLiteral(b, type, typeName);
            case DECIMAL:
            case DOUBLE:
                BigDecimal d = new BigDecimal(literal);
                return new RexLiteral(d, type, typeName);
            case BINARY:
                byte[] bytes = ConversionUtil.toByteArrayFromString(literal, 16);
                return new RexLiteral(new ByteString(bytes), type, typeName);
            case NULL:
                return new RexLiteral(null, type, typeName);
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
                long millis = SqlParserUtil.intervalToMillis(literal, type.getIntervalQualifier());
                return new RexLiteral(BigDecimal.valueOf(millis), type, typeName);
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                long months = SqlParserUtil.intervalToMonths(literal, type.getIntervalQualifier());
                return new RexLiteral(BigDecimal.valueOf(months), type, typeName);
            case DATE:
            case TIME:
            case TIMESTAMP:
                String format = getCalendarFormat(typeName);
                TimeZone tz = DateTimeUtils.UTC_ZONE;
                final Comparable v;
                switch (typeName) {
                    case DATE:
                        final Calendar cal =
                                DateTimeUtils.parseDateFormat(
                                        literal, new SimpleDateFormat(format, Locale.ROOT), tz);
                        if (cal == null) {
                            throw new AssertionError(
                                    "fromJdbcString: invalid date/time value '" + literal + "'");
                        }
                        v = DateString.fromCalendarFields(cal);
                        break;
                    default:
                        // Allow fractional seconds for times and timestamps
                        assert format != null;
                        final DateTimeUtils.PrecisionTime ts =
                                DateTimeUtils.parsePrecisionDateTimeLiteral(
                                        literal, new SimpleDateFormat(format, Locale.ROOT), tz, -1);
                        if (ts == null) {
                            throw new AssertionError(
                                    "fromJdbcString: invalid date/time value '" + literal + "'");
                        }
                        switch (typeName) {
                            case TIMESTAMP:
                                v =
                                        TimestampString.fromCalendarFields(ts.getCalendar())
                                                .withFraction(ts.getFraction());
                                break;
                            case TIME:
                                v =
                                        TimeString.fromCalendarFields(ts.getCalendar())
                                                .withFraction(ts.getFraction());
                                break;
                            default:
                                throw new AssertionError();
                        }
                }
                return new RexLiteral(v, type, typeName);

            case SYMBOL:
                // Symbols are for internal use
            default:
                throw new AssertionError("fromJdbcString: unsupported type");
        }
    }

    private static String getCalendarFormat(SqlTypeName typeName) {
        switch (typeName) {
            case DATE:
                return DateTimeUtils.DATE_FORMAT_STRING;
            case TIME:
                return DateTimeUtils.TIME_FORMAT_STRING;
            case TIMESTAMP:
                return DateTimeUtils.TIMESTAMP_FORMAT_STRING;
            default:
                throw new AssertionError("getCalendarFormat: unknown type");
        }
    }

    public SqlTypeName getTypeName() {
        return typeName;
    }

    public RelDataType getType() {
        return type;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.LITERAL;
    }

    /** Returns whether this literal's value is null. */
    public boolean isNull() {
        return value == null;
    }

    /**
     * Returns the value of this literal.
     *
     * <p>For backwards compatibility, returns DATE. TIME and TIMESTAMP as a {@link Calendar} value
     * in UTC time zone.
     */
    public Comparable getValue() {
        assert valueMatchesType(value, typeName, true) : value;
        if (value == null) {
            return null;
        }
        switch (typeName) {
            case TIME:
            case DATE:
            case TIMESTAMP:
                return getValueAs(Calendar.class);
            default:
                return value;
        }
    }

    /**
     * Returns the value of this literal, in the form that the calculator program builder wants it.
     */
    public Object getValue2() {
        if (value == null) {
            return null;
        }
        switch (typeName) {
            case CHAR:
                return getValueAs(String.class);
            case DECIMAL:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return getValueAs(Long.class);
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return getValueAs(Integer.class);
            default:
                return value;
        }
    }

    /** Returns the value of this literal, in the form that the rex-to-lix translator wants it. */
    public Object getValue3() {
        if (value == null) {
            return null;
        }
        switch (typeName) {
            case DECIMAL:
                assert value instanceof BigDecimal;
                return value;
            default:
                return getValue2();
        }
    }

    /** Returns the value of this literal, in the form that {@link RexInterpreter} wants it. */
    public Comparable getValue4() {
        if (value == null) {
            return null;
        }
        switch (typeName) {
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return getValueAs(Long.class);
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return getValueAs(Integer.class);
            default:
                return value;
        }
    }

    /**
     * Returns the value of this literal as an instance of the specified class.
     *
     * <p>The following SQL types allow more than one form:
     *
     * <ul>
     *   <li>CHAR as {@link NlsString} or {@link String}
     *   <li>TIME as {@link TimeString}, {@link Integer} (milliseconds since midnight), {@link
     *       Calendar} (in UTC)
     *   <li>DATE as {@link DateString}, {@link Integer} (days since 1970-01-01), {@link Calendar}
     *   <li>TIMESTAMP as {@link TimestampString}, {@link Long} (milliseconds since 1970-01-01
     *       00:00:00), {@link Calendar}
     *   <li>DECIMAL as {@link BigDecimal} or {@link Long}
     * </ul>
     *
     * <p>Called with {@code clazz} = {@link Comparable}, returns the value in its native form.
     *
     * @param clazz Desired return type
     * @param <T> Return type
     * @return Value of this literal in the desired type
     */
    public <T> T getValueAs(Class<T> clazz) {
        if (value == null || clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        switch (typeName) {
            case BINARY:
                if (clazz == byte[].class) {
                    return clazz.cast(((ByteString) value).getBytes());
                }
                break;
            case CHAR:
                if (clazz == String.class) {
                    return clazz.cast(((NlsString) value).getValue());
                } else if (clazz == Character.class) {
                    return clazz.cast(((NlsString) value).getValue().charAt(0));
                }
                break;
            case VARCHAR:
                if (clazz == String.class) {
                    return clazz.cast(((NlsString) value).getValue());
                }
                break;
            case DECIMAL:
                if (clazz == Long.class) {
                    return clazz.cast(((BigDecimal) value).unscaledValue().longValue());
                }
                // fall through
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case DOUBLE:
            case REAL:
            case FLOAT:
                if (clazz == Long.class) {
                    return clazz.cast(((BigDecimal) value).longValue());
                } else if (clazz == Integer.class) {
                    return clazz.cast(((BigDecimal) value).intValue());
                } else if (clazz == Short.class) {
                    return clazz.cast(((BigDecimal) value).shortValue());
                } else if (clazz == Byte.class) {
                    return clazz.cast(((BigDecimal) value).byteValue());
                } else if (clazz == Double.class) {
                    return clazz.cast(((BigDecimal) value).doubleValue());
                } else if (clazz == Float.class) {
                    return clazz.cast(((BigDecimal) value).floatValue());
                }
                break;
            case DATE:
                if (clazz == Integer.class) {
                    return clazz.cast(((DateString) value).getDaysSinceEpoch());
                } else if (clazz == Calendar.class) {
                    return clazz.cast(((DateString) value).toCalendar());
                }
                break;
            case TIME:
                if (clazz == Integer.class) {
                    return clazz.cast(((TimeString) value).getMillisOfDay());
                } else if (clazz == Calendar.class) {
                    // Note: Nanos are ignored
                    return clazz.cast(((TimeString) value).toCalendar());
                }
                break;
            case TIME_WITH_LOCAL_TIME_ZONE:
                if (clazz == Integer.class) {
                    // Milliseconds since 1970-01-01 00:00:00
                    return clazz.cast(((TimeString) value).getMillisOfDay());
                }
                break;
            case TIMESTAMP:
                if (clazz == Long.class) {
                    // Milliseconds since 1970-01-01 00:00:00
                    return clazz.cast(((TimestampString) value).getMillisSinceEpoch());
                } else if (clazz == Calendar.class) {
                    // Note: Nanos are ignored
                    return clazz.cast(((TimestampString) value).toCalendar());
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (clazz == Long.class) {
                    // Milliseconds since 1970-01-01 00:00:00
                    return clazz.cast(((TimestampString) value).getMillisSinceEpoch());
                } else if (clazz == Calendar.class) {
                    // Note: Nanos are ignored
                    return clazz.cast(((TimestampString) value).toCalendar());
                }
                break;
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
                if (clazz == Integer.class) {
                    return clazz.cast(((BigDecimal) value).intValue());
                } else if (clazz == Long.class) {
                    return clazz.cast(((BigDecimal) value).longValue());
                } else if (clazz == String.class) {
                    return clazz.cast(intervalString(getValueAs(BigDecimal.class).abs()));
                } else if (clazz == Boolean.class) {
                    // return whether negative
                    return clazz.cast(getValueAs(BigDecimal.class).signum() < 0);
                }
                break;
        }
        throw new AssertionError("cannot convert " + typeName + " literal to " + clazz);
    }

    public static boolean booleanValue(RexNode node) {
        return (Boolean) ((RexLiteral) node).value;
    }

    public boolean isAlwaysTrue() {
        if (typeName != SqlTypeName.BOOLEAN) {
            return false;
        }
        return booleanValue(this);
    }

    public boolean isAlwaysFalse() {
        if (typeName != SqlTypeName.BOOLEAN) {
            return false;
        }
        return !booleanValue(this);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return (obj instanceof RexLiteral)
                && equals(((RexLiteral) obj).value, value)
                && equals(((RexLiteral) obj).type, type);
    }

    public int hashCode() {
        return Objects.hash(value, type);
    }

    public static Comparable value(RexNode node) {
        return findValue(node);
    }

    public static int intValue(RexNode node) {
        final Comparable value = findValue(node);
        return ((Number) value).intValue();
    }

    public static String stringValue(RexNode node) {
        final Comparable value = findValue(node);
        return (value == null) ? null : ((NlsString) value).getValue();
    }

    private static Comparable findValue(RexNode node) {
        if (node instanceof RexLiteral) {
            return ((RexLiteral) node).value;
        }
        if (node instanceof RexCall) {
            final RexCall call = (RexCall) node;
            final SqlOperator operator = call.getOperator();
            if (operator == SqlStdOperatorTable.CAST) {
                return findValue(call.getOperands().get(0));
            }
            if (operator == SqlStdOperatorTable.UNARY_MINUS) {
                final BigDecimal value = (BigDecimal) findValue(call.getOperands().get(0));
                return value.negate();
            }
        }
        throw new AssertionError("not a literal: " + node);
    }

    public static boolean isNullLiteral(RexNode node) {
        return (node instanceof RexLiteral) && (((RexLiteral) node).value == null);
    }

    private static boolean equals(Object o1, Object o2) {
        return Objects.equals(o1, o2);
    }

    public <R> R accept(RexVisitor<R> visitor) {
        return visitor.visitLiteral(this);
    }

    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        return visitor.visitLiteral(this, arg);
    }
}
