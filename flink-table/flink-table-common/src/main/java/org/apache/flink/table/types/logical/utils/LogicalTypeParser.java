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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.utils.TypeStringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parser for creating instances of {@link LogicalType} from a serialized string created with {@link
 * LogicalType#asSerializableString()}.
 *
 * <p>In addition to the serializable string representations, this parser also supports common
 * shortcuts for certain types. This includes:
 *
 * <ul>
 *   <li>{@code STRING} as a synonym for {@code VARCHAR(INT_MAX)}
 *   <li>{@code BYTES} as a synonym for {@code VARBINARY(INT_MAX)}
 *   <li>{@code NUMERIC} and {@code DEC} as synonyms for {@code DECIMAL}
 *   <li>{@code INTEGER} as a synonym for {@code INT}
 *   <li>{@code DOUBLE PRECISION} as a synonym for {@code DOUBLE}
 *   <li>{@code TIME WITHOUT TIME ZONE} as a synonym for {@code TIME}
 *   <li>{@code TIMESTAMP WITHOUT TIME ZONE} as a synonym for {@code TIMESTAMP}
 *   <li>{@code TIMESTAMP WITH LOCAL TIME ZONE} as a synonym for {@code TIMESTAMP_LTZ}
 *   <li>{@code type ARRAY} as a synonym for {@code ARRAY<type>}
 *   <li>{@code type MULTISET} as a synonym for {@code MULTISET<type>}
 *   <li>{@code ROW(...)} as a synonym for {@code ROW<...>}
 *   <li>{@code type NULL} as a synonym for {@code type}
 * </ul>
 *
 * <p>Furthermore, it returns {@link UnresolvedUserDefinedType} for unknown types (partially or
 * fully qualified such as {@code [catalog].[database].[type]}).
 */
@PublicEvolving
public final class LogicalTypeParser {

    /**
     * Parses a type string. All types will be fully resolved except for {@link
     * UnresolvedUserDefinedType}s.
     *
     * @param typeString a string like "ROW(field1 INT, field2 BOOLEAN)"
     * @param classLoader class loader for loading classes of the RAW type
     * @throws ValidationException in case of parsing errors.
     */
    public static LogicalType parse(String typeString, ClassLoader classLoader) {
        final List<Token> tokens = tokenize(typeString);
        final TokenParser converter = new TokenParser(typeString, tokens, classLoader);
        return converter.parseTokens();
    }

    /**
     * Parses a type string. All types will be fully resolved except for {@link
     * UnresolvedUserDefinedType}s.
     *
     * @param typeString a string like "ROW(field1 INT, field2 BOOLEAN)"
     * @throws ValidationException in case of parsing errors.
     */
    public static LogicalType parse(String typeString) {
        return parse(typeString, Thread.currentThread().getContextClassLoader());
    }

    // --------------------------------------------------------------------------------------------
    // Tokenizer
    // --------------------------------------------------------------------------------------------

    private static final char CHAR_BEGIN_SUBTYPE = '<';
    private static final char CHAR_END_SUBTYPE = '>';
    private static final char CHAR_BEGIN_PARAMETER = '(';
    private static final char CHAR_END_PARAMETER = ')';
    private static final char CHAR_LIST_SEPARATOR = ',';
    private static final char CHAR_STRING = '\'';
    private static final char CHAR_IDENTIFIER = '`';
    private static final char CHAR_DOT = '.';

    private static boolean isDelimiter(char character) {
        return Character.isWhitespace(character)
                || character == CHAR_BEGIN_SUBTYPE
                || character == CHAR_END_SUBTYPE
                || character == CHAR_BEGIN_PARAMETER
                || character == CHAR_END_PARAMETER
                || character == CHAR_LIST_SEPARATOR
                || character == CHAR_DOT;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static List<Token> tokenize(String chars) {
        final List<Token> tokens = new ArrayList<>();
        final StringBuilder builder = new StringBuilder();
        for (int cursor = 0; cursor < chars.length(); cursor++) {
            char curChar = chars.charAt(cursor);
            switch (curChar) {
                case CHAR_BEGIN_SUBTYPE:
                    tokens.add(
                            new Token(
                                    TokenType.BEGIN_SUBTYPE,
                                    cursor,
                                    Character.toString(CHAR_BEGIN_SUBTYPE)));
                    break;
                case CHAR_END_SUBTYPE:
                    tokens.add(
                            new Token(
                                    TokenType.END_SUBTYPE,
                                    cursor,
                                    Character.toString(CHAR_END_SUBTYPE)));
                    break;
                case CHAR_BEGIN_PARAMETER:
                    tokens.add(
                            new Token(
                                    TokenType.BEGIN_PARAMETER,
                                    cursor,
                                    Character.toString(CHAR_BEGIN_PARAMETER)));
                    break;
                case CHAR_END_PARAMETER:
                    tokens.add(
                            new Token(
                                    TokenType.END_PARAMETER,
                                    cursor,
                                    Character.toString(CHAR_END_PARAMETER)));
                    break;
                case CHAR_LIST_SEPARATOR:
                    tokens.add(
                            new Token(
                                    TokenType.LIST_SEPARATOR,
                                    cursor,
                                    Character.toString(CHAR_LIST_SEPARATOR)));
                    break;
                case CHAR_DOT:
                    tokens.add(
                            new Token(
                                    TokenType.IDENTIFIER_SEPARATOR,
                                    cursor,
                                    Character.toString(CHAR_DOT)));
                    break;
                case CHAR_STRING:
                    builder.setLength(0);
                    cursor = consumeEscaped(builder, chars, cursor, CHAR_STRING);
                    tokens.add(new Token(TokenType.LITERAL_STRING, cursor, builder.toString()));
                    break;
                case CHAR_IDENTIFIER:
                    builder.setLength(0);
                    cursor = consumeEscaped(builder, chars, cursor, CHAR_IDENTIFIER);
                    tokens.add(new Token(TokenType.IDENTIFIER, cursor, builder.toString()));
                    break;
                default:
                    if (Character.isWhitespace(curChar)) {
                        continue;
                    }
                    if (isDigit(curChar)) {
                        builder.setLength(0);
                        cursor = consumeInt(builder, chars, cursor);
                        tokens.add(new Token(TokenType.LITERAL_INT, cursor, builder.toString()));
                        break;
                    }
                    builder.setLength(0);
                    cursor = consumeIdentifier(builder, chars, cursor);
                    final String token = builder.toString();
                    final String normalizedToken = token.toUpperCase();
                    if (KEYWORDS.contains(normalizedToken)) {
                        tokens.add(new Token(TokenType.KEYWORD, cursor, normalizedToken));
                    } else {
                        tokens.add(new Token(TokenType.IDENTIFIER, cursor, token));
                    }
            }
        }

        return tokens;
    }

    private static int consumeEscaped(
            StringBuilder builder, String chars, int cursor, char delimiter) {
        // skip delimiter
        cursor++;
        for (; chars.length() > cursor; cursor++) {
            final char curChar = chars.charAt(cursor);
            if (curChar == delimiter
                    && cursor + 1 < chars.length()
                    && chars.charAt(cursor + 1) == delimiter) {
                // escaping of the escaping char e.g. "'Hello '' World'"
                cursor++;
                builder.append(curChar);
            } else if (curChar == delimiter) {
                break;
            } else {
                builder.append(curChar);
            }
        }
        return cursor;
    }

    private static int consumeInt(StringBuilder builder, String chars, int cursor) {
        for (; chars.length() > cursor && isDigit(chars.charAt(cursor)); cursor++) {
            builder.append(chars.charAt(cursor));
        }
        return cursor - 1;
    }

    private static int consumeIdentifier(StringBuilder builder, String chars, int cursor) {
        for (; cursor < chars.length() && !isDelimiter(chars.charAt(cursor)); cursor++) {
            builder.append(chars.charAt(cursor));
        }
        return cursor - 1;
    }

    private enum TokenType {
        // e.g. "ROW<"
        BEGIN_SUBTYPE,

        // e.g. "ROW<..>"
        END_SUBTYPE,

        // e.g. "CHAR("
        BEGIN_PARAMETER,

        // e.g. "CHAR(...)"
        END_PARAMETER,

        // e.g. "ROW<INT,"
        LIST_SEPARATOR,

        // e.g. "ROW<name INT 'Comment'"
        LITERAL_STRING,

        // CHAR(12
        LITERAL_INT,

        // e.g. "CHAR" or "TO"
        KEYWORD,

        // e.g. "ROW<name" or "myCatalog.myDatabase"
        IDENTIFIER,

        // e.g. "myCatalog.myDatabase."
        IDENTIFIER_SEPARATOR
    }

    private enum Keyword {
        CHAR,
        VARCHAR,
        STRING,
        BOOLEAN,
        BINARY,
        VARBINARY,
        BYTES,
        DECIMAL,
        NUMERIC,
        DEC,
        TINYINT,
        SMALLINT,
        INT,
        INTEGER,
        BIGINT,
        FLOAT,
        DOUBLE,
        PRECISION,
        DATE,
        TIME,
        WITH,
        WITHOUT,
        LOCAL,
        ZONE,
        TIMESTAMP,
        TIMESTAMP_LTZ,
        INTERVAL,
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        TO,
        ARRAY,
        MULTISET,
        MAP,
        ROW,
        NULL,
        RAW,
        LEGACY,
        NOT
    }

    private static final Set<String> KEYWORDS =
            Stream.of(Keyword.values())
                    .map(k -> k.toString().toUpperCase())
                    .collect(Collectors.toSet());

    private static class Token {
        public final TokenType type;
        public final int cursorPosition;
        public final String value;

        public Token(TokenType type, int cursorPosition, String value) {
            this.type = type;
            this.cursorPosition = cursorPosition;
            this.value = value;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Token Parsing
    // --------------------------------------------------------------------------------------------

    private static class TokenParser {

        private final String inputString;

        private final List<Token> tokens;

        private final ClassLoader classLoader;

        private int lastValidToken;

        private int currentToken;

        public TokenParser(String inputString, List<Token> tokens, ClassLoader classLoader) {
            this.inputString = inputString;
            this.tokens = tokens;
            this.classLoader = classLoader;
            this.lastValidToken = -1;
            this.currentToken = -1;
        }

        private LogicalType parseTokens() {
            final LogicalType type = parseTypeWithNullability();
            if (hasRemainingTokens()) {
                nextToken();
                throw parsingError("Unexpected token: " + token().value);
            }
            return type;
        }

        private int lastCursor() {
            if (lastValidToken < 0) {
                return 0;
            }
            return tokens.get(lastValidToken).cursorPosition + 1;
        }

        private ValidationException parsingError(String cause, @Nullable Throwable t) {
            return new ValidationException(
                    String.format(
                            "Could not parse type at position %d: %s\n Input type string: %s",
                            lastCursor(), cause, inputString),
                    t);
        }

        private ValidationException parsingError(String cause) {
            return parsingError(cause, null);
        }

        private boolean hasRemainingTokens() {
            return currentToken + 1 < tokens.size();
        }

        private Token token() {
            return tokens.get(currentToken);
        }

        private int tokenAsInt() {
            try {
                return Integer.parseInt(token().value);
            } catch (NumberFormatException e) {
                throw parsingError("Invalid integer value.", e);
            }
        }

        private Keyword tokenAsKeyword() {
            return Keyword.valueOf(token().value);
        }

        private String tokenAsString() {
            return token().value;
        }

        private void nextToken() {
            this.currentToken++;
            if (currentToken >= tokens.size()) {
                throw parsingError("Unexpected end.");
            }
            lastValidToken = this.currentToken - 1;
        }

        private void nextToken(TokenType type) {
            nextToken();
            final Token token = token();
            if (token.type != type) {
                throw parsingError("<" + type.name() + "> expected but was <" + token.type + ">.");
            }
        }

        private void nextToken(Keyword keyword) {
            nextToken(TokenType.KEYWORD);
            final Token token = token();
            if (!keyword.equals(Keyword.valueOf(token.value))) {
                throw parsingError(
                        "Keyword '" + keyword + "' expected but was '" + token.value + "'.");
            }
        }

        private boolean hasNextToken(TokenType... types) {
            if (currentToken + types.length + 1 > tokens.size()) {
                return false;
            }
            for (int i = 0; i < types.length; i++) {
                final Token lookAhead = tokens.get(currentToken + i + 1);
                if (lookAhead.type != types[i]) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasNextToken(Keyword... keywords) {
            if (currentToken + keywords.length + 1 > tokens.size()) {
                return false;
            }
            for (int i = 0; i < keywords.length; i++) {
                final Token lookAhead = tokens.get(currentToken + i + 1);
                if (lookAhead.type != TokenType.KEYWORD
                        || keywords[i] != Keyword.valueOf(lookAhead.value)) {
                    return false;
                }
            }
            return true;
        }

        private boolean parseNullability() {
            // "NOT NULL"
            if (hasNextToken(Keyword.NOT, Keyword.NULL)) {
                nextToken(Keyword.NOT);
                nextToken(Keyword.NULL);
                return false;
            }
            // explicit "NULL"
            else if (hasNextToken(Keyword.NULL)) {
                nextToken(Keyword.NULL);
                return true;
            }
            // implicit "NULL"
            return true;
        }

        private LogicalType parseTypeWithNullability() {
            final LogicalType logicalType;
            if (hasNextToken(TokenType.IDENTIFIER)) {
                logicalType = parseTypeByIdentifier().copy(parseNullability());
            } else {
                logicalType = parseTypeByKeyword().copy(parseNullability());
            }

            // special case: suffix notation for ARRAY and MULTISET types
            if (hasNextToken(Keyword.ARRAY)) {
                nextToken(Keyword.ARRAY);
                return new ArrayType(logicalType).copy(parseNullability());
            } else if (hasNextToken(Keyword.MULTISET)) {
                nextToken(Keyword.MULTISET);
                return new MultisetType(logicalType).copy(parseNullability());
            }

            return logicalType;
        }

        private LogicalType parseTypeByKeyword() {
            nextToken(TokenType.KEYWORD);
            switch (tokenAsKeyword()) {
                case CHAR:
                    return parseCharType();
                case VARCHAR:
                    return parseVarCharType();
                case STRING:
                    return new VarCharType(VarCharType.MAX_LENGTH);
                case BOOLEAN:
                    return new BooleanType();
                case BINARY:
                    return parseBinaryType();
                case VARBINARY:
                    return parseVarBinaryType();
                case BYTES:
                    return new VarBinaryType(VarBinaryType.MAX_LENGTH);
                case DECIMAL:
                case NUMERIC:
                case DEC:
                    return parseDecimalType();
                case TINYINT:
                    return new TinyIntType();
                case SMALLINT:
                    return new SmallIntType();
                case INT:
                case INTEGER:
                    return new IntType();
                case BIGINT:
                    return new BigIntType();
                case FLOAT:
                    return new FloatType();
                case DOUBLE:
                    return parseDoubleType();
                case DATE:
                    return new DateType();
                case TIME:
                    return parseTimeType();
                case TIMESTAMP:
                    return parseTimestampType();
                case TIMESTAMP_LTZ:
                    return parseTimestampLtzType();
                case INTERVAL:
                    return parseIntervalType();
                case ARRAY:
                    return parseArrayType();
                case MULTISET:
                    return parseMultisetType();
                case MAP:
                    return parseMapType();
                case ROW:
                    return parseRowType();
                case NULL:
                    return new NullType();
                case RAW:
                    return parseRawType();
                case LEGACY:
                    return parseLegacyType();
                default:
                    throw parsingError("Unsupported type: " + token().value);
            }
        }

        private LogicalType parseTypeByIdentifier() {
            nextToken(TokenType.IDENTIFIER);
            List<String> parts = new ArrayList<>();
            parts.add(tokenAsString());
            if (hasNextToken(TokenType.IDENTIFIER_SEPARATOR)) {
                nextToken(TokenType.IDENTIFIER_SEPARATOR);
                nextToken(TokenType.IDENTIFIER);
                parts.add(tokenAsString());
            }
            if (hasNextToken(TokenType.IDENTIFIER_SEPARATOR)) {
                nextToken(TokenType.IDENTIFIER_SEPARATOR);
                nextToken(TokenType.IDENTIFIER);
                parts.add(tokenAsString());
            }
            final String[] identifierParts =
                    Stream.of(lastPart(parts, 2), lastPart(parts, 1), lastPart(parts, 0))
                            .filter(Objects::nonNull)
                            .toArray(String[]::new);
            return new UnresolvedUserDefinedType(UnresolvedIdentifier.of(identifierParts));
        }

        private @Nullable String lastPart(List<String> parts, int inversePos) {
            final int pos = parts.size() - inversePos - 1;
            if (pos < 0) {
                return null;
            }
            return parts.get(pos);
        }

        private int parseStringType() {
            // explicit length
            if (hasNextToken(TokenType.BEGIN_PARAMETER)) {
                nextToken(TokenType.BEGIN_PARAMETER);
                nextToken(TokenType.LITERAL_INT);
                final int length = tokenAsInt();
                nextToken(TokenType.END_PARAMETER);
                return length;
            }
            // implicit length
            return -1;
        }

        private LogicalType parseCharType() {
            final int length = parseStringType();
            if (length < 0) {
                return new CharType();
            } else {
                return new CharType(length);
            }
        }

        private LogicalType parseVarCharType() {
            final int length = parseStringType();
            if (length < 0) {
                return new VarCharType();
            } else {
                return new VarCharType(length);
            }
        }

        private LogicalType parseBinaryType() {
            final int length = parseStringType();
            if (length < 0) {
                return new BinaryType();
            } else {
                return new BinaryType(length);
            }
        }

        private LogicalType parseVarBinaryType() {
            final int length = parseStringType();
            if (length < 0) {
                return new VarBinaryType();
            } else {
                return new VarBinaryType(length);
            }
        }

        private LogicalType parseDecimalType() {
            int precision = DecimalType.DEFAULT_PRECISION;
            int scale = DecimalType.DEFAULT_SCALE;
            if (hasNextToken(TokenType.BEGIN_PARAMETER)) {
                nextToken(TokenType.BEGIN_PARAMETER);
                nextToken(TokenType.LITERAL_INT);
                precision = tokenAsInt();
                if (hasNextToken(TokenType.LIST_SEPARATOR)) {
                    nextToken(TokenType.LIST_SEPARATOR);
                    nextToken(TokenType.LITERAL_INT);
                    scale = tokenAsInt();
                }
                nextToken(TokenType.END_PARAMETER);
            }
            return new DecimalType(precision, scale);
        }

        private LogicalType parseDoubleType() {
            if (hasNextToken(Keyword.PRECISION)) {
                nextToken(Keyword.PRECISION);
            }
            return new DoubleType();
        }

        private LogicalType parseTimeType() {
            int precision = parseOptionalPrecision(TimeType.DEFAULT_PRECISION);
            if (hasNextToken(Keyword.WITHOUT)) {
                nextToken(Keyword.WITHOUT);
                nextToken(Keyword.TIME);
                nextToken(Keyword.ZONE);
            }
            return new TimeType(precision);
        }

        private LogicalType parseTimestampType() {
            int precision = parseOptionalPrecision(TimestampType.DEFAULT_PRECISION);
            if (hasNextToken(Keyword.WITHOUT)) {
                nextToken(Keyword.WITHOUT);
                nextToken(Keyword.TIME);
                nextToken(Keyword.ZONE);
            } else if (hasNextToken(Keyword.WITH)) {
                nextToken(Keyword.WITH);
                if (hasNextToken(Keyword.LOCAL)) {
                    nextToken(Keyword.LOCAL);
                    nextToken(Keyword.TIME);
                    nextToken(Keyword.ZONE);
                    return new LocalZonedTimestampType(precision);
                } else {
                    nextToken(Keyword.TIME);
                    nextToken(Keyword.ZONE);
                    return new ZonedTimestampType(precision);
                }
            }
            return new TimestampType(precision);
        }

        private LogicalType parseTimestampLtzType() {
            int precision = parseOptionalPrecision(LocalZonedTimestampType.DEFAULT_PRECISION);
            return new LocalZonedTimestampType(precision);
        }

        private LogicalType parseIntervalType() {
            nextToken(TokenType.KEYWORD);
            switch (tokenAsKeyword()) {
                case YEAR:
                case MONTH:
                    return parseYearMonthIntervalType();
                case DAY:
                case HOUR:
                case MINUTE:
                case SECOND:
                    return parseDayTimeIntervalType();
                default:
                    throw parsingError("Invalid interval resolution.");
            }
        }

        private LogicalType parseYearMonthIntervalType() {
            int yearPrecision = YearMonthIntervalType.DEFAULT_PRECISION;
            switch (tokenAsKeyword()) {
                case YEAR:
                    yearPrecision = parseOptionalPrecision(yearPrecision);
                    if (hasNextToken(Keyword.TO)) {
                        nextToken(Keyword.TO);
                        nextToken(Keyword.MONTH);
                        return new YearMonthIntervalType(
                                YearMonthResolution.YEAR_TO_MONTH, yearPrecision);
                    }
                    return new YearMonthIntervalType(YearMonthResolution.YEAR, yearPrecision);
                case MONTH:
                    return new YearMonthIntervalType(YearMonthResolution.MONTH, yearPrecision);
                default:
                    throw parsingError("Invalid year-month interval resolution.");
            }
        }

        private LogicalType parseDayTimeIntervalType() {
            int dayPrecision = DayTimeIntervalType.DEFAULT_DAY_PRECISION;
            int fractionalPrecision = DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION;
            switch (tokenAsKeyword()) {
                case DAY:
                    dayPrecision = parseOptionalPrecision(dayPrecision);
                    if (hasNextToken(Keyword.TO)) {
                        nextToken(Keyword.TO);
                        nextToken(TokenType.KEYWORD);
                        switch (tokenAsKeyword()) {
                            case HOUR:
                                return new DayTimeIntervalType(
                                        DayTimeResolution.DAY_TO_HOUR,
                                        dayPrecision,
                                        fractionalPrecision);
                            case MINUTE:
                                return new DayTimeIntervalType(
                                        DayTimeResolution.DAY_TO_MINUTE,
                                        dayPrecision,
                                        fractionalPrecision);
                            case SECOND:
                                fractionalPrecision = parseOptionalPrecision(fractionalPrecision);
                                return new DayTimeIntervalType(
                                        DayTimeResolution.DAY_TO_SECOND,
                                        dayPrecision,
                                        fractionalPrecision);
                            default:
                                throw parsingError("Invalid day-time interval resolution.");
                        }
                    }
                    return new DayTimeIntervalType(
                            DayTimeResolution.DAY, dayPrecision, fractionalPrecision);
                case HOUR:
                    if (hasNextToken(Keyword.TO)) {
                        nextToken(Keyword.TO);
                        nextToken(TokenType.KEYWORD);
                        switch (tokenAsKeyword()) {
                            case MINUTE:
                                return new DayTimeIntervalType(
                                        DayTimeResolution.HOUR_TO_MINUTE,
                                        dayPrecision,
                                        fractionalPrecision);
                            case SECOND:
                                fractionalPrecision = parseOptionalPrecision(fractionalPrecision);
                                return new DayTimeIntervalType(
                                        DayTimeResolution.HOUR_TO_SECOND,
                                        dayPrecision,
                                        fractionalPrecision);
                            default:
                                throw parsingError("Invalid day-time interval resolution.");
                        }
                    }
                    return new DayTimeIntervalType(
                            DayTimeResolution.HOUR, dayPrecision, fractionalPrecision);
                case MINUTE:
                    if (hasNextToken(Keyword.TO)) {
                        nextToken(Keyword.TO);
                        nextToken(Keyword.SECOND);
                        fractionalPrecision = parseOptionalPrecision(fractionalPrecision);
                        return new DayTimeIntervalType(
                                DayTimeResolution.MINUTE_TO_SECOND,
                                dayPrecision,
                                fractionalPrecision);
                    }
                    return new DayTimeIntervalType(
                            DayTimeResolution.MINUTE, dayPrecision, fractionalPrecision);
                case SECOND:
                    fractionalPrecision = parseOptionalPrecision(fractionalPrecision);
                    return new DayTimeIntervalType(
                            DayTimeResolution.SECOND, dayPrecision, fractionalPrecision);
                default:
                    throw parsingError("Invalid day-time interval resolution.");
            }
        }

        private int parseOptionalPrecision(int defaultPrecision) {
            int precision = defaultPrecision;
            if (hasNextToken(TokenType.BEGIN_PARAMETER)) {
                nextToken(TokenType.BEGIN_PARAMETER);
                nextToken(TokenType.LITERAL_INT);
                precision = tokenAsInt();
                nextToken(TokenType.END_PARAMETER);
            }
            return precision;
        }

        private LogicalType parseArrayType() {
            nextToken(TokenType.BEGIN_SUBTYPE);
            final LogicalType elementType = parseTypeWithNullability();
            nextToken(TokenType.END_SUBTYPE);
            return new ArrayType(elementType);
        }

        private LogicalType parseMultisetType() {
            nextToken(TokenType.BEGIN_SUBTYPE);
            final LogicalType elementType = parseTypeWithNullability();
            nextToken(TokenType.END_SUBTYPE);
            return new MultisetType(elementType);
        }

        private LogicalType parseMapType() {
            nextToken(TokenType.BEGIN_SUBTYPE);
            final LogicalType keyType = parseTypeWithNullability();
            nextToken(TokenType.LIST_SEPARATOR);
            final LogicalType valueType = parseTypeWithNullability();
            nextToken(TokenType.END_SUBTYPE);
            return new MapType(keyType, valueType);
        }

        private LogicalType parseRowType() {
            List<RowType.RowField> fields;
            // SQL standard notation
            if (hasNextToken(TokenType.BEGIN_PARAMETER)) {
                nextToken(TokenType.BEGIN_PARAMETER);
                fields = parseRowFields(TokenType.END_PARAMETER);
                nextToken(TokenType.END_PARAMETER);
            } else {
                nextToken(TokenType.BEGIN_SUBTYPE);
                fields = parseRowFields(TokenType.END_SUBTYPE);
                nextToken(TokenType.END_SUBTYPE);
            }
            return new RowType(fields);
        }

        private List<RowType.RowField> parseRowFields(TokenType endToken) {
            List<RowType.RowField> fields = new ArrayList<>();
            boolean isFirst = true;
            while (!hasNextToken(endToken)) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    nextToken(TokenType.LIST_SEPARATOR);
                }
                nextToken(TokenType.IDENTIFIER);
                final String name = tokenAsString();
                final LogicalType type = parseTypeWithNullability();
                if (hasNextToken(TokenType.LITERAL_STRING)) {
                    nextToken(TokenType.LITERAL_STRING);
                    final String description = tokenAsString();
                    fields.add(new RowType.RowField(name, type, description));
                } else {
                    fields.add(new RowType.RowField(name, type));
                }
            }
            return fields;
        }

        @SuppressWarnings("unchecked")
        private LogicalType parseRawType() {
            nextToken(TokenType.BEGIN_PARAMETER);
            nextToken(TokenType.LITERAL_STRING);
            final String className = tokenAsString();

            nextToken(TokenType.LIST_SEPARATOR);
            nextToken(TokenType.LITERAL_STRING);
            final String serializerString = tokenAsString();
            nextToken(TokenType.END_PARAMETER);

            return RawType.restore(classLoader, className, serializerString);
        }

        @SuppressWarnings("unchecked")
        private LogicalType parseLegacyType() {
            nextToken(TokenType.BEGIN_PARAMETER);
            nextToken(TokenType.LITERAL_STRING);
            final String rootString = tokenAsString();

            nextToken(TokenType.LIST_SEPARATOR);
            nextToken(TokenType.LITERAL_STRING);
            final String typeInfoString = tokenAsString();
            nextToken(TokenType.END_PARAMETER);

            try {
                final LogicalTypeRoot root = LogicalTypeRoot.valueOf(rootString);
                final TypeInformation typeInfo = TypeStringUtils.readTypeInfo(typeInfoString);
                return new LegacyTypeInformationType<>(root, typeInfo);
            } catch (Throwable t) {
                throw parsingError(
                        "Unable to restore the Legacy type of '"
                                + typeInfoString
                                + "' with "
                                + "type root '"
                                + rootString
                                + "'.",
                        t);
            }
        }
    }
}
