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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class represents a parser to implement the built-in function TO_NUMBER. Inspired from
 * ToNumberParser in Spark, but returns null instead of throwing an exception for invalid cases.
 *
 * <p>It works by consuming an input string and a format string. This class accepts the format
 * string for construction, and proceeds to iterate through the format string to generate a sequence
 * of tokens. Then, when the function is called with an input string, this class steps through the
 * sequence of tokens and compares them against the input string, returning a {@link DecimalData}
 * object if they match (or null otherwise).
 */
@Internal
public final class NumberParser {

    private static final char ANGLE_BRACKET_CLOSE = '>';
    private static final char ANGLE_BRACKET_OPEN = '<';
    private static final char COMMA_LETTER = 'G';
    private static final char COMMA_SIGN = ',';
    private static final char DOLLAR_LETTER = 'L';
    private static final char DOLLAR_SIGN = '$';
    private static final char MINUS_SIGN = '-';
    private static final char NINE_DIGIT = '9';
    private static final char OPTIONAL_PLUS_OR_MINUS_LETTER = 'S';
    private static final char PLUS_SIGN = '+';
    private static final char POINT_LETTER = 'D';
    private static final char POINT_SIGN = '.';
    private static final char ZERO_DIGIT = '0';

    // OPTIONAL_MINUS_STRING = "MI"
    private static final char OPTIONAL_MINUS_STRING_START = 'M';
    private static final char OPTIONAL_MINUS_STRING_END = 'I';

    // WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER = "PR"
    private static final char WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START = 'P';
    private static final char WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END = 'R';

    // This enum class represents one or more characters that we expect to be present in the input
    // string based on the format string.
    private enum Token {
        // Represents exactly 'num' digits (0-9).
        EXACTLY_AS_MANY_DIGITS,
        // Represents at most 'num' digits (0-9).
        AT_MOST_AS_MANY_DIGITS,
        // See DigitGroup.
        DIGIT_GROUP,
        // Represents a decimal point (.).
        DECIMAL_POINT,
        // Represents a thousand separator (,).
        THOUSANDS_SEPARATOR,
        // Represents a dollar sign ($).
        DOLLAR_SIGN,
        // Represents an optional plus sign (+) or minus sign (-).
        OPTIONAL_PLUS_OR_MINUS_SIGN,
        // Represents an optional minus sign (-).
        OPTIONAL_MINUS_SIGN,
        // Represents an opening angle bracket (<).
        OPENING_ANGLE_BRACKET,
        // Represents a closing angle bracket (>).
        CLOSING_ANGLE_BRACKET,
        // Represents any unrecognized character other than the above.
        INVALID_UNRECOGNIZED_CHARACTER
    }

    // Consecutive digits and thousands separators are combined into a Token.DIGIT_GROUP, this class
    // is an extension for detailed tokens and digit lengths inside.
    private static class DigitGroup {

        // Token.EXACTLY_AS_MANY_DIGITS & Token.AT_MOST_AS_MANY_DIGITS & Token.THOUSANDS_SEPARATOR
        private final List<Token> tokens;

        // digit lengths for Token.EXACTLY_AS_MANY_DIGITS & Token.AT_MOST_AS_MANY_DIGITS
        // NOTE: tokens.size() and sizes.size() may differ due to Token.THOUSANDS_SEPARATOR
        private final List<Integer> sizes;

        public DigitGroup(List<Token> tokens, List<Integer> sizes) {
            this.tokens = new ArrayList<>(tokens);
            this.sizes = new ArrayList<>(sizes);
        }
    }

    // ------------------------------------------------

    private final List<Token> formatTokens;
    private final List<DigitGroup> digitGroups;

    private int precision;
    private int scale;

    private final StringBuilder parsedBeforeDecimalPoint;
    private final StringBuilder parsedAfterDecimalPoint;

    // ------------------------------------------------

    private NumberParser(final String format) {
        formatTokens = new ArrayList<>();
        digitGroups = new ArrayList<>();
        parsedBeforeDecimalPoint = new StringBuilder(precision);
        parsedAfterDecimalPoint = new StringBuilder(scale);
        parseFormat(format);
    }

    public static Optional<NumberParser> of(final String numberFormat) {
        NumberParser parser = new NumberParser(numberFormat);
        // return Optional.empty() for invalid format string
        return parser.isValidFormat() ? Optional.of(parser) : Optional.empty();
    }

    private void parseFormat(final String format) {
        List<Token> tokens = new ArrayList<>();
        List<Integer> digitGroupSizes = new ArrayList<>();

        int len = format.length();
        boolean reachedDecimalPoint = false;

        for (int i = 0, prevI; i < len; ) {
            char charValue = format.charAt(i);
            switch (charValue) {
                case ZERO_DIGIT:
                    // consecutive digits with a leading 0
                    prevI = i++;
                    while (i < len
                            && (format.charAt(i) == ZERO_DIGIT || format.charAt(i) == NINE_DIGIT)) {
                        i++;
                    }
                    tokens.add(
                            reachedDecimalPoint
                                    ? Token.AT_MOST_AS_MANY_DIGITS
                                    : Token.EXACTLY_AS_MANY_DIGITS);
                    digitGroupSizes.add(i - prevI);
                    precision += i - prevI;
                    if (reachedDecimalPoint) {
                        scale += i - prevI;
                    }
                    break;
                case NINE_DIGIT:
                    // consecutive digits with a leading 9
                    prevI = i++;
                    while (i < len
                            && (format.charAt(i) == ZERO_DIGIT || format.charAt(i) == NINE_DIGIT)) {
                        i++;
                    }
                    tokens.add(Token.AT_MOST_AS_MANY_DIGITS);
                    digitGroupSizes.add(i - prevI);
                    precision += i - prevI;
                    if (reachedDecimalPoint) {
                        scale += i - prevI;
                    }
                    break;
                case POINT_SIGN:
                case POINT_LETTER:
                    tokens.add(Token.DECIMAL_POINT);
                    reachedDecimalPoint = true;
                    i++;
                    break;
                case COMMA_SIGN:
                case COMMA_LETTER:
                    tokens.add(Token.THOUSANDS_SEPARATOR);
                    i++;
                    break;
                case DOLLAR_SIGN:
                case DOLLAR_LETTER:
                    tokens.add(Token.DOLLAR_SIGN);
                    i++;
                    break;
                case OPTIONAL_PLUS_OR_MINUS_LETTER:
                    tokens.add(Token.OPTIONAL_PLUS_OR_MINUS_SIGN);
                    i++;
                    break;
                case OPTIONAL_MINUS_STRING_START:
                    if (i < len - 1 && format.charAt(i + 1) == OPTIONAL_MINUS_STRING_END) {
                        tokens.add(Token.OPTIONAL_MINUS_SIGN);
                        i += 2;
                        continue;
                    }
                    // fall through to default
                case WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START:
                    if (i < len - 1
                            && format.charAt(i + 1)
                                    == WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END) {
                        tokens.add(0, Token.OPENING_ANGLE_BRACKET);
                        tokens.add(Token.CLOSING_ANGLE_BRACKET);
                        i += 2;
                        continue;
                    }
                    // fall through to default
                default:
                    tokens.add(Token.INVALID_UNRECOGNIZED_CHARACTER);
                    i++;
                    break;
            }
        }
        combineIntoDigitGroup(tokens, digitGroupSizes);
    }

    private void combineIntoDigitGroup(
            final List<Token> tokens, final List<Integer> digitGroupSizes) {
        List<Token> currentGroup = new ArrayList<>();
        List<Integer> currentSize = new ArrayList<>();

        for (int i = 0, j = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token == Token.AT_MOST_AS_MANY_DIGITS || token == Token.EXACTLY_AS_MANY_DIGITS) {
                currentGroup.add(token);
                currentSize.add(digitGroupSizes.get(j++));
            } else if (token == Token.THOUSANDS_SEPARATOR) {
                currentGroup.add(token);
            } else {
                if (!currentGroup.isEmpty()) {
                    formatTokens.add(Token.DIGIT_GROUP);
                    digitGroups.add(new DigitGroup(currentGroup, currentSize));
                    currentGroup.clear();
                    currentSize.clear();
                }
                formatTokens.add(token);
            }
        }

        if (!currentGroup.isEmpty()) {
            // handle the last group if it exists
            formatTokens.add(Token.DIGIT_GROUP);
            digitGroups.add(new DigitGroup(currentGroup, currentSize));
        }
    }

    private boolean isValidFormat() {
        // Make sure the format string contains at least one digit.
        if (digitGroups.isEmpty()) {
            return false;
        }

        return !hasInvalidThousandsSeparator()
                && !hasProhibitedDuplicateToken()
                && !hasDisorderedToken();
    }

    private boolean hasInvalidThousandsSeparator() {
        int firstDecimalPointIndex = formatTokens.indexOf(Token.DECIMAL_POINT);
        int digitGroupsCountBeforeDecimalPoint =
                firstDecimalPointIndex == -1
                        ? digitGroups.size()
                        : (int)
                                formatTokens.subList(0, firstDecimalPointIndex).stream()
                                        .filter(token -> token == Token.DIGIT_GROUP)
                                        .count();

        // Make sure any thousands separators in the format string have digits before and after.
        for (DigitGroup dg : digitGroups.subList(0, digitGroupsCountBeforeDecimalPoint)) {
            boolean hasInvalidThousandSeparatorsBeforeDecimal =
                    IntStream.range(0, dg.tokens.size())
                            .filter(index -> dg.tokens.get(index) == Token.THOUSANDS_SEPARATOR)
                            .anyMatch(
                                    index ->
                                            (index == 0
                                                    || index == dg.tokens.size() - 1
                                                    || dg.tokens.get(index + 1)
                                                            == Token.THOUSANDS_SEPARATOR));
            if (hasInvalidThousandSeparatorsBeforeDecimal) {
                return true;
            }
        }

        // Make sure that thousands separators do not appear after the decimal point, if any.
        return digitGroups.subList(digitGroupsCountBeforeDecimalPoint, digitGroups.size()).stream()
                .anyMatch(dg -> dg.tokens.contains(Token.THOUSANDS_SEPARATOR));
    }

    private boolean hasProhibitedDuplicateToken() {
        // Make sure that the format string does not contain any prohibited duplicate tokens.
        List<Token> prohibitedDuplicates =
                Arrays.asList(
                        Token.DECIMAL_POINT,
                        Token.OPTIONAL_PLUS_OR_MINUS_SIGN,
                        Token.DOLLAR_SIGN,
                        Token.OPTIONAL_MINUS_SIGN,
                        Token.CLOSING_ANGLE_BRACKET);
        return formatTokens.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .anyMatch(
                        entry ->
                                prohibitedDuplicates.contains(entry.getKey())
                                        && entry.getValue() > 1);
    }

    private boolean hasDisorderedToken() {
        // Ordering of tokens:
        // [ MI | S ]
        // [ L | $ ]
        // [ 0 | 9 | G | , ]*
        // [ D | . ]
        // [ 0 | 9 ]*
        // [ MI | S | PR]
        List<List<Token>> allowedFormatTokens =
                Arrays.asList(
                        Arrays.asList(Token.OPENING_ANGLE_BRACKET),
                        Arrays.asList(Token.OPTIONAL_MINUS_SIGN, Token.OPTIONAL_PLUS_OR_MINUS_SIGN),
                        Arrays.asList(Token.DOLLAR_SIGN),
                        Arrays.asList(Token.DIGIT_GROUP),
                        Arrays.asList(Token.DECIMAL_POINT),
                        Arrays.asList(Token.DIGIT_GROUP),
                        Arrays.asList(
                                Token.OPTIONAL_MINUS_SIGN,
                                Token.OPTIONAL_PLUS_OR_MINUS_SIGN,
                                Token.CLOSING_ANGLE_BRACKET));

        int formatTokenIndex = 0;
        for (List<Token> allowed : allowedFormatTokens) {
            if (formatTokenIndex < formatTokens.size()
                    && allowed.contains(formatTokens.get(formatTokenIndex))) {
                formatTokenIndex++;
            }
        }
        return formatTokenIndex != formatTokens.size();
    }

    public Optional<DataType> getOutputType() {
        try {
            return Optional.of(DataTypes.DECIMAL(precision, scale));
        } catch (Throwable t) {
            return Optional.empty();
        }
    }

    public DecimalData parse(StringData expr) {
        String str = expr.toString();
        int strLength = str.length();

        if (strLength == 0) {
            return null;
        }

        parsedBeforeDecimalPoint.setLength(0);
        parsedAfterDecimalPoint.setLength(0);
        boolean reachedDecimalPoint = false;
        boolean unmatchedOpeningAngleBracket = false;
        boolean negateResult = false;
        int strIndex = 0;
        int formatIndex = 0;
        int digitGroupIndex = 0;

        while (formatIndex < formatTokens.size()) {
            Token token = formatTokens.get(formatIndex);
            Character ch = (strIndex < strLength) ? str.charAt(strIndex) : null;

            switch (token) {
                case DIGIT_GROUP:
                    if (ch == null && !reachedDecimalPoint) {
                        return null;
                    }
                    strIndex =
                            parseDigitGroup(
                                    digitGroups.get(digitGroupIndex),
                                    str,
                                    strIndex,
                                    reachedDecimalPoint);
                    if (strIndex == -1) {
                        return null;
                    }
                    digitGroupIndex++;
                    break;
                case DECIMAL_POINT:
                    // optional
                    reachedDecimalPoint = true;
                    if (ch != null && ch.equals(POINT_SIGN)) {
                        strIndex++;
                    }
                    break;
                case DOLLAR_SIGN:
                    // essential
                    if (ch != null && ch.equals(DOLLAR_SIGN)) {
                        strIndex++;
                    } else {
                        return null;
                    }
                    break;
                case OPTIONAL_PLUS_OR_MINUS_SIGN:
                    if (ch != null) {
                        if (ch.equals(PLUS_SIGN)) {
                            strIndex++;
                        } else if (ch.equals(MINUS_SIGN)) {
                            negateResult = !negateResult;
                            strIndex++;
                        }
                    }
                    break;
                case OPTIONAL_MINUS_SIGN:
                    if (ch != null && ch.equals(MINUS_SIGN)) {
                        negateResult = !negateResult;
                        strIndex++;
                    }
                    break;
                case OPENING_ANGLE_BRACKET:
                    // ch is null only if str is empty since OPENING_ANGLE_BRACKET will only appears
                    // at the beginning of formatTokens.
                    if (ch.equals(ANGLE_BRACKET_OPEN)) {
                        unmatchedOpeningAngleBracket = true;
                        strIndex++;
                    }
                    break;
                case CLOSING_ANGLE_BRACKET:
                    // optional
                    if (ch != null && ch.equals(ANGLE_BRACKET_CLOSE)) {
                        if (!unmatchedOpeningAngleBracket) {
                            return null;
                        }
                        unmatchedOpeningAngleBracket = false;
                        negateResult = !negateResult;
                        strIndex++;
                    }
                    break;
            }
            formatIndex++;
        }

        if (strIndex < strLength || unmatchedOpeningAngleBracket) {
            return null;
        }

        return parseResultToDecimalValue(negateResult);
    }

    private int parseDigitGroup(
            DigitGroup digitGroup, String str, int strIndex, boolean reachedDecimalPoint) {
        int strLength = str.length();

        int parsedDigitsNumInCurrentGroup = 0;
        List<Integer> parsedDigitGroupSizes = new ArrayList<>();
        for (; strIndex < strLength; strIndex++) {
            char character = str.charAt(strIndex);
            if (Character.isDigit(character)) {
                parsedDigitsNumInCurrentGroup++;
                // Append each group of input digits to the appropriate
                // before/parsedAfterDecimalPoint
                // StringBuilder for later use in constructing the result Decimal value.
                if (reachedDecimalPoint) {
                    parsedAfterDecimalPoint.append(character);
                } else {
                    parsedBeforeDecimalPoint.append(character);
                }
            } else if (character == COMMA_SIGN) {
                parsedDigitGroupSizes.add(parsedDigitsNumInCurrentGroup);
                parsedDigitsNumInCurrentGroup = 0;
            } else if (!Character.isWhitespace(character)) {
                // Ignore whitespace and keep advancing through the input string.
                parsedDigitGroupSizes.add(parsedDigitsNumInCurrentGroup);
                break;
            }
        }

        if (strIndex == strLength) {
            // handle the last group if it exists
            parsedDigitGroupSizes.add(parsedDigitsNumInCurrentGroup);
        }

        return isActualDigitsAsExpected(parsedDigitGroupSizes, digitGroup) ? strIndex : -1;
    }

    private boolean isActualDigitsAsExpected(
            List<Integer> actualDigitsSizes, DigitGroup expectedDigitGroup) {
        if (actualDigitsSizes.size() > expectedDigitGroup.sizes.size()) {
            return false;
        }

        // Traverse from the end so that leading Token.AT_MOST_AS_MANY_DIGITS can be omitted.
        int actualIndexDiff = actualDigitsSizes.size() - expectedDigitGroup.tokens.size();
        int expectedIndexDiff = expectedDigitGroup.sizes.size() - expectedDigitGroup.tokens.size();
        for (int tokenIndex = expectedDigitGroup.tokens.size() - 1; tokenIndex >= 0; tokenIndex--) {
            Token expectedToken = expectedDigitGroup.tokens.get(tokenIndex);
            if (expectedToken == Token.THOUSANDS_SEPARATOR) {
                actualIndexDiff++;
                expectedIndexDiff++;
                continue;
            }

            int actualSize =
                    (tokenIndex + actualIndexDiff >= 0)
                            ? actualDigitsSizes.get(tokenIndex + actualIndexDiff)
                            : 0;
            int expectedSize = expectedDigitGroup.sizes.get(tokenIndex + expectedIndexDiff);

            if (expectedToken == Token.EXACTLY_AS_MANY_DIGITS && actualSize != expectedSize) {
                return false;
            } else if (expectedToken == Token.AT_MOST_AS_MANY_DIGITS && actualSize > expectedSize) {
                return false;
            }
        }
        return true;
    }

    private DecimalData parseResultToDecimalValue(boolean negateResult) {
        // Append zeros to parsedAfterDecimalPoint to match the scale defined by the format string.
        for (int i = scale - parsedAfterDecimalPoint.length(); i > 0; i--) {
            parsedAfterDecimalPoint.append('0');
        }

        // Construct the full decimal string, including optional negative sign and decimal point.
        String numStr =
                (negateResult ? "-" : "")
                        + parsedBeforeDecimalPoint
                        + (scale == 0 ? "" : ".")
                        + parsedAfterDecimalPoint;

        java.math.BigDecimal javaDecimal = new java.math.BigDecimal(numStr);

        // Determine how to construct the final Decimal object based on its size.
        // DecimalData.MAX_LONG_DIGITS = 18
        if (precision <= 18) {
            // Use long value if possible.
            return DecimalData.fromUnscaledLong(
                    javaDecimal.unscaledValue().longValue(), precision, scale);
        } else {
            // Use BigInteger for larger values.
            return DecimalData.fromBigDecimal(javaDecimal, precision, scale);
        }
    }
}
