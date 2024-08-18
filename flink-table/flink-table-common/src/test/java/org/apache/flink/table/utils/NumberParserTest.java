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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NumberParser}. */
class NumberParserTest {

    @Test
    void testOfInvalidFormat() {
        // without digits
        assertThat(NumberParser.of("")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("$")).isEqualTo(Optional.empty());

        // invalid thousands separator
        assertThat(NumberParser.of("000,000,.")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of(",000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("L,000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000,,000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000.000,000")).isEqualTo(Optional.empty());

        // prohibited duplicate tokens
        assertThat(NumberParser.of("000.0D00")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("S000000S")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("$000,000L")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("MI000,000MI")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000,000PRPR")).isEqualTo(Optional.empty());

        // disordering
        assertThat(NumberParser.of("$MI000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("$S000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000$")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("PR000,000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000S.00")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000MI.00")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000PR.00")).isEqualTo(Optional.empty());

        // conflicted tokens
        assertThat(NumberParser.of("MIS000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000PRS")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000MIPR")).isEqualTo(Optional.empty());

        // invalid token
        assertThat(NumberParser.of("000B")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000E")).isEqualTo(Optional.empty());

        // incomplete token of "MI" & "PR"
        assertThat(NumberParser.of("000P")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000PPR")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("M000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("ML000")).isEqualTo(Optional.empty());
        assertThat(NumberParser.of("000M")).isEqualTo(Optional.empty());
    }

    @Test
    void testGetOutputType() {
        // invalid type
        assertThat(
                        NumberParser.of("0999999999,0999999999,0999999999,0999999999")
                                .get()
                                .getOutputType())
                .isEqualTo(Optional.empty());

        // valid type
        assertThat(NumberParser.of("S$999,099G999.90PR").get().getOutputType().get())
                .isEqualTo(DataTypes.DECIMAL(11, 2));
        assertThat(NumberParser.of("999999PR").get().getOutputType().get())
                .isEqualTo(DataTypes.DECIMAL(6, 0));
        assertThat(NumberParser.of("99D99").get().getOutputType().get())
                .isEqualTo(DataTypes.DECIMAL(4, 2));
        assertThat(NumberParser.of("L999.99").get().getOutputType().get())
                .isEqualTo(DataTypes.DECIMAL(5, 2));
        assertThat(NumberParser.of(".99").get().getOutputType().get())
                .isEqualTo(DataTypes.DECIMAL(2, 2));
        assertThat(NumberParser.of("L999D").get().getOutputType().get())
                .isEqualTo(DataTypes.DECIMAL(3, 0));
    }

    @Test
    void testParseMismatching() {
        // empty input
        assertThat(NumberParser.of("$000").get().parse(BinaryStringData.fromString("")))
                .isEqualTo(null);

        // DIGIT_GROUP
        // null character
        assertThat(NumberParser.of("L999").get().parse(BinaryStringData.fromString("$")))
                .isEqualTo(null);
        // invalid character
        assertThat(NumberParser.of("000").get().parse(BinaryStringData.fromString("$12")))
                .isEqualTo(null);
        assertThat(NumberParser.of("0D0").get().parse(BinaryStringData.fromString("1D2")))
                .isEqualTo(null);
        // DigitGroup size mismatch
        assertThat(NumberParser.of("900").get().parse(BinaryStringData.fromString("1234")))
                .isEqualTo(null);
        assertThat(NumberParser.of("900").get().parse(BinaryStringData.fromString("123,456")))
                .isEqualTo(null);
        assertThat(NumberParser.of("0,9").get().parse(BinaryStringData.fromString("12")))
                .isEqualTo(null);
        assertThat(NumberParser.of("09").get().parse(BinaryStringData.fromString("1")))
                .isEqualTo(null);
        assertThat(NumberParser.of("099,099").get().parse(BinaryStringData.fromString("123")))
                .isEqualTo(null);

        // DECIMAL_POINT
        assertThat(NumberParser.of("900.00").get().parse(BinaryStringData.fromString("12-")))
                .isEqualTo(null);
        assertThat(NumberParser.of("900").get().parse(BinaryStringData.fromString("12.")))
                .isEqualTo(null);

        // DOLLAR_SIGN
        assertThat(NumberParser.of("L999.99").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(null);
        assertThat(NumberParser.of("SL999.99").get().parse(BinaryStringData.fromString("-")))
                .isEqualTo(null);

        // OPTIONAL_PLUS_OR_MINUS_SIGN
        assertThat(NumberParser.of("SL999.99").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(null);
        assertThat(NumberParser.of("999.99S").get().parse(BinaryStringData.fromString("1.2$")))
                .isEqualTo(null);

        // OPTIONAL_MINUS_SIGN
        assertThat(NumberParser.of("MIL999.99").get().parse(BinaryStringData.fromString("1.2")))
                .isEqualTo(null);
        assertThat(NumberParser.of("999.99MI").get().parse(BinaryStringData.fromString("1.2$")))
                .isEqualTo(null);
        assertThat(NumberParser.of("999.99MI").get().parse(BinaryStringData.fromString("1.2+")))
                .isEqualTo(null);

        // ANGLE_BRACKET
        assertThat(
                        NumberParser.of("999.99PR")
                                .get()
                                .parse(BinaryStringData.fromString("<<123.45>>")))
                .isEqualTo(null);
        assertThat(NumberParser.of("999.99PR").get().parse(BinaryStringData.fromString("123.45>")))
                .isEqualTo(null);
        assertThat(NumberParser.of("999.99PR").get().parse(BinaryStringData.fromString("<123.45")))
                .isEqualTo(null);

        // not enough token
        assertThat(NumberParser.of("000").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(null);
    }

    @Test
    void testParseMatching() {
        // DIGIT_GROUP
        // white space
        assertThat(
                        NumberParser.of("099.099")
                                .get()
                                .parse(BinaryStringData.fromString("1 2  3.4\n5\f6")))
                .isEqualTo(DecimalData.fromUnscaledLong(123456L, 6, 3));
        assertThat(NumberParser.of("900.99").get().parse(BinaryStringData.fromString("123 ")))
                .isEqualTo(DecimalData.fromUnscaledLong(12300L, 5, 2));
        assertThat(NumberParser.of("9099").get().parse(BinaryStringData.fromString("  123 ")))
                .isEqualTo(DecimalData.fromUnscaledLong(123L, 4, 0));
        // AT_MOST_AS_MANY_DIGITS
        assertThat(NumberParser.of("999.999").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(123450L, 6, 3));
        assertThat(
                        NumberParser.of("999,999.999")
                                .get()
                                .parse(BinaryStringData.fromString("123.456")))
                .isEqualTo(DecimalData.fromUnscaledLong(123456L, 9, 3));
        assertThat(NumberParser.of("900,099").get().parse(BinaryStringData.fromString("123")))
                .isEqualTo(DecimalData.fromUnscaledLong(123L, 6, 0));
        assertThat(NumberParser.of("999.99").get().parse(BinaryStringData.fromString("12")))
                .isEqualTo(DecimalData.fromUnscaledLong(1200L, 5, 2));

        // DECIMAL_POINT
        assertThat(NumberParser.of("900.90").get().parse(BinaryStringData.fromString(".1")))
                .isEqualTo(DecimalData.fromUnscaledLong(10L, 2, 2));
        assertThat(NumberParser.of("099.").get().parse(BinaryStringData.fromString("123")))
                .isEqualTo(DecimalData.fromUnscaledLong(123L, 3, 0));
        assertThat(NumberParser.of("099.99").get().parse(BinaryStringData.fromString("123.")))
                .isEqualTo(DecimalData.fromUnscaledLong(12300L, 5, 2));

        // DOLLAR_SIGN
        assertThat(NumberParser.of("L999.99").get().parse(BinaryStringData.fromString("$123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345L, 5, 2));

        // OPTIONAL_PLUS_OR_MINUS_SIGN
        assertThat(NumberParser.of("S999.99").get().parse(BinaryStringData.fromString("-123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(-12345L, 5, 2));
        assertThat(NumberParser.of("999.99S").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345L, 5, 2));
        assertThat(NumberParser.of("S999.99").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345L, 5, 2));
        assertThat(NumberParser.of("S999.99").get().parse(BinaryStringData.fromString("+123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345L, 5, 2));

        // OPTIONAL_MINUS_SIGN
        assertThat(NumberParser.of("MI999.99").get().parse(BinaryStringData.fromString("-123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(-12345L, 5, 2));
        assertThat(NumberParser.of("MI999.99").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345L, 5, 2));
        assertThat(NumberParser.of("999.99MI").get().parse(BinaryStringData.fromString("123.45-")))
                .isEqualTo(DecimalData.fromUnscaledLong(-12345L, 5, 2));
        assertThat(NumberParser.of("999.99MI").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345L, 5, 2));

        // ANGLE_BRACKET
        assertThat(NumberParser.of("999.99PR").get().parse(BinaryStringData.fromString("<123.45>")))
                .isEqualTo(DecimalData.fromUnscaledLong(-12345L, 5, 2));
        assertThat(NumberParser.of("999.99PR").get().parse(BinaryStringData.fromString("123.45")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345L, 5, 2));
    }

    @Test
    void testParseResultToDecimalValue() {
        // append zeros
        assertThat(
                        NumberParser.of("S$999,099G999.90PR")
                                .get()
                                .parse(BinaryStringData.fromString("<-$123,456,789.0>")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345678900L, 11, 2));
        assertThat(
                        NumberParser.of("S$999,099G999.00PR")
                                .get()
                                .parse(BinaryStringData.fromString("<-$123,456,789>")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345678900L, 11, 2));

        // multiple minus sign
        assertThat(
                        NumberParser.of("S999,000.99PR")
                                .get()
                                .parse(BinaryStringData.fromString("<-123,456.78>")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345678L, 8, 2));
        assertThat(
                        NumberParser.of("S999,000.99MI")
                                .get()
                                .parse(BinaryStringData.fromString("-123,456.78-")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345678L, 8, 2));
        assertThat(
                        NumberParser.of("MI999,000.99PR")
                                .get()
                                .parse(BinaryStringData.fromString("<-123,456.78>")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345678L, 8, 2));

        // fromUnscaledLong
        assertThat(
                        NumberParser.of("999,000.99")
                                .get()
                                .parse(BinaryStringData.fromString("123,456.78")))
                .isEqualTo(DecimalData.fromUnscaledLong(12345678L, 8, 2));

        // fromBigDecimal
        assertThat(
                        NumberParser.of("999,999,999,999,999,999,999")
                                .get()
                                .parse(BinaryStringData.fromString("123,456,789,123,456,789,123")))
                .isEqualTo(
                        DecimalData.fromBigDecimal(new BigDecimal("123456789123456789123"), 21, 0));
        assertThat(
                        NumberParser.of("0999999999,0999999999,0999999999,0999999999")
                                .get()
                                .parse(
                                        BinaryStringData.fromString(
                                                "9999999999,0999999999,0999999999,0999999999")))
                .isEqualTo(
                        DecimalData.fromBigDecimal(
                                new BigDecimal("9999999999099999999909999999990999999999"), 40, 0));
    }
}
