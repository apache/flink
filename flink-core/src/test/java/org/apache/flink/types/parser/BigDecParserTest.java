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

package org.apache.flink.types.parser;

import java.math.BigDecimal;

public class BigDecParserTest extends ParserTestBase<BigDecimal> {

    @Override
    public String[] getValidTestValues() {
        return new String[] {
            "-12.5E1000",
            "-12.5E100",
            "-10000",
            "-1.1",
            "-1",
            "-0.44",
            "0",
            "0000000",
            "0e0",
            "0.000000000000000000000000001",
            "0.0000001",
            "0.1234123413478523984729447",
            "1",
            "10000",
            "10E100000",
            "10E1000000000"
        };
    }

    @Override
    public BigDecimal[] getValidTestResults() {
        return new BigDecimal[] {
            new BigDecimal("-12.5E1000"),
            new BigDecimal("-12.5E100"),
            new BigDecimal("-10000"),
            new BigDecimal("-1.1"),
            new BigDecimal("-1"),
            new BigDecimal("-0.44"),
            new BigDecimal("0"),
            new BigDecimal("0"),
            new BigDecimal("0e0"),
            new BigDecimal("0.000000000000000000000000001"),
            new BigDecimal("0.0000001"),
            new BigDecimal("0.1234123413478523984729447"),
            new BigDecimal("1"),
            new BigDecimal("10000"),
            new BigDecimal("10E100000"),
            new BigDecimal("10E1000000000"),
        };
    }

    @Override
    public String[] getInvalidTestValues() {
        return new String[] {"a", "123abc4", "-57-6", "7-877678", " 1", "2 ", " ", "\t"};
    }

    @Override
    public boolean allowsEmptyField() {
        return false;
    }

    @Override
    public FieldParser<BigDecimal> getParser() {
        return new BigDecParser();
    }

    @Override
    public Class<BigDecimal> getTypeClass() {
        return BigDecimal.class;
    }
}
