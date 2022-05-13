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

import java.math.BigInteger;

public class BigIntParserTest extends ParserTestBase<BigInteger> {

    @Override
    public String[] getValidTestValues() {
        return new String[] {
            "-8745979691234123413478523984729447",
            "-10000",
            "-1",
            "0",
            "0000000",
            "8745979691234123413478523984729447"
        };
    }

    @Override
    public BigInteger[] getValidTestResults() {
        return new BigInteger[] {
            new BigInteger("-8745979691234123413478523984729447"),
            new BigInteger("-10000"),
            new BigInteger("-1"),
            new BigInteger("0"),
            new BigInteger("0"),
            new BigInteger("8745979691234123413478523984729447")
        };
    }

    @Override
    public String[] getInvalidTestValues() {
        return new String[] {"1.1", "a", "123abc4", "-57-6", "7-877678", " 1", "2 ", " ", "\t"};
    }

    @Override
    public boolean allowsEmptyField() {
        return false;
    }

    @Override
    public FieldParser<BigInteger> getParser() {
        return new BigIntParser();
    }

    @Override
    public Class<BigInteger> getTypeClass() {
        return BigInteger.class;
    }
}
