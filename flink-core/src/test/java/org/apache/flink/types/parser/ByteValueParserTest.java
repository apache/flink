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

import org.apache.flink.types.ByteValue;

public class ByteValueParserTest extends ParserTestBase<ByteValue> {

    @Override
    public String[] getValidTestValues() {
        return new String[] {
            "0",
            "1",
            "76",
            "-66",
            String.valueOf(Byte.MAX_VALUE),
            String.valueOf(Byte.MIN_VALUE),
            "19"
        };
    }

    @Override
    public ByteValue[] getValidTestResults() {
        return new ByteValue[] {
            new ByteValue((byte) 0),
            new ByteValue((byte) 1),
            new ByteValue((byte) 76),
            new ByteValue((byte) -66),
            new ByteValue(Byte.MAX_VALUE),
            new ByteValue(Byte.MIN_VALUE),
            new ByteValue((byte) 19)
        };
    }

    @Override
    public String[] getInvalidTestValues() {
        return new String[] {
            "a",
            "9a",
            "-57-6",
            "7-88",
            String.valueOf(Byte.MAX_VALUE) + "0",
            String.valueOf(Short.MIN_VALUE),
            String.valueOf(Byte.MAX_VALUE + 1),
            String.valueOf(Byte.MIN_VALUE - 1),
            " 1",
            "2 ",
            " ",
            "\t"
        };
    }

    @Override
    public boolean allowsEmptyField() {
        return false;
    }

    @Override
    public FieldParser<ByteValue> getParser() {
        return new ByteValueParser();
    }

    @Override
    public Class<ByteValue> getTypeClass() {
        return ByteValue.class;
    }
}
