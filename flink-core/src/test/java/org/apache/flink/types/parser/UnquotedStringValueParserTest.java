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

import org.apache.flink.types.StringValue;

public class UnquotedStringValueParserTest extends ParserTestBase<StringValue> {

    @Override
    public String[] getValidTestValues() {
        return new String[] {"abcdefgh", "i", "jklmno", "\"abc\"defgh\"", "\"i\"", "Hello9"};
    }

    @Override
    public StringValue[] getValidTestResults() {
        return new StringValue[] {
            new StringValue("abcdefgh"), new StringValue("i"), new StringValue("jklmno"),
            new StringValue("\"abc\"defgh\""), new StringValue("\"i\""), new StringValue("Hello9")
        };
    }

    @Override
    public String[] getInvalidTestValues() {
        return new String[] {};
    }

    @Override
    public boolean allowsEmptyField() {
        return true;
    }

    @Override
    public FieldParser<StringValue> getParser() {
        return new StringValueParser();
    }

    @Override
    public Class<StringValue> getTypeClass() {
        return StringValue.class;
    }
}
