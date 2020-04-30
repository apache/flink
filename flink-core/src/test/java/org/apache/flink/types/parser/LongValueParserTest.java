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

import org.apache.flink.types.LongValue;


public class LongValueParserTest extends ParserTestBase<LongValue> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "1", "576", "-877678", String.valueOf(Integer.MAX_VALUE), String.valueOf(Integer.MIN_VALUE),
			String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MIN_VALUE), "7656", "1239"
		};
	}
	
	@Override
	public LongValue[] getValidTestResults() {
		return new LongValue[] {
			new LongValue(0L), new LongValue(1L), new LongValue(576L), new LongValue(-877678L),
			new LongValue((long) Integer.MAX_VALUE), new LongValue((long) Integer.MIN_VALUE),
			new LongValue(Long.MAX_VALUE), new LongValue(Long.MIN_VALUE), new LongValue(7656L), new LongValue(1239L)
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"a", "1569a86", "-57-6", "7-877678", String.valueOf(Long.MAX_VALUE) + "0", String.valueOf(Long.MIN_VALUE) + "0",
			"9223372036854775808", "-9223372036854775809", " 1", "2 ", " ", "\t"
		};
	}

	@Override
	public boolean allowsEmptyField() {
		return false;
	}

	@Override
	public FieldParser<LongValue> getParser() {
		return new LongValueParser();
	}

	@Override
	public Class<LongValue> getTypeClass() {
		return LongValue.class;
	}
}
