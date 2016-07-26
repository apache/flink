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


public class ShortParserTest extends ParserTestBase<Short> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "1", "576", "-8778", String.valueOf(Short.MAX_VALUE), String.valueOf(Short.MIN_VALUE), "119"
		};
	}
	
	@Override
	public Short[] getValidTestResults() {
		return new Short[] {
			(short) 0, (short)1, (short)576, (short) -8778, Short.MAX_VALUE, Short.MIN_VALUE, (short) 119
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"a", "1569a86", "-57-6", "7-877678", String.valueOf(Short.MAX_VALUE) + "0", String.valueOf(Integer.MIN_VALUE),
			String.valueOf(Short.MAX_VALUE + 1), String.valueOf(Short.MIN_VALUE - 1), " 1", "2 ", " ", "\t"
		};
	}

	@Override
	public boolean allowsEmptyField() {
		return false;
	}

	@Override
	public FieldParser<Short> getParser() {
		return new ShortParser();
	}

	@Override
	public Class<Short> getTypeClass() {
		return Short.class;
	}
}
