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

import org.apache.flink.types.parser.StringParser;
import org.apache.flink.types.parser.FieldParser;


public class StringParserTest extends ParserTestBase<String> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"abcdefgh", "i", "jklmno", "\"abcdefgh\"", "\"i\"", "\"jklmno\"", 
			"\"ab,cde|fg\"", "\"hij|m|n|op\"",
			"  \"abcdefgh\"", "     \"i\"\t\t\t", "\t \t\"jklmno\"  ",
			"  \"     abcd    \" \t "
		};
	}
	
	@Override
	public String[] getValidTestResults() {
		return new String[] {
			"abcdefgh", "i", "jklmno", "abcdefgh", "i", "jklmno", 
			"ab,cde|fg", "hij|m|n|op",
			"abcdefgh", "i", "jklmno",
			"     abcd    "
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"  \"abcdefgh ", "  \"ijklmno\" hj"
		};
	}

	@Override
	public FieldParser<String> getParser() {
		return new StringParser();
	}

	@Override
	public Class<String> getTypeClass() {
		return String.class;
	}
}
