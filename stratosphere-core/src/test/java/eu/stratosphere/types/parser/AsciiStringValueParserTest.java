/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.types.parser;

import eu.stratosphere.types.StringValue;


public class AsciiStringValueParserTest extends ParserTestBase<StringValue> {

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
	public StringValue[] getValidTestResults() {
		return new StringValue[] {
			new StringValue("abcdefgh"), new StringValue("i"), new StringValue("jklmno"),
			new StringValue("abcdefgh"), new StringValue("i"), new StringValue("jklmno"), 
			new StringValue("ab,cde|fg"), new StringValue("hij|m|n|op"),
			new StringValue("abcdefgh"), new StringValue("i"), new StringValue("jklmno"),
			new StringValue("     abcd    ")
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"  \"abcdefgh ", "  \"ijklmno\" hj"
		};
	}

	@Override
	public FieldParser<StringValue> getParser() {
		return new VarLengthStringParser();
	}

	@Override
	public Class<StringValue> getTypeClass() {
		return StringValue.class;
	}
}
