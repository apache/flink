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


public class IntParserTest extends ParserTestBase<Integer> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "1", "576", "-877678", String.valueOf(Integer.MAX_VALUE), String.valueOf(Integer.MIN_VALUE)
		};
	}
	
	@Override
	public Integer[] getValidTestResults() {
		return new Integer[] {
			0, 1, 576, -877678, Integer.MAX_VALUE, Integer.MIN_VALUE
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"a", "1569a86", "-57-6", "7-877678", String.valueOf(Integer.MAX_VALUE) + "0", String.valueOf(Long.MIN_VALUE),
			String.valueOf(((long) Integer.MAX_VALUE) + 1), String.valueOf(((long) Integer.MIN_VALUE) - 1)
		};
	}

	@Override
	public FieldParser<Integer> getParser() {
		return new IntParser();
	}

	@Override
	public Class<Integer> getTypeClass() {
		return Integer.class;
	}
}
