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


public class LongParserTest extends ParserTestBase<Long> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "1", "576", "-877678", String.valueOf(Integer.MAX_VALUE), String.valueOf(Integer.MIN_VALUE),
			String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MIN_VALUE), "7656"
		};
	}
	
	@Override
	public Long[] getValidTestResults() {
		return new Long[] {
			0L, 1L, 576L, -877678L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, 7656L
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"a", "1569a86", "-57-6", "7-877678", String.valueOf(Long.MAX_VALUE) + "0", String.valueOf(Long.MIN_VALUE) + "0",
			"9223372036854775808", "-9223372036854775809"
		};
	}

	@Override
	public FieldParser<Long> getParser() {
		return new LongParser();
	}

	@Override
	public Class<Long> getTypeClass() {
		return Long.class;
	}
}
