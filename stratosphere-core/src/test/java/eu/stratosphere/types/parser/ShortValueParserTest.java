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

import eu.stratosphere.types.ShortValue;


public class ShortValueParserTest extends ParserTestBase<ShortValue> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "1", "576", "-8778", String.valueOf(Short.MAX_VALUE), String.valueOf(Short.MIN_VALUE)
		};
	}
	
	@Override
	public ShortValue[] getValidTestResults() {
		return new ShortValue[] {
			new ShortValue((short) 0), new ShortValue((short) 1), new ShortValue((short) 576),
			new ShortValue((short) -8778), new ShortValue(Short.MAX_VALUE), new ShortValue(Short.MIN_VALUE)
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"a", "1569a86", "-57-6", "7-877678", String.valueOf(Short.MAX_VALUE) + "0", String.valueOf(Integer.MIN_VALUE),
			String.valueOf(Short.MAX_VALUE + 1), String.valueOf(Short.MIN_VALUE - 1)
		};
	}

	@Override
	public FieldParser<ShortValue> getParser() {
		return new DecimalTextShortParser();
	}

	@Override
	public Class<ShortValue> getTypeClass() {
		return ShortValue.class;
	}
}
