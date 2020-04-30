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

import org.apache.flink.types.DoubleValue;


public class DoubleValueParserTest extends ParserTestBase<DoubleValue> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "0.0", "123.4", "0.124", ".623", "1234", "-12.34", 
			String.valueOf(Double.MAX_VALUE), String.valueOf(Double.MIN_VALUE),
			String.valueOf(Double.NEGATIVE_INFINITY), String.valueOf(Double.POSITIVE_INFINITY),
			String.valueOf(Double.NaN),
			"1.234E2", "1.234e3", "1.234E-2", "1239"
		};
	}
	
	@Override
	public DoubleValue[] getValidTestResults() {
		return new DoubleValue[] {
			new DoubleValue(0d), new DoubleValue(0.0d), new DoubleValue(123.4d), new DoubleValue(0.124d),
			new DoubleValue(.623d), new DoubleValue(1234d), new DoubleValue(-12.34d),
			new DoubleValue(Double.MAX_VALUE), new DoubleValue(Double.MIN_VALUE),
			new DoubleValue(Double.NEGATIVE_INFINITY), new DoubleValue(Double.POSITIVE_INFINITY),
			new DoubleValue(Double.NaN),
			new DoubleValue(1.234E2), new DoubleValue(1.234e3), new DoubleValue(1.234E-2), new DoubleValue(1239d)
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"a", "123abc4", "-57-6", "7-877678", " 1", "2 ", " ", "\t"
		};
	}

	@Override
	public boolean allowsEmptyField() {
		return false;
	}

	@Override
	public FieldParser<DoubleValue> getParser() {
		return new DoubleValueParser();
	}

	@Override
	public Class<DoubleValue> getTypeClass() {
		return DoubleValue.class;
	}
}
