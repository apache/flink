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


public class FloatParserTest extends ParserTestBase<Float> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "0.0", "123.4", "0.124", ".623", "1234", "-12.34", 
			String.valueOf(Float.MAX_VALUE), String.valueOf(Float.MIN_VALUE),
			String.valueOf(Float.NEGATIVE_INFINITY), String.valueOf(Float.POSITIVE_INFINITY),
			String.valueOf(Float.NaN),
			"1.234E2", "1.234e3", "1.234E-2", "1239"
		};
	}
	
	@Override
	public Float[] getValidTestResults() {
		return new Float[] {
			0f, 0.0f, 123.4f, 0.124f, .623f, 1234f, -12.34f, 
			Float.MAX_VALUE, Float.MIN_VALUE,
			Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY,
			Float.NaN,
			1.234E2f, 1.234e3f, 1.234E-2f, 1239f
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
	public FieldParser<Float> getParser() {
		return new FloatParser();
	}

	@Override
	public Class<Float> getTypeClass() {
		return Float.class;
	}
}
