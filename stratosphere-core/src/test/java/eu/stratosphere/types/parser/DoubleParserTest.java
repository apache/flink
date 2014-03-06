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


public class DoubleParserTest extends ParserTestBase<Double> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"0", "0.0", "123.4", "0.124", ".623", "1234", "-12.34", 
			String.valueOf(Double.MAX_VALUE), String.valueOf(Double.MIN_VALUE),
			String.valueOf(Double.NEGATIVE_INFINITY), String.valueOf(Double.POSITIVE_INFINITY),
			String.valueOf(Double.NaN),
			"1.234E2", "1.234e3", "1.234E-2"
		};
	}
	
	@Override
	public Double[] getValidTestResults() {
		return new Double[] {
			0d, 0.0d, 123.4d, 0.124d, .623d, 1234d, -12.34d, 
			Double.MAX_VALUE, Double.MIN_VALUE,
			Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
			Double.NaN,
			1.234E2, 1.234e3, 1.234E-2
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			"a", "123abc4", "-57-6", "7-877678"
		};
	}

	@Override
	public FieldParser<Double> getParser() {
		return new DoubleParser();
	}

	@Override
	public Class<Double> getTypeClass() {
		return Double.class;
	}
}
