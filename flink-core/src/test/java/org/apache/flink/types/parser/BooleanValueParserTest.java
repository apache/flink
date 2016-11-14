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

import org.apache.flink.types.BooleanValue;


public class BooleanValueParserTest extends ParserTestBase<BooleanValue> {


	@Override
	public String[] getValidTestValues() {
		return new String[] {
				"true", "false", "0", "1", "TRUE", "FALSE", "True", "False"
		};
	}

	@Override
	public BooleanValue[] getValidTestResults() {
		return new BooleanValue[] {
				new BooleanValue(true), new BooleanValue(false), new BooleanValue(false), new BooleanValue(true),
				new BooleanValue(true), new BooleanValue(false), new BooleanValue(true), new BooleanValue(false)
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[]{
				"yes", "no", "2", "-1", "wahr", "falsch", "", "asdf"
		};
	}

	@Override
	public boolean allowsEmptyField() {
		return false;
	}

	@Override
	public FieldParser<BooleanValue> getParser() {
		return new BooleanValueParser();
	}

	@Override
	public Class<BooleanValue> getTypeClass() {
		return BooleanValue.class;
	}
}
