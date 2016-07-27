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


import java.sql.Time;

public class SqlTimeParserTest extends ParserTestBase<Time> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"00:00:00", "02:42:25", "14:15:51", "18:00:45", "23:59:58", "0:0:0"
		};
	}

	@Override
	public Time[] getValidTestResults() {
		return new Time[] {
			Time.valueOf("00:00:00"), Time.valueOf("02:42:25"), Time.valueOf("14:15:51"),
			Time.valueOf("18:00:45"), Time.valueOf("23:59:58"), Time.valueOf("0:0:0")
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			" 00:00:00", "00:00:00 ", "00:00::00", "00x00:00", "2013/08/12", " ", "\t"
		};
	}

	@Override
	public boolean allowsEmptyField() {
		return false;
	}

	@Override
	public FieldParser<Time> getParser() {
		return new SqlTimeParser();
	}

	@Override
	public Class<Time> getTypeClass() {
		return Time.class;
	}
}
