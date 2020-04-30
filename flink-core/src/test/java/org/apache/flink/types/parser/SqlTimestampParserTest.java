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


import java.sql.Timestamp;

public class SqlTimestampParserTest extends ParserTestBase<Timestamp> {

	@Override
	public String[] getValidTestValues() {
		return new String[] {
			"1970-01-01 00:00:00.000", "1990-10-14 02:42:25", "1990-10-14 02:42:25.123", "1990-10-14 02:42:25.123000001",
			"1990-10-14 02:42:25.123000002", "2013-08-12 14:15:59.478", "2013-08-12 14:15:59.47",
			"0000-01-01 00:00:00.000",
		};
	}

	@Override
	public Timestamp[] getValidTestResults() {
		return new Timestamp[] {
			Timestamp.valueOf("1970-01-01 00:00:00.000"), Timestamp.valueOf("1990-10-14 02:42:25"), Timestamp.valueOf("1990-10-14 02:42:25.123"),
			Timestamp.valueOf("1990-10-14 02:42:25.123000001"), Timestamp.valueOf("1990-10-14 02:42:25.123000002"),
			Timestamp.valueOf("2013-08-12 14:15:59.478"), Timestamp.valueOf("2013-08-12 14:15:59.47"),
			Timestamp.valueOf("0000-01-01 00:00:00.000")
		};
	}

	@Override
	public String[] getInvalidTestValues() {
		return new String[] {
			" 2013-08-12 14:15:59.479", "2013-08-12 14:15:59.479 ", "1970-01-01 00:00::00",
			"00x00:00", "2013/08/12", "0000-01-01 00:00:00.f00", "2013-08-12 14:15:59.4788888888888888",
			" ", "\t"
		};
	}

	@Override
	public boolean allowsEmptyField() {
		return false;
	}

	@Override
	public FieldParser<Timestamp> getParser() {
		return new SqlTimestampParser();
	}

	@Override
	public Class<Timestamp> getTypeClass() {
		return Timestamp.class;
	}
}
