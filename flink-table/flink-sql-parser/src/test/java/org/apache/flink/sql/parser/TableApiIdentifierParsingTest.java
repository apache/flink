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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.impl.ParseException;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.util.SourceStringReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for parsing a Table API specific SqlIdentifier.
 */
@RunWith(Parameterized.class)
public class TableApiIdentifierParsingTest {

	private static final String ANTHROPOS_IN_GREEK_IN_UNICODE = "#03B1#03BD#03B8#03C1#03C9#03C0#03BF#03C2";
	private static final String ANTHROPOS_IN_GREEK = "ανθρωπος";

	@Parameterized.Parameters(name = "Parsing: {0}. Expected identifier: {1}")
	public static Object[][] parameters() {
		return new Object[][] {
			new Object[] {
				"array",
				singletonList("array")
			},

			new Object[] {
				"table",
				singletonList("table")
			},

			new Object[] {
				"cat.db.array",
				asList("cat", "db", "array")
			},

			new Object[] {
				"`cat.db`.table",
				asList("cat.db", "table")
			},

			new Object[] {
				"db.table",
				asList("db", "table")
			},

			new Object[] {
				"`ta``ble`",
				singletonList("ta`ble")
			},

			new Object[] {
				"`c``at`.`d``b`.`ta``ble`",
				asList("c`at", "d`b", "ta`ble")
			},

			new Object[] {
				"db.U&\"" + ANTHROPOS_IN_GREEK_IN_UNICODE + "\" UESCAPE '#'",
				asList("db", ANTHROPOS_IN_GREEK)
			},

			new Object[] {
				"db.ανθρωπος",
				asList("db", ANTHROPOS_IN_GREEK)
			}
		};
	}

	@Parameterized.Parameter
	public String stringIdentifier;

	@Parameterized.Parameter(1)
	public List<String> expectedParsedIdentifier;

	@Test
	public void testTableApiIdentifierParsing() throws ParseException {
		FlinkSqlParserImpl parser = createFlinkParser(stringIdentifier);

		SqlIdentifier sqlIdentifier = parser.TableApiIdentifier();
		assertThat(sqlIdentifier.names, equalTo(expectedParsedIdentifier));
	}

	private FlinkSqlParserImpl createFlinkParser(String expr) {
		SourceStringReader reader = new SourceStringReader(expr);
		FlinkSqlParserImpl parser = (FlinkSqlParserImpl) FlinkSqlParserImpl.FACTORY.getParser(reader);
		parser.setTabSize(1);
		parser.setUnquotedCasing(Lex.JAVA.unquotedCasing);
		parser.setQuotedCasing(Lex.JAVA.quotedCasing);
		parser.setIdentifierMaxLength(256);
		parser.setConformance(FlinkSqlConformance.DEFAULT);
		parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);

		return parser;
	}
}
