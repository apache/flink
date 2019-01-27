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
import org.apache.flink.sql.parser.util.SqlValidatorTestCase;
import org.apache.flink.sql.parser.util.TestUtil;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Util;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import static org.junit.Assert.assertThat;

/**
 *
 */
public abstract class ParserTestBase {

	protected String readFile(String path) throws IOException {
		URL url = getClass().getClassLoader().getResource(path);

		assert url != null;
		File file = new File(url.getFile());
		return Files.toString(file, Charset.forName("UTF-8"));
	}

	private static final ThreadLocal<boolean[]> LINUXIFY =
		new ThreadLocal<boolean[]>() {
			@Override
			protected boolean[] initialValue() {
				return new boolean[]{true};
			}
		};

	// Helper functions -------------------------------------------------------

	protected Tester getTester() {
		return new TesterImpl();
	}

	protected void check(
		String sql,
		String expected) {
		sql(sql).ok(expected);
	}

	protected Sql sql(String sql) {
		return new Sql(sql);
	}

	/**
	 * Implementors of custom parsing logic who want to reuse this test should
	 * override this method with the factory for their extension parser.
	 */
	protected SqlParserImplFactory parserImplFactory() {
		return FlinkSqlParserImpl.FACTORY;
	}

	protected SqlParser.Config getParserConfig() {
		return SqlParser.configBuilder()
			.setParserFactory(parserImplFactory())
			.setQuoting(Quoting.BACK_TICK)
			.setUnquotedCasing(Casing.UNCHANGED)
			.setQuotedCasing(Casing.UNCHANGED)
			.setConformance(SqlConformanceEnum.DEFAULT)
			.setIdentifierMaxLength(256)
			.setLex(Lex.JAVA)
			.build();
	}

	protected SqlParser getSqlParser(String sql) {
		return SqlParser.create(sql, getParserConfig());
	}

	protected void checkExp(
		String sql,
		String expected) {
		getTester().checkExp(sql, expected);
	}

	protected void checkExpSame(String sql) {
		checkExp(sql, sql);
	}

	/**
	 * Returns a {@link Matcher} that succeeds if the given {@link SqlNode} is a
	 * DDL statement.
	 */
	public static Matcher<SqlNode> isDdl() {
		return new BaseMatcher<SqlNode>() {
			public boolean matches(Object item) {
				return item instanceof SqlNode
					&& SqlKind.DDL.contains(((SqlNode) item).getKind());
			}

			public void describeTo(Description description) {
				description.appendText("isDdl");
			}
		};
	}

	//~ Inner Interfaces -------------------------------------------------------

	/**
	 * Callback to control how test actions are performed.
	 */
	protected interface Tester {
		void check(String sql, String expected);

		void checkExp(String sql, String expected);

		void checkFails(String sql, String expectedMsgPattern);

		void checkNode(String sql, Matcher<SqlNode> matcher);
	}

	//~ Inner Classes ----------------------------------------------------------

	/**
	 * Default implementation of {@link Tester}.
	 */
	protected class TesterImpl implements Tester {
		public void check(
			String sql,
			String expected) {
			final SqlNode sqlNode = parseStmtAndHandleEx(sql);

			// no dialect, always parenthesize
			String actual = sqlNode.toSqlString(null, true).getSql();
			if (LINUXIFY.get()[0]) {
				actual = Util.toLinux(actual);
			}
			TestUtil.assertEqualsVerbose(expected, actual);
		}

		public void checkFails(
			String sql,
			String expectedMsgPattern) {
			SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
			Throwable thrown = null;
			try {
				final SqlNode sqlNode = getSqlParser(sap.sql).parseStmt();
				Util.discard(sqlNode);
			} catch (Throwable ex) {
				thrown = ex;
			}

			SqlValidatorTestCase.checkEx(thrown, expectedMsgPattern, sap);
		}

		protected SqlNode parseStmtAndHandleEx(String sql) {
			final SqlNode sqlNode;
			try {
				sqlNode = getSqlParser(sql).parseStmt();
			} catch (SqlParseException e) {
				throw new RuntimeException("Error while parsing SQL: " + sql, e);
			}
			return sqlNode;
		}

		public void checkExp(
			String sql,
			String expected) {
			final SqlNode sqlNode = parseExpressionAndHandleEx(sql);
			String actual = sqlNode.toSqlString(null, true).getSql();
			if (LINUXIFY.get()[0]) {
				actual = Util.toLinux(actual);
			}
			TestUtil.assertEqualsVerbose(expected, actual);
		}

		protected SqlNode parseExpressionAndHandleEx(String sql) {
			final SqlNode sqlNode;
			try {
				sqlNode = getSqlParser(sql).parseExpression();
			} catch (SqlParseException e) {
				throw new RuntimeException("Error while parsing expression: " + sql, e);
			}
			return sqlNode;
		}

		public void checkNode(String sql, Matcher<SqlNode> matcher) {
			SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
			try {
				final SqlNode sqlNode = getSqlParser(sap.sql).parseStmt();
				assertThat(sqlNode, matcher);
			} catch (SqlParseException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 *
	 */
	protected class Sql {
		private final String sql;

		Sql(String sql) {
			this.sql = sql;
		}

		public Sql ok(String expected) {
			getTester().check(sql, expected);
			return this;
		}

		public Sql fails(String expectedMsgPattern) {
			getTester().checkFails(sql, expectedMsgPattern);
			return this;
		}

		public Sql node(Matcher<SqlNode> matcher) {
			getTester().checkNode(sql, matcher);
			return this;
		}
	}
}
