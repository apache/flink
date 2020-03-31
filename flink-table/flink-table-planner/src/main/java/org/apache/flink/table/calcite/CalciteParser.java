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

package org.apache.flink.table.calcite;

import org.apache.flink.table.api.SqlParserException;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Thin wrapper around {@link SqlParser} that does exception conversion and {@link SqlNode} casting.
 */
public class CalciteParser {
	private final SqlParser.Config config;

	public CalciteParser(SqlParser.Config config) {
		this.config = config;
	}

	/**
	 * Parses a SQL statement into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
	 *
	 * @param sql a sql string to parse
	 * @return a parsed sql node
	 * @throws SqlParserException if an exception is thrown when parsing the statement
	 */
	public SqlNode parse(String sql) {
		try {
			SqlParser parser = SqlParser.create(sql, config);
			return parser.parseStmt();
		} catch (SqlParseException e) {
			throw new SqlParserException("SQL parse failed. " + e.getMessage());
		}
	}

	/**
	 * Parses a SQL string as an identifier into a {@link SqlIdentifier}.
	 *
	 * @param identifier a sql string to parse as an identifier
	 * @return a parsed sql node
	 * @throws SqlParserException if an exception is thrown when parsing the identifier
	 */
	public SqlIdentifier parseIdentifier(String identifier) {
		try {
			SqlParser parser = SqlParser.create(identifier, config);
			SqlNode sqlNode = parser.parseExpression();
			return (SqlIdentifier) sqlNode;
		} catch (Exception e) {
			throw new SqlParserException(String.format(
				"Invalid SQL identifier %s. All SQL keywords must be escaped.", identifier));
		}
	}
}
