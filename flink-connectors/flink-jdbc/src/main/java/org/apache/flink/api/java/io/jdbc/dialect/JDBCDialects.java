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

package org.apache.flink.api.java.io.jdbc.dialect;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Default Jdbc dialects.
 */
public final class JDBCDialects {

	private static final List<JDBCDialect> DIALECTS = Arrays.asList(
		new DerbyDialect(),
		new MySQLDialect(),
		new PostgresDialect()
	);

	/**
	 * Fetch the JDBCDialect class corresponding to a given database url.
	 */
	public static Optional<JDBCDialect> get(String url) {
		for (JDBCDialect dialect : DIALECTS) {
			if (dialect.canHandle(url)) {
				return Optional.of(dialect);
			}
		}
		return Optional.empty();
	}

	private static class DerbyDialect implements JDBCDialect {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:derby:");
		}

		@Override
		public Optional<String> defaultDriverName() {
			return Optional.of("org.apache.derby.jdbc.EmbeddedDriver");
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return identifier;
		}
	}

	private static class MySQLDialect implements JDBCDialect {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:mysql:");
		}

		@Override
		public Optional<String> defaultDriverName() {
			return Optional.of("com.mysql.jdbc.Driver");
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return "`" + identifier + "`";
		}

		/**
		 * Mysql upsert query use DUPLICATE KEY UPDATE.
		 *
		 * <p>NOTE: It requires Mysql's primary key to be consistent with pkFields.
		 *
		 * <p>We don't use REPLACE INTO, if there are other fields, we can keep their previous values.
		 */
		@Override
		public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
			String updateClause = Arrays.stream(fieldNames)
					.map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
					.collect(Collectors.joining(", "));
			return Optional.of(getInsertIntoStatement(tableName, fieldNames) +
					" ON DUPLICATE KEY UPDATE " + updateClause
			);
		}
	}

	private static class PostgresDialect implements JDBCDialect {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:postgresql:");
		}

		@Override
		public Optional<String> defaultDriverName() {
			return Optional.of("org.postgresql.Driver");
		}

		/**
		 * Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres.
		 */
		@Override
		public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
			String uniqueColumns = Arrays.stream(uniqueKeyFields)
					.map(this::quoteIdentifier)
					.collect(Collectors.joining(", "));
			String updateClause = Arrays.stream(fieldNames)
					.map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
					.collect(Collectors.joining(", "));
			return Optional.of(getInsertIntoStatement(tableName, fieldNames) +
							" ON CONFLICT (" + uniqueColumns +
							" DO UPDATE SET " + updateClause
			);
		}
	}
}
