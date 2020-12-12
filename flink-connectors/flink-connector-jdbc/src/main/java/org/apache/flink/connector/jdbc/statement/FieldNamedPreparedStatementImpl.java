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

package org.apache.flink.connector.jdbc.statement;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple implementation of {@link FieldNamedPreparedStatement}.
 */
public class FieldNamedPreparedStatementImpl implements FieldNamedPreparedStatement {

	private final PreparedStatement statement;
	private final int[][] indexMapping;

	private FieldNamedPreparedStatementImpl(PreparedStatement statement, int[][] indexMapping) {
		this.statement = statement;
		this.indexMapping = indexMapping;
	}

	@Override
	public void clearParameters() throws SQLException {
		statement.clearParameters();
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return statement.executeQuery();
	}

	@Override
	public void addBatch() throws SQLException {
		statement.addBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return statement.executeBatch();
	}

	@Override
	public void setNull(int fieldIndex, int sqlType) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setNull(index, sqlType);
		}
	}

	@Override
	public void setBoolean(int fieldIndex, boolean x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setBoolean(index, x);
		}
	}

	@Override
	public void setByte(int fieldIndex, byte x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setByte(index, x);
		}
	}

	@Override
	public void setShort(int fieldIndex, short x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setShort(index, x);
		}
	}

	@Override
	public void setInt(int fieldIndex, int x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setInt(index, x);
		}
	}

	@Override
	public void setLong(int fieldIndex, long x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setLong(index, x);
		}
	}

	@Override
	public void setFloat(int fieldIndex, float x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setFloat(index, x);
		}
	}

	@Override
	public void setDouble(int fieldIndex, double x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setDouble(index, x);
		}
	}

	@Override
	public void setBigDecimal(int fieldIndex, BigDecimal x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setBigDecimal(index, x);
		}
	}

	@Override
	public void setString(int fieldIndex, String x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setString(index, x);
		}
	}

	@Override
	public void setBytes(int fieldIndex, byte[] x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setBytes(index, x);
		}
	}

	@Override
	public void setDate(int fieldIndex, Date x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setDate(index, x);
		}
	}

	@Override
	public void setTime(int fieldIndex, Time x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setTime(index, x);
		}
	}

	@Override
	public void setTimestamp(int fieldIndex, Timestamp x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setTimestamp(index, x);
		}
	}

	@Override
	public void setObject(int fieldIndex, Object x) throws SQLException {
		for (int index : indexMapping[fieldIndex]) {
			statement.setObject(index, x);
		}
	}

	@Override
	public void close() throws SQLException {
		statement.close();
	}

	// ----------------------------------------------------------------------------------------

	public static FieldNamedPreparedStatement prepareStatement(Connection connection, String sql, String[] fieldNames) throws SQLException {
		checkNotNull(connection, "connection must not be null.");
		checkNotNull(sql, "sql must not be null.");
		checkNotNull(fieldNames, "fieldNames must not be null.");

		if (sql.contains("?")) {
			throw new IllegalArgumentException("SQL statement must not contain ? character.");
		}

		HashMap<String, List<Integer>> parameterMap = new HashMap<>();
		String parsedSQL = parseNamedStatement(sql, parameterMap);
		// currently, the statements must contain all the field parameters
		checkArgument(parameterMap.size() == fieldNames.length);
		int[][] indexMapping = new int[fieldNames.length][];
		for (int i = 0; i < fieldNames.length; i++) {
			String fieldName = fieldNames[i];
			checkArgument(
				parameterMap.containsKey(fieldName),
				fieldName + " doesn't exist in the parameters of SQL statement: " + sql);
			indexMapping[i] = parameterMap
				.get(fieldName)
				.stream()
				.mapToInt(v -> v).toArray();
		}

		return new FieldNamedPreparedStatementImpl(connection.prepareStatement(parsedSQL), indexMapping);
	}

	/**
	 * Parses a sql with named parameters. The parameter-index mappings are put into the map,
	 * and the parsed sql is returned.
	 * @param sql    sql to parse
	 * @param paramMap map to hold parameter-index mappings
	 * @return the parsed sql
	 */
	public static String parseNamedStatement(String sql, Map<String, List<Integer>> paramMap) {
		StringBuilder parsedSql = new StringBuilder();
		int fieldIndex = 1; // SQL statement parameter index starts from 1
		int length = sql.length();
		for (int i = 0; i < length; i++) {
			char c = sql.charAt(i);
			if (':' == c) {
				int j = i + 1;
				while (j < length && Character.isJavaIdentifierPart(sql.charAt(j))) {
					j++;
				}
				String parameterName = sql.substring(i + 1, j);
				checkArgument(!parameterName.isEmpty(), "Named parameters in SQL statement must not be empty.");
				paramMap.computeIfAbsent(parameterName, n -> new ArrayList<>()).add(fieldIndex);
				fieldIndex++;
				i = j - 1;
				parsedSql.append('?');
			} else {
				parsedSql.append(c);
			}
		}
		return parsedSql.toString();
	}
}
