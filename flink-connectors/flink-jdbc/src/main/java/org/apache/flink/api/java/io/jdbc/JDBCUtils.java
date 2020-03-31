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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Utils for jdbc connectors.
 */
public class JDBCUtils {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCUtils.class);

	/**
	 * Adds a record to the prepared statement.
	 *
	 * <p>When this method is called, the output format is guaranteed to be opened.
	 *
	 * <p>WARNING: this may fail when no column types specified (because a best effort approach is attempted in order to
	 * insert a null value but it's not guaranteed that the JDBC driver handles PreparedStatement.setObject(pos, null))
	 *
	 * @param upload The prepared statement.
	 * @param typesArray The jdbc types of the row.
	 * @param row The records to add to the output.
	 * @see PreparedStatement
	 */
	public static void setRecordToStatement(PreparedStatement upload, int[] typesArray, Row row) throws SQLException {
		if (typesArray != null && typesArray.length > 0 && typesArray.length != row.getArity()) {
			LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
		}
		if (typesArray == null) {
			// no types provided
			for (int index = 0; index < row.getArity(); index++) {
				LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.", index + 1, row.getField(index));
				upload.setObject(index + 1, row.getField(index));
			}
		} else {
			// types provided
			for (int i = 0; i < row.getArity(); i++) {
				setField(upload, typesArray[i], row.getField(i), i);
			}
		}
	}

	public static void setField(PreparedStatement upload, int type, Object field, int index) throws SQLException {
		if (field == null) {
			upload.setNull(index + 1, type);
		} else {
			try {
				// casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
				switch (type) {
					case java.sql.Types.NULL:
						upload.setNull(index + 1, type);
						break;
					case java.sql.Types.BOOLEAN:
					case java.sql.Types.BIT:
						upload.setBoolean(index + 1, (boolean) field);
						break;
					case java.sql.Types.CHAR:
					case java.sql.Types.NCHAR:
					case java.sql.Types.VARCHAR:
					case java.sql.Types.LONGVARCHAR:
					case java.sql.Types.LONGNVARCHAR:
						upload.setString(index + 1, (String) field);
						break;
					case java.sql.Types.TINYINT:
						upload.setByte(index + 1, (byte) field);
						break;
					case java.sql.Types.SMALLINT:
						upload.setShort(index + 1, (short) field);
						break;
					case java.sql.Types.INTEGER:
						upload.setInt(index + 1, (int) field);
						break;
					case java.sql.Types.BIGINT:
						upload.setLong(index + 1, (long) field);
						break;
					case java.sql.Types.REAL:
						upload.setFloat(index + 1, (float) field);
						break;
					case java.sql.Types.FLOAT:
					case java.sql.Types.DOUBLE:
						upload.setDouble(index + 1, (double) field);
						break;
					case java.sql.Types.DECIMAL:
					case java.sql.Types.NUMERIC:
						upload.setBigDecimal(index + 1, (java.math.BigDecimal) field);
						break;
					case java.sql.Types.DATE:
						upload.setDate(index + 1, (java.sql.Date) field);
						break;
					case java.sql.Types.TIME:
						upload.setTime(index + 1, (java.sql.Time) field);
						break;
					case java.sql.Types.TIMESTAMP:
						upload.setTimestamp(index + 1, (java.sql.Timestamp) field);
						break;
					case java.sql.Types.BINARY:
					case java.sql.Types.VARBINARY:
					case java.sql.Types.LONGVARBINARY:
						upload.setBytes(index + 1, (byte[]) field);
						break;
					default:
						upload.setObject(index + 1, field);
						LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
							type, index + 1, field);
						// case java.sql.Types.SQLXML
						// case java.sql.Types.ARRAY:
						// case java.sql.Types.JAVA_OBJECT:
						// case java.sql.Types.BLOB:
						// case java.sql.Types.CLOB:
						// case java.sql.Types.NCLOB:
						// case java.sql.Types.DATALINK:
						// case java.sql.Types.DISTINCT:
						// case java.sql.Types.OTHER:
						// case java.sql.Types.REF:
						// case java.sql.Types.ROWID:
						// case java.sql.Types.STRUC
				}
			} catch (ClassCastException e) {
				// enrich the exception with detailed information.
				String errorMessage = String.format(
					"%s, field index: %s, field value: %s.", e.getMessage(), index, field);
				ClassCastException enrichedException = new ClassCastException(errorMessage);
				enrichedException.setStackTrace(e.getStackTrace());
				throw enrichedException;
			}
		}
	}

	public static Object getFieldFromResultSet(int index, int type, ResultSet set) throws SQLException {
		Object ret;
		switch (type) {
			case java.sql.Types.NULL:
				ret = null;
				break;
			case java.sql.Types.BOOLEAN:
			case java.sql.Types.BIT:
				ret = set.getBoolean(index + 1);
				break;
			case java.sql.Types.CHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.LONGNVARCHAR:
				ret = set.getString(index + 1);
				break;
			case java.sql.Types.TINYINT:
				ret = set.getByte(index + 1);
				break;
			case java.sql.Types.SMALLINT:
				ret = set.getShort(index + 1);
				break;
			case java.sql.Types.INTEGER:
				ret = set.getInt(index + 1);
				break;
			case java.sql.Types.BIGINT:
				ret = set.getLong(index + 1);
				break;
			case java.sql.Types.REAL:
				ret = set.getFloat(index + 1);
				break;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				ret = set.getDouble(index + 1);
				break;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
				ret = set.getBigDecimal(index + 1);
				break;
			case java.sql.Types.DATE:
				ret = set.getDate(index + 1);
				break;
			case java.sql.Types.TIME:
				ret = set.getTime(index + 1);
				break;
			case java.sql.Types.TIMESTAMP:
				ret = set.getTimestamp(index + 1);
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				ret = set.getBytes(index + 1);
				break;
			default:
				ret = set.getObject(index + 1);
				LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to get its value: {}.",
					type, index + 1, ret);
				break;

			// case java.sql.Types.SQLXML
			// case java.sql.Types.ARRAY:
			// case java.sql.Types.JAVA_OBJECT:
			// case java.sql.Types.BLOB:
			// case java.sql.Types.CLOB:
			// case java.sql.Types.NCLOB:
			// case java.sql.Types.DATALINK:
			// case java.sql.Types.DISTINCT:
			// case java.sql.Types.OTHER:
			// case java.sql.Types.REF:
			// case java.sql.Types.ROWID:
			// case java.sql.Types.STRUC
		}

		if (set.wasNull()) {
			return null;
		} else {
			return ret;
		}
	}
}
