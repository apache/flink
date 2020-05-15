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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all converters that convert Flink internal data structure to JDBC object.
 */
public abstract class AbstractRowToJdbcConverter implements RowToJdbcConverter {

	protected final LogicalType[] logicalTypes;
	protected final RowFieldConverter[] toExternalConverters;

	public AbstractRowToJdbcConverter(LogicalType[] logicalTypes) {
		checkNotNull(logicalTypes, "logical types types can not be null.");
		this.logicalTypes = logicalTypes;
		this.toExternalConverters = new RowToJdbcConverter.RowFieldConverter[logicalTypes.length];
		for (int i = 0; i < toExternalConverters.length; i++) {
			toExternalConverters[i] = createExternalConverter(logicalTypes[i]);
		}
	}

	@Override
	public PreparedStatement toExternal(RowData rowData, PreparedStatement statement) throws SQLException {
		for (int index = 0; index < rowData.getArity(); index++) {
			toExternalConverters[index].convert(statement, index, logicalTypes[index], rowData);
		}
		return statement;
	}

	/**
	 * Create a runtime JDBC field converter from given sql type.
	 */
	public RowFieldConverter createExternalConverter(LogicalType logicalType) {
		return (statement, index, type, val) -> {
			final int sqlType = JdbcTypeUtil.typeInformationToSqlType(TypeConversions.fromDataTypeToLegacyInfo(
					TypeConversions.fromLogicalToDataType(type)));
			if (val.isNullAt(index)) {
				statement.setNull(index + 1, sqlType);
			} else {
				try {
					switch (sqlType) {
						case java.sql.Types.NULL:
							statement.setNull(index + 1, sqlType);
							break;
						case java.sql.Types.BOOLEAN:
						case java.sql.Types.BIT:
							statement.setBoolean(index + 1, val.getBoolean(index));
							break;
						case java.sql.Types.CHAR:
						case java.sql.Types.NCHAR:
						case java.sql.Types.VARCHAR:
						case java.sql.Types.LONGVARCHAR:
						case java.sql.Types.LONGNVARCHAR:
							statement.setString(index + 1, val.getString(index).toString());
							break;
						case java.sql.Types.TINYINT:
							statement.setByte(index + 1, val.getByte(index));
							break;
						case java.sql.Types.SMALLINT:
							statement.setShort(index + 1, val.getShort(index));
							break;
						case java.sql.Types.INTEGER:
							statement.setInt(index + 1, val.getInt(index));
							break;
						case java.sql.Types.BIGINT:
							statement.setLong(index + 1, val.getLong(index));
							break;
						case java.sql.Types.REAL:
							statement.setFloat(index + 1, val.getFloat(index));
							break;
						case java.sql.Types.FLOAT:
						case java.sql.Types.DOUBLE:
							statement.setDouble(index + 1, val.getDouble(index));
							break;
						case java.sql.Types.DECIMAL:
						case java.sql.Types.NUMERIC:
							// legacy decimal precision and scale is (38, 18).
							final int precision = logicalType instanceof DecimalType ? ((DecimalType) logicalType).getPrecision() : 38;
							final int scale = logicalType instanceof DecimalType ? ((DecimalType) logicalType).getScale() : 18;
							statement.setBigDecimal(index + 1, (val.getDecimal(index, precision, scale)).toBigDecimal());
							break;
						case java.sql.Types.DATE:
							statement.setDate(index + 1, Date.valueOf(LocalDate.ofEpochDay(val.getLong(index))));
							break;
						case java.sql.Types.TIME:
							statement.setTime(index + 1, Time.valueOf(LocalTime.ofSecondOfDay(val.getInt(index) / 1000L)));
							break;
						case java.sql.Types.TIMESTAMP:
							// default timestamp precision is 3.
							final int precison1 = logicalType instanceof TimestampType ? ((TimestampType) logicalType).getPrecision() : 3;
							statement.setTimestamp(index + 1, (val.getTimestamp(index, precison1)).toTimestamp());
							break;
						case java.sql.Types.BINARY:
						case java.sql.Types.VARBINARY:
						case java.sql.Types.LONGVARBINARY:
							statement.setBytes(index + 1, val.getBinary(index));
							break;
						default:
							statement.setObject(index + 1, RowData.get(val, index, logicalType));
					}
				} catch (ClassCastException e) {
					// enrich the exception with detailed information.
					String errorMessage = String.format(
						"%s, field index: %s, field value: %s.", e.getMessage(), index, val);
					ClassCastException enrichedException = new ClassCastException(errorMessage);
					enrichedException.setStackTrace(e.getStackTrace());
					throw enrichedException;
				}
			}
		};
	}
}
