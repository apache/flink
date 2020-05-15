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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all converters that convert JDBC object to Flink internal data structure.
 */
public abstract class AbstractJdbcToRowConverter implements JdbcToRowConverter {

	protected final RowType rowType;
	protected final JdbcFieldConverter[] converters;

	public AbstractJdbcToRowConverter(RowType rowType) {
		this.rowType = checkNotNull(rowType);
		converters = new JdbcFieldConverter[rowType.getFieldCount()];

		for (int i = 0; i < converters.length; i++) {
			converters[i] = createConverter(rowType.getTypeAt(i));
		}
	}

	@Override
	public RowData toInternal(ResultSet resultSet) throws SQLException {
		GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
		try {
			for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
				genericRowData.setField(pos, converters[pos].convert(resultSet.getObject(pos + 1)));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return genericRowData;
	}

	/**
	 * Create a runtime JDBC field converter from given {@link LogicalType}.
	 */
	public JdbcFieldConverter createConverter(LogicalType type) {
		return v -> {
			if (v == null) {
				return null;
			} else {
				try {
					switch (type.getTypeRoot()) {
						case NULL:
							return null;
						case BOOLEAN:
							return (boolean) v;
						case TINYINT:
							return (byte) v;
						case SMALLINT:
							// Converter for small type that casts value to int and then return short value, since
							// JDBC 1.0 use int type for small values.
							return (Integer.valueOf(v.toString())).shortValue();
						case INTEGER:
						case INTERVAL_YEAR_MONTH:
							return (int) v;
						case BIGINT:
						case INTERVAL_DAY_TIME:
							return (long) v;
						case DATE:
							return (int) (((Date) v).toLocalDate().toEpochDay());
						case TIME_WITHOUT_TIME_ZONE:
							return ((Time) v).toLocalTime().toSecondOfDay() * 1000;
						case TIMESTAMP_WITH_TIME_ZONE:
						case TIMESTAMP_WITHOUT_TIME_ZONE:
							return TimestampData.fromTimestamp((Timestamp) v);
						case FLOAT:
							return (float) v;
						case DOUBLE:
							return (double) v;
						case CHAR:
						case VARCHAR:
							return StringData.fromString((String) v);
						case BINARY:
						case VARBINARY:
							return (byte[]) v;
						case DECIMAL:
							final int precision = ((DecimalType) type).getPrecision();
							final int scale = ((DecimalType) type).getScale();
							return DecimalData.fromBigDecimal((BigDecimal) v, precision, scale);
						case ARRAY:
							return createArrayConverter((ArrayType) type).convert(v);
						case ROW:
						case MAP:
						case MULTISET:
						case RAW:
						default:
							throw new UnsupportedOperationException("Unsupported type: " + type);
					}
				} catch (ClassCastException e) {
					// enrich the exception with detailed information.
					String errorMessage = String.format(
						"%s, field type: %s, field value: %s.", e.getMessage(), type, v);
					ClassCastException enrichedException = new ClassCastException(errorMessage);
					enrichedException.setStackTrace(e.getStackTrace());
					throw enrichedException;
				}
			}
		};
	}

	private JdbcFieldConverter createArrayConverter(ArrayType arrayType) {
		final JdbcFieldConverter elementConverter = createConverter(arrayType.getElementType());
		final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
		return v -> {
			final Object[] objects = (Object[]) v;
			final Object[] array = (Object[]) Array.newInstance(elementClass, objects.length);
			for (int i = 0; i < objects.length; i++) {
				array[i] = elementConverter.convert(objects[i]);
			}
			return new GenericArrayData(array);
		};
	}
}
