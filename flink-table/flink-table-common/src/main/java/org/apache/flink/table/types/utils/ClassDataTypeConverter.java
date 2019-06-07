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

package org.apache.flink.table.types.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Class-based data type extractor that supports extraction of clearly identifiable data types for
 * input and output conversion.
 */
@Internal
public final class ClassDataTypeConverter {

	private static final Map<String, DataType> defaultDataTypes = new HashMap<>();
	static {
		// NOTE: this list explicitly excludes data types that need further parameters
		// exclusions: DECIMAL, INTERVAL YEAR TO MONTH, MAP, MULTISET, ROW, NULL, ANY
		addDefaultDataType(String.class, DataTypes.STRING());
		addDefaultDataType(Boolean.class, DataTypes.BOOLEAN());
		addDefaultDataType(boolean.class, DataTypes.BOOLEAN());
		addDefaultDataType(Byte.class, DataTypes.TINYINT());
		addDefaultDataType(byte.class, DataTypes.TINYINT());
		addDefaultDataType(Short.class, DataTypes.SMALLINT());
		addDefaultDataType(short.class, DataTypes.SMALLINT());
		addDefaultDataType(Integer.class, DataTypes.INT());
		addDefaultDataType(int.class, DataTypes.INT());
		addDefaultDataType(Long.class, DataTypes.BIGINT());
		addDefaultDataType(long.class, DataTypes.BIGINT());
		addDefaultDataType(Float.class, DataTypes.FLOAT());
		addDefaultDataType(float.class, DataTypes.FLOAT());
		addDefaultDataType(Double.class, DataTypes.DOUBLE());
		addDefaultDataType(double.class, DataTypes.DOUBLE());
		addDefaultDataType(java.sql.Date.class, DataTypes.DATE());
		addDefaultDataType(java.time.LocalDate.class, DataTypes.DATE());
		addDefaultDataType(java.sql.Time.class, DataTypes.TIME(0));
		addDefaultDataType(java.time.LocalTime.class, DataTypes.TIME(9));
		addDefaultDataType(java.sql.Timestamp.class, DataTypes.TIMESTAMP(9));
		addDefaultDataType(java.time.LocalDateTime.class, DataTypes.TIMESTAMP(9));
		addDefaultDataType(java.time.OffsetDateTime.class, DataTypes.TIMESTAMP_WITH_TIME_ZONE(9));
		addDefaultDataType(java.time.Instant.class, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9));
		addDefaultDataType(java.time.Duration.class, DataTypes.INTERVAL(DataTypes.SECOND(9)));
	}

	private static void addDefaultDataType(Class<?> clazz, DataType rootType) {
		final DataType dataType;
		if (clazz.isPrimitive()) {
			dataType = rootType.notNull();
		} else {
			dataType = rootType.nullable();
		}
		defaultDataTypes.put(clazz.getName(), dataType.bridgedTo(clazz));
	}

	/**
	 * Returns the clearly identifiable data type if possible. For example, {@link Long} can be
	 * expressed as {@link DataTypes#BIGINT()}. However, for example, {@link Row} cannot be extracted
	 * as information about the fields is missing. Or {@link BigDecimal} needs to be mapped from a
	 * variable precision/scale to constant ones.
	 */
	@SuppressWarnings("unchecked")
	public static Optional<DataType> extractDataType(Class<?> clazz) {
		// byte arrays have higher priority than regular arrays
		if (clazz.equals(byte[].class)) {
			return Optional.of(DataTypes.BYTES());
		}

		if (clazz.isArray()) {
			return extractDataType(clazz.getComponentType())
				.map(DataTypes::ARRAY);
		}

		if (TableSymbol.class.isAssignableFrom(clazz)) {
			return Optional.of(new AtomicDataType(new SymbolType(clazz)));
		}

		return Optional.ofNullable(defaultDataTypes.get(clazz.getName()));
	}

	private ClassDataTypeConverter() {
		// no instantiation
	}
}
