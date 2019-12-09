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

package org.apache.flink.table.api;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Map;

/**
 * This class enumerates all supported types of the Table API & SQL.
 *
 * @deprecated This class will be removed in future versions as it uses the old type system. It is
 *             recommended to use {@link DataTypes} instead which uses the new type system based on
 *             instances of {@link DataType}. Please make sure to use either the old or the new type
 *             system consistently to avoid unintended behavior. See the website documentation
 *             for more information.
 */
@Deprecated
public final class Types {

	// we use SQL-like naming for types and avoid Java keyword clashes
	// CHECKSTYLE.OFF: MethodName

	/**
	 * Returns type information for a Table API string or SQL VARCHAR type.
	 */
	public static TypeInformation<String> STRING() {
		return org.apache.flink.api.common.typeinfo.Types.STRING;
	}

	/**
	 * Returns type information for a Table API boolean or SQL BOOLEAN type.
	 */
	public static TypeInformation<Boolean> BOOLEAN() {
		return org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
	}

	/**
	 * Returns type information for a Table API byte or SQL TINYINT type.
	 */
	public static TypeInformation<Byte> BYTE() {
		return org.apache.flink.api.common.typeinfo.Types.BYTE;
	}

	/**
	 * Returns type information for a Table API short or SQL SMALLINT type.
	 */
	public static TypeInformation<Short> SHORT() {
		return org.apache.flink.api.common.typeinfo.Types.SHORT;
	}

	/**
	 * Returns type information for a Table API integer or SQL INT/INTEGER type.
	 */
	public static TypeInformation<Integer> INT() {
		return org.apache.flink.api.common.typeinfo.Types.INT;
	}

	/**
	 * Returns type information for a Table API long or SQL BIGINT type.
	 */
	public static TypeInformation<Long> LONG() {
		return org.apache.flink.api.common.typeinfo.Types.LONG;
	}

	/**
	 * Returns type information for a Table API float or SQL FLOAT/REAL type.
	 */
	public static TypeInformation<Float> FLOAT() {
		return org.apache.flink.api.common.typeinfo.Types.FLOAT;
	}

	/**
	 * Returns type information for a Table API integer or SQL DOUBLE type.
	 */
	public static TypeInformation<Double> DOUBLE() {
		return org.apache.flink.api.common.typeinfo.Types.DOUBLE;
	}

	/**
	 * Returns type information for a Table API big decimal or SQL DECIMAL type.
	 */
	public static TypeInformation<BigDecimal> DECIMAL() {
		return org.apache.flink.api.common.typeinfo.Types.BIG_DEC;
	}

	/**
	 * Returns type information for a Table API SQL date or SQL DATE type.
	 */
	public static TypeInformation<java.sql.Date> SQL_DATE() {
		return org.apache.flink.api.common.typeinfo.Types.SQL_DATE;
	}

	/**
	 * Returns type information for a Table API SQL time or SQL TIME type.
	 */
	public static TypeInformation<java.sql.Time> SQL_TIME() {
		return org.apache.flink.api.common.typeinfo.Types.SQL_TIME;
	}

	/**
	 * Returns type information for a Table API SQL timestamp or SQL TIMESTAMP type.
	 */
	public static TypeInformation<java.sql.Timestamp> SQL_TIMESTAMP() {
		return org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
	}

	/**
	 * Returns type information for a Table API LocalDate type.
	 */
	public static TypeInformation<java.time.LocalDate> LOCAL_DATE() {
		return org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE;
	}

	/**
	 * Returns type information for a Table API LocalTime type.
	 */
	public static TypeInformation<java.time.LocalTime> LOCAL_TIME() {
		return org.apache.flink.api.common.typeinfo.Types.LOCAL_TIME;
	}

	/**
	 * Returns type information for a Table API LocalDateTime type.
	 */
	public static TypeInformation<java.time.LocalDateTime> LOCAL_DATE_TIME() {
		return org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME;
	}

	/**
	 * Returns type information for a Table API interval of months.
	 */
	public static TypeInformation<Integer> INTERVAL_MONTHS() {
		return TimeIntervalTypeInfo.INTERVAL_MONTHS;
	}

	/**
	 * Returns type information for a Table API interval of milliseconds.
	 */
	public static TypeInformation<Long> INTERVAL_MILLIS() {
		return TimeIntervalTypeInfo.INTERVAL_MILLIS;
	}

	/**
	 * Returns type information for {@link Row} with fields of the given types.
	 *
	 * <p>A row is a variable-length, null-aware composite type for storing multiple values in a
	 * deterministic field order. Every field can be null regardless of the field's type. The type of
	 * row fields cannot be automatically inferred; therefore, it is required to provide type information
	 * whenever a row is used.
	 *
	 * <p>The schema of rows can have up to <code>Integer.MAX_VALUE</code> fields, however, all row
	 * instances must strictly adhere to the schema defined by the type info.
	 *
	 * <p>This method generates type information with fields of the given types; the fields have
	 * the default names (f0, f1, f2 ..).
	 *
	 * @param types The types of the row fields, e.g., Types.STRING(), Types.INT()
	 */
	public static TypeInformation<Row> ROW(TypeInformation<?>... types) {
		return org.apache.flink.api.common.typeinfo.Types.ROW(types);
	}

	/**
	 * Returns type information for {@link Row} with fields of the given types and with given names.
	 *
	 * <p>A row is a variable-length, null-aware composite type for storing multiple values in a
	 * deterministic field order. Every field can be null independent of the field's type. The type of
	 * row fields cannot be automatically inferred; therefore, it is required to provide type information
	 * whenever a row is used.
	 *
	 * <p>The schema of rows can have up to <code>Integer.MAX_VALUE</code> fields, however, all row
	 * instances must strictly adhere to the schema defined by the type info.
	 *
	 * @param fieldNames The array of field names
	 * @param types The types of the row fields, e.g., Types.STRING(), Types.INT()
	 */
	public static TypeInformation<Row> ROW(String[] fieldNames, TypeInformation<?>[] types) {
		return org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(fieldNames, types);
	}

	/**
	 * Generates type information for an array consisting of Java primitive elements. The elements do
	 * not support null values.
	 *
	 * @param elementType type of the array elements; e.g. Types.INT()
	 */
	public static TypeInformation<?> PRIMITIVE_ARRAY(TypeInformation<?> elementType) {
		if (elementType.equals(BOOLEAN())) {
			return PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType.equals(BYTE())) {
			return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType.equals(SHORT())) {
			return PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType.equals(INT())) {
			return PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType.equals(LONG())) {
			return PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType.equals(FLOAT())) {
			return PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType.equals(DOUBLE())) {
			return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
		}
		throw new TableException(
			String.format(
				"%s cannot be an element of a primitive array. Only Java primitive types are supported.",
				elementType));
	}

	/**
	 * Generates type information for an array consisting of Java object elements. Null values for
	 * elements are supported.
	 *
	 * @param elementType type of the array elements; e.g. Types.INT()
	 */
	public static <E> TypeInformation<E[]> OBJECT_ARRAY(TypeInformation<E> elementType) {
		return ObjectArrayTypeInfo.getInfoFor(elementType);
	}

	/**
	 * Generates type information for a Java HashMap. Null values in keys are not supported. An
	 * entry's value can be null.
	 *
	 * @param keyType type of the keys of the map e.g. Types.STRING()
	 * @param valueType type of the values of the map e.g. Types.STRING()
	 */
	public static <K, V> TypeInformation<Map<K, V>> MAP(TypeInformation<K> keyType, TypeInformation<V> valueType) {
		return new MapTypeInfo<>(keyType, valueType);
	}

	/**
	 * Generates type information for a Multiset. A Multiset is baked by a Java HashMap and maps an
	 * arbitrary key to an integer value. Null values in keys are not supported.
	 *
	 * @param elementType type of the elements of the multiset e.g. Types.STRING()
	 */
	public static <E> TypeInformation<Map<E, Integer>> MULTISET(TypeInformation<E> elementType) {
		return new MultisetTypeInfo<>(elementType);
	}

	// CHECKSTYLE.ON: MethodName

	private Types() {
		// no instantiation
	}
}
