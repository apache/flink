/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;

/**
 * rowData util.
 */
@Internal
public class RowDataUtil {

	public static void setField(GenericRowData rowData, int pos, Object value) {
		if (value instanceof LocalDate) {
			rowData.setField(pos, (int) ((LocalDate) value).toEpochDay());
		} else if (value instanceof LocalTime) {
			rowData.setField(pos, ((LocalTime) value).toNanoOfDay());
		} else if (value instanceof Instant) {
			rowData.setField(pos, TimestampData.fromInstant((Instant) value));
		} else if (value instanceof String) {
			rowData.setField(pos, StringData.fromString((String) value));
		} else if (value instanceof LocalDateTime) {
			rowData.setField(pos, TimestampData.fromLocalDateTime((LocalDateTime) value));
		} else if (value instanceof Map) {
			rowData.setField(pos, new GenericMapData((Map<?, ?>) value));
		} else {
			rowData.setField(pos, value);
		}
	}

	public static Object getField(RowData rowData, int pos, Class<?> clazz) {
		if (clazz.isAssignableFrom(LocalDate.class)) {
			final int value = rowData.getInt(pos);
			return LocalDate.ofEpochDay(value);
		} else if (clazz.isAssignableFrom(LocalTime.class)) {
			final long value = rowData.getLong(pos);
			return LocalTime.ofNanoOfDay(value);
		} else if (clazz.isAssignableFrom(Instant.class)) {
			final TimestampData timestamp = rowData.getTimestamp(pos, 3);
			return timestamp.toInstant();
		} else if (clazz.isAssignableFrom(String.class)) {
			return rowData.getString(pos).toString();
		} else if (clazz.isAssignableFrom(LocalDateTime.class)) {
			return rowData.getTimestamp(pos, 3).toLocalDateTime();
		} else if (clazz.isAssignableFrom(Integer.class) || clazz == Integer.TYPE) {
			return rowData.getInt(pos);
		} else if (clazz.isAssignableFrom(Long.class) || clazz == Long.TYPE) {
			return rowData.getLong(pos);
		} else if (clazz.isAssignableFrom(Byte.class) || clazz == Byte.TYPE) {
			return rowData.getByte(pos);
		} else if (clazz.isAssignableFrom(Short.class) || clazz == Short.TYPE) {
			return rowData.getShort(pos);
		} else if (clazz.isAssignableFrom(Float.class) || clazz == Float.TYPE) {
			return rowData.getFloat(pos);
		} else if (clazz.isAssignableFrom(Double.class) || clazz == Double.TYPE) {
			return rowData.getDouble(pos);
		} else if (clazz.isAssignableFrom(byte[].class)) {
			return rowData.getBinary(pos);
		} else if (clazz.isAssignableFrom(Boolean.class) || clazz == Boolean.TYPE) {
			return rowData.getBoolean(pos);
		} else if (clazz.isAssignableFrom(Instant.class)) {
			return rowData.getTimestamp(pos, 3).toInstant();
		} else {
			throw new IllegalArgumentException("not support " + clazz.getName());
		}
	}
}
