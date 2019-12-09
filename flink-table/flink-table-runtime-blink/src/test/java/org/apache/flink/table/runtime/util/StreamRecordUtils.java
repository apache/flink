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

package org.apache.flink.table.runtime.util;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;

import static org.apache.flink.table.dataformat.BinaryString.fromString;

/**
 * Utilities to generate StreamRecord which encapsulates BaseRow.
 */
public class StreamRecordUtils {

	/**
	 * Receives a object array, generates an acc BaseRow based on the array, wraps the BaseRow in a StreamRecord.
	 *
	 * @param fields input object array
	 * @return generated StreamRecord
	 */
	public static StreamRecord<BaseRow> record(Object... fields) {
		return new StreamRecord<>(baserow(fields));
	}

	/**
	 * Receives a object array, generates a retract BaseRow based on the array, wraps the BaseRow in a StreamRecord.
	 *
	 * @param fields input object array
	 * @return generated StreamRecord
	 */
	public static StreamRecord<BaseRow> retractRecord(Object... fields) {
		BaseRow row = baserow(fields);
		BaseRowUtil.setRetract(row);
		return new StreamRecord<>(row);
	}

	/**
	 * Receives a object array, generates a delete BaseRow based on the array, wraps the BaseRow in a StreamRecord.
	 *
	 * @param fields input object array
	 * @return generated StreamRecord
	 */
	public static StreamRecord<BaseRow> deleteRecord(Object... fields) {
		BaseRow row = baserow(fields);
		BaseRowUtil.setRetract(row);
		return new StreamRecord<>(row);
	}

	/**
	 * Receives a object array, generates a BaseRow based on the array.
	 *
	 * @param fields input object array
	 * @return generated BaseRow.
	 */
	public static BaseRow baserow(Object... fields) {
		Object[] objects = new Object[fields.length];
		for (int i = 0; i < fields.length; i++) {
			Object field = fields[i];
			if (field instanceof String) {
				objects[i] = fromString((String) field);
			} else {
				objects[i] = field;
			}
		}
		return GenericRow.of(objects);
	}

	/**
	 * Receives a object array, generates a BinaryRow based on the array.
	 *
	 * @param fields input object array
	 * @return generated BinaryRow.
	 */
	public static BinaryRow binaryrow(Object... fields) {
		BinaryRow row = new BinaryRow(fields.length);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		for (int j = 0; j < fields.length; j++) {
			Object value = fields[j];
			if (value == null) {
				writer.setNullAt(j);
			} else if (value instanceof Integer) {
				writer.writeInt(j, (Integer) value);
			} else if (value instanceof String) {
				writer.writeString(j, BinaryString.fromString((String) value));
			} else if (value instanceof Double) {
				writer.writeDouble(j, (Double) value);
			} else if (value instanceof Float) {
				writer.writeFloat(j, (Float) value);
			} else if (value instanceof Long) {
				writer.writeLong(j, (Long) value);
			} else if (value instanceof Boolean) {
				writer.writeBoolean(j, (Boolean) value);
			} else {
				throw new RuntimeException("Not support yet!");
			}
		}

		writer.complete();
		return row;
	}

	private StreamRecordUtils() {
		// deprecate default constructor
	}
}
