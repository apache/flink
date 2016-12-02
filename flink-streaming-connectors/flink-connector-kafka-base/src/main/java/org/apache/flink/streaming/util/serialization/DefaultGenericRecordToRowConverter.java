/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.serialization;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;

/**
 * Default converter that converts every field from from GenericRecord to a Row field.
 */
public class DefaultGenericRecordToRowConverter implements GenericRecordToRowConverter {

	/** Field names in a row */
	private final String[] fieldNames;
	/** Types to parse fields as. Indices match fieldNames indices. */
	private final TypeInformation[] fieldTypes;

	/**
	 * Create DefaultGenericRecordToRowConverter.
	 *
	 * @param fieldNames Row fields names
	 * @param fieldTypes Row fields types
	 */
	public DefaultGenericRecordToRowConverter(String[] fieldNames, TypeInformation[] fieldTypes) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

	@Override
	public Row convert(GenericRecord record) {
		Row row = new Row(fieldNames.length);

		for (int i = 0; i < fieldNames.length; i++) {
			Object val = record.get(fieldNames[i]);
			// Avro deserializes strings into Utf8 type
			if (fieldTypes[i].getTypeClass().equals(String.class) && val != null) {
				val = val.toString();
			}
			row.setField(i, val);
		}

		return row;
	}
}
