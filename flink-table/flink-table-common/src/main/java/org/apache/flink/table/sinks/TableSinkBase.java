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

package org.apache.flink.table.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Base class for TableSink.
 *
 * @param <T> The return type of the {@link TableSinkBase}.
 */
public abstract class TableSinkBase<T> implements TableSink<T> {

	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	/**
	 * Return a deep copy of the {@link TableSink}.
	 */
	protected abstract TableSinkBase<T> copy();

	/**
 	 * Return the field names of the Table to emit.
 	 */
	public String[] getFieldNames() {
		if (fieldNames != null) {
			return fieldNames;
		} else {
			throw new IllegalStateException(
				"TableSink must be configured to retrieve field names.");
		}
	}

	/**
	 * Return the field types of the Table to emit.
	 */
	public TypeInformation<?>[] getFieldTypes() {
		if (fieldTypes != null) {
			return fieldTypes;
		} else {
			throw new IllegalStateException(
				"TableSink must be configured to retrieve field types.");
		}
	}

	/**
 	 * Return a copy of this {@link TableSink} configured with the field names and types of the
 	 * Table to emit.
 	 *
 	 * @param fieldNames The field names of the table to emit.
 	 * @param fieldTypes The field types of the table to emit.
 	 * @return A copy of this {@link TableSink} configured with the field names and types of the
 	 * Table to emit.
 	 */
	public final TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

		TableSinkBase<T> configuredSink = this.copy();
		configuredSink.fieldNames = fieldNames;
		configuredSink.fieldTypes = fieldTypes;

		return configuredSink;
	}
}
