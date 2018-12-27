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
import org.apache.flink.table.api.Table;

import java.util.Optional;

/** A {@link TableSinkBase} provides ways to set and get field names and types.
 *
 * @param <T> The return type of the {@link TableSinkBase}.
 */
public abstract class TableSinkBase<T> implements TableSink<T> {

	private Optional<String[]> fieldNames = Optional.ofNullable(null);
	private Optional<TypeInformation<?>[]> fieldTypes = Optional.ofNullable(null);

	/** Return a deep copy of the {@link TableSink}. */
	protected abstract TableSinkBase<T> copy();

	/**
	 * Return the field names of the {@link Table} to emit. */
	public String[] getFieldNames() {
		return this.fieldNames.orElseThrow(() -> new IllegalStateException(
			"TableSink must be configured to retrieve field names."));
	}

	/** Return the field types of the {@link Table} to emit. */
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes.orElseThrow(() -> new IllegalStateException(
			"TableSink must be configured to retrieve field types."));
	}

	/**
	 * Return a copy of this {@link TableSink} configured with the field names and types of the
	 * {@link Table} to emit.
	 *
	 * @param fieldNames The field names of the table to emit.
	 * @param fieldTypes The field types of the table to emit.
	 * @return A copy of this {@link TableSink} configured with the field names and types of the
	 *         {@link Table} to emit.
	 */
	public final TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		TableSinkBase configuredSink = this.copy();
		configuredSink.fieldNames = Optional.ofNullable(fieldNames);
		configuredSink.fieldTypes = Optional.ofNullable(fieldTypes);
		return configuredSink;
	}
}
