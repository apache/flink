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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Optional;

/**
 * Base class for {@link TableSink}.
 *
 * @param <T> The return type of the {@link TableSinkBase}.
 */
@Internal
public abstract class TableSinkBase<T> implements TableSink<T> {

	private Optional<String[]> fieldNames;
	private Optional<TypeInformation<?>[]> fieldTypes;

	/**
	 * Returns a deep copy of the {@link TableSink}.
	 */
	protected abstract TableSinkBase<T> copy();

	/**
	 * Returns the field names of the table to emit.
	 */
	@Override
	public String[] getFieldNames() {
		if (fieldNames.isPresent()) {
			return fieldNames.get();
		} else {
			throw new IllegalStateException(
				"Table sink must be configured to retrieve field names.");
		}
	}

	/**
	 * Returns the field types of the table to emit.
	 */
	@Override
	public TypeInformation<?>[] getFieldTypes() {
		if (fieldTypes.isPresent()) {
			return fieldTypes.get();
		} else {
			throw new IllegalStateException(
				"Table sink must be configured to retrieve field types.");
		}
	}

	/**
	 * Returns a copy of this {@link TableSink} configured with the field names and types of the
	 * table to emit.
	 *
	 * @param fieldNames The field names of the table to emit.
	 * @param fieldTypes The field types of the table to emit.
	 * @return A copy of this {@link TableSink} configured with the field names and types of the
	 *         table to emit.
	 */
	@Override
	public final TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

		final TableSinkBase<T> configuredSink = this.copy();
		configuredSink.fieldNames = Optional.of(fieldNames);
		configuredSink.fieldTypes = Optional.of(fieldTypes);

		return configuredSink;
	}
}
