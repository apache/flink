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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * A {@link TableSink} specifies how to emit a table to an external
 * system or location.
 *
 * <p>The interface is generic such that it can support different storage locations and formats.
 *
 * @param <T> The return type of the {@link TableSink}.
 */
@PublicEvolving
public interface TableSink<T> {

	/**
	 * Returns the type expected by this {@link TableSink}.
	 *
	 * <p>This type should depend on the types returned by {@link TableSink#getFieldNames()}.
	 *
	 * @return The type expected by this {@link TableSink}.
	 */
	TypeInformation<T> getOutputType();

	/**
	 * Returns the names of the table fields.
	 */
	String[] getFieldNames();

	/**
	 * Returns the types of the table fields.
	 */
	TypeInformation<?>[] getFieldTypes();

	/**
	 * Returns a copy of this {@link TableSink} configured with the field names and types of the
	 * table to emit.
	 *
	 * @param fieldNames The field names of the table to emit.
	 * @param fieldTypes The field types of the table to emit.
	 * @return A copy of this {@link TableSink} configured with the field names and types of the
	 *         table to emit.
	 */
	TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes);
}
