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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for representing the operation structure behind a user-facing {@link Table} API.
 */
@Internal
public abstract class TableOperation {

	/**
	 * Resolved schema of this operation.
	 */
	public abstract TableSchema getTableSchema();

	/**
	 * Returns a string that summarizes this operation for printing to a console. An implementation might
	 * skip very specific properties.
	 *
	 * <p>Use {@link #asSerializableString()} for a operation string that fully serializes
	 * this instance.
	 *
	 * @return summary string of this operation for debugging purposes
	 */
	public abstract String asSummaryString();

	/**
	 * Returns a string that fully serializes this instance. The serialized string can be used for storing
	 * the query in e.g. a {@link org.apache.flink.table.catalog.Catalog} as a view.
	 *
	 * @return detailed string for persisting in a catalog
	 */
	public String asSerializableString() {
		throw new UnsupportedOperationException("TableOperations are not string serializable for now.");
	}

	public abstract List<TableOperation> getChildren();

	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitOther(this);
	}

	protected final String formatWithChildren(String format, Object... args) {
		return String.format(format, args) +
			getChildren().stream()
				.map(child -> TableOperationUtils.indent(child.asSummaryString()))
				.collect(Collectors.joining());
	}
}
