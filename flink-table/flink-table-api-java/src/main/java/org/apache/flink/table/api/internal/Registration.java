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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

/**
 * A way to register a table in a {@link TableEnvironment} that this descriptor originates from.
 */
@Internal
public interface Registration {
	/**
	 * Registers an external {@link TableSource} in this {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * @param name The name under which the {@link TableSource} is registered.
	 * @param tableSource The {@link TableSource} to register.
	 * @see TableEnvironment#registerTableSource(String, TableSource)
	 */
	void createTableSource(String name, TableSource<?> tableSource);

	/**
	 * Registers an external {@link TableSink} with already configured field names and field types in
	 * this {@link TableEnvironment}'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * @param name The name under which the {@link TableSink} is registered.
	 * @param tableSink The configured {@link TableSink} to register.
	 * @see TableEnvironment#registerTableSink(String, TableSink)
	 */
	void createTableSink(String name, TableSink<?> tableSink);

	/**
	 * Creates a temporary table in a given path.
	 *
	 * @param path Path where to register the given table
	 * @param table table to register
	 */
	void createTemporaryTable(String path, CatalogBaseTable table);
}
