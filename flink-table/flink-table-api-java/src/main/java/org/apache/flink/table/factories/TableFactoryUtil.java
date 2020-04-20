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

package org.apache.flink.table.factories;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.Optional;

/**
 * Utility for dealing with {@link TableFactory} using the {@link TableFactoryService}.
 */
public class TableFactoryUtil {

	/**
	 * Returns a table source matching the descriptor.
	 */
	@SuppressWarnings("unchecked")
	public static <T> TableSource<T> findAndCreateTableSource(TableSourceFactory.Context context) {
		try {
			return TableFactoryService
					.find(TableSourceFactory.class, context.getTable().toProperties())
					.createTableSource(context);
		} catch (Throwable t) {
			throw new TableException("findAndCreateTableSource failed.", t);
		}
	}

	/**
	 * Returns a table sink matching the context.
	 */
	@SuppressWarnings("unchecked")
	public static <T> TableSink<T> findAndCreateTableSink(TableSinkFactory.Context context) {
		try {
			return TableFactoryService
					.find(TableSinkFactory.class, context.getTable().toProperties())
					.createTableSink(context);
		} catch (Throwable t) {
			throw new TableException("findAndCreateTableSink failed.", t);
		}
	}

	/**
	 * Creates a table sink for a {@link CatalogTable} using table factory associated with the catalog.
	 */
	public static Optional<TableSink> createTableSinkForCatalogTable(Catalog catalog, TableSinkFactory.Context context) {
		TableFactory tableFactory = catalog.getTableFactory().orElse(null);
		if (tableFactory instanceof TableSinkFactory) {
			return Optional.ofNullable(((TableSinkFactory) tableFactory).createTableSink(context));
		}
		return Optional.empty();
	}

}
