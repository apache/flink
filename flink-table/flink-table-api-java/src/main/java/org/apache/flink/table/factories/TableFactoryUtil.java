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
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.Map;
import java.util.Optional;

/**
 * Utility for dealing with {@link TableFactory} using the {@link TableFactoryService}.
 */
public class TableFactoryUtil {

	/**
	 * Returns a table source matching the descriptor.
	 */
	public static <T> TableSource<T> findAndCreateTableSource(Descriptor descriptor) {
		Map<String, String> properties = descriptor.toProperties();
		return findAndCreateTableSource(properties);
	}

	/**
	 * Returns a table source matching the properties.
	 */
	@SuppressWarnings("unchecked")
	private static <T> TableSource<T> findAndCreateTableSource(Map<String, String> properties) {
		try {
			return TableFactoryService
				.find(TableSourceFactory.class, properties)
				.createTableSource(properties);
		} catch (Throwable t) {
			throw new TableException("findAndCreateTableSource failed.", t);
		}
	}

	/**
	 * Returns a table sink matching the descriptor.
	 */
	public static <T> TableSink<T> findAndCreateTableSink(Descriptor descriptor) {
		Map<String, String> properties = descriptor.toProperties();
		return findAndCreateTableSink(properties);
	}

	@SuppressWarnings("unchecked")
	private static <T> TableSink<T> findAndCreateTableSink(Map<String, String> properties) {
		TableSink tableSink;
		try {
			tableSink = TableFactoryService
				.find(TableSinkFactory.class, properties)
				.createTableSink(properties);
		} catch (Throwable t) {
			throw new TableException("findAndCreateTableSink failed.", t);
		}

		return tableSink;
	}

	/**
	 * Returns a table sink matching the {@link org.apache.flink.table.catalog.CatalogTable}.
	 */
	public static <T> TableSink<T> findAndCreateTableSink(CatalogTable table) {
		return findAndCreateTableSink(table.toProperties());
	}

	/**
	 * Returns a table source matching the {@link org.apache.flink.table.catalog.CatalogTable}.
	 */
	public static <T> TableSource<T> findAndCreateTableSource(CatalogTable table) {
		return findAndCreateTableSource(table.toProperties());
	}

	/**
	 * Creates a table sink for a {@link CatalogTable} using table factory associated with the catalog.
	 */
	public static Optional<TableSink> createTableSinkForCatalogTable(Catalog catalog, CatalogTable catalogTable, ObjectPath tablePath) {
		TableFactory tableFactory = catalog.getTableFactory().orElse(null);
		if (tableFactory instanceof TableSinkFactory) {
			return Optional.ofNullable(((TableSinkFactory) tableFactory).createTableSink(tablePath, catalogTable));
		}
		return Optional.empty();
	}

}
