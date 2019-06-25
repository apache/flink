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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * A table factory implementation for tables stored in Hive catalog.
 */
public class HiveTableFactory implements TableSourceFactory<Row>, TableSinkFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> supportedProperties() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSink<Row> createTableSink(Map<String, String> properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSource<Row> createTableSource(Map<String, String> properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSource<Row> createTableSource(CatalogTable table) {
		Preconditions.checkNotNull(table);
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.valueOf(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return createInputFormatTableSource(table);
		} else {
			return TableFactoryUtil.findAndCreateTableSource(table);
		}
	}

	/**
	 * Creates and configures a {@link org.apache.flink.table.sources.InputFormatTableSource} using the given {@link CatalogTable}.
	 */
	private InputFormatTableSource<Row> createInputFormatTableSource(CatalogTable table) {
		// TODO: create an InputFormatTableSource from a HiveCatalogTable instance.
		return null;
	}

	@Override
	public TableSink<Row> createTableSink(CatalogTable table) {
		Preconditions.checkNotNull(table);
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.valueOf(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return createOutputFormatTableSink(table);
		} else {
			return TableFactoryUtil.findAndCreateTableSink(table);
		}
	}

	/**
	 * Creates and configures a {@link org.apache.flink.table.sinks.OutputFormatTableSink} using the given {@link CatalogTable}.
	 */
	private OutputFormatTableSink<Row> createOutputFormatTableSink(CatalogTable table) {
		// TODO: create an outputFormatTableSink from a HiveCatalogTable instance.
		return null;
	}

}
