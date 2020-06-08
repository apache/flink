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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.catalog.config.CatalogConfig.IS_GENERIC;

/**
 * A dynamic table factory implementation for Hive catalog. Now it only support generic tables.
 * Hive tables should be resolved by {@link HiveTableFactory}.
 */
public class HiveDynamicTableFactory implements
		DynamicTableSourceFactory,
		DynamicTableSinkFactory {

	@Override
	public String factoryIdentifier() {
		throw new UnsupportedOperationException("Hive factory is only work for catalog.");
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		throw new UnsupportedOperationException("Hive factory is only work for catalog.");
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		throw new UnsupportedOperationException("Hive factory is only work for catalog.");
	}

	private static CatalogTable removeIsGenericFlag(CatalogTable table) {
		Map<String, String> newOptions = new HashMap<>(table.getOptions());
		boolean isGeneric = Boolean.parseBoolean(newOptions.remove(IS_GENERIC));
		if (!isGeneric) {
			throw new ValidationException(
					"Hive dynamic table factory now only work for generic table.");
		}
		return table.copy(newOptions);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		return FactoryUtil.createTableSink(
				null, // we already in the factory of catalog
				context.getObjectIdentifier(),
				removeIsGenericFlag(context.getCatalogTable()),
				context.getConfiguration(),
				context.getClassLoader());
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		return FactoryUtil.createTableSource(
				null, // we already in the factory of catalog
				context.getObjectIdentifier(),
				removeIsGenericFlag(context.getCatalogTable()),
				context.getConfiguration(),
				context.getClassLoader());
	}
}
