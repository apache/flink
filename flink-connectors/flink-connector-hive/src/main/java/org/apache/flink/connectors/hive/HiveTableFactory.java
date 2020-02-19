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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A table factory implementation for Hive catalog.
 */
public class HiveTableFactory
		implements TableSourceFactory<BaseRow>, TableSinkFactory<Row> {

	private final HiveConf hiveConf;

	public HiveTableFactory(HiveConf hiveConf) {
		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");
	}

	@Override
	public Map<String, String> requiredContext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> supportedProperties() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSource<BaseRow> createTableSource(TableSourceFactory.Context context) {
		CatalogTable table = checkNotNull(context.getTable());
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.parseBoolean(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return createHiveTableSource(context.getObjectIdentifier().toObjectPath(), table);
		} else {
			return TableFactoryUtil.findAndCreateTableSource(context);
		}
	}

	/**
	 * Creates and configures a {@link StreamTableSource} using the given {@link CatalogTable}.
	 */
	private StreamTableSource<BaseRow> createHiveTableSource(ObjectPath tablePath, CatalogTable table) {
		return new HiveTableSource(new JobConf(hiveConf), tablePath, table);
	}

	@Override
	public TableSink<Row> createTableSink(TableSinkFactory.Context context) {
		CatalogTable table = checkNotNull(context.getTable());
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.parseBoolean(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return createOutputFormatTableSink(context.getObjectIdentifier().toObjectPath(), table);
		} else {
			return TableFactoryUtil.findAndCreateTableSink(context);
		}
	}

	/**
	 * Creates and configures a {@link org.apache.flink.table.sinks.OutputFormatTableSink} using the given {@link CatalogTable}.
	 */
	private OutputFormatTableSink<Row> createOutputFormatTableSink(ObjectPath tablePath, CatalogTable table) {
		return new HiveTableSink(new JobConf(hiveConf), tablePath, table);
	}
}
