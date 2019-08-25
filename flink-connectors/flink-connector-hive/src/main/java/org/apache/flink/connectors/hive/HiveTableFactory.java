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

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.hive.HiveFunctionWrapper;
import org.apache.flink.table.functions.hive.HiveGenericUDAF;
import org.apache.flink.table.functions.hive.HiveGenericUDF;
import org.apache.flink.table.functions.hive.HiveGenericUDTF;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A table factory implementation for Hive catalog.
 */
public class HiveTableFactory
		implements TableSourceFactory<Row>, TableSinkFactory<Row>, FunctionDefinitionFactory {
	private static final Logger LOG = LoggerFactory.getLogger(HiveTableFactory.class);

	private final HiveConf hiveConf;
	private final String hiveVersion;

	public HiveTableFactory(HiveConf hiveConf) {
		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");

		// this has to come from hiveConf, otherwise we may lose what user specifies in the yaml file
		this.hiveVersion = checkNotNull(hiveConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
				"Hive version is not defined");
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
	public TableSink<Row> createTableSink(Map<String, String> properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSource<Row> createTableSource(Map<String, String> properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSource<Row> createTableSource(ObjectPath tablePath, CatalogTable table) {
		Preconditions.checkNotNull(table);
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.valueOf(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return createInputFormatTableSource(tablePath, table);
		} else {
			return TableFactoryUtil.findAndCreateTableSource(table);
		}
	}

	/**
	 * Creates and configures a {@link org.apache.flink.table.sources.InputFormatTableSource} using the given {@link CatalogTable}.
	 */
	private InputFormatTableSource<Row> createInputFormatTableSource(ObjectPath tablePath, CatalogTable table) {
		return new HiveTableSource(new JobConf(hiveConf), tablePath, table);
	}

	@Override
	public TableSink<Row> createTableSink(ObjectPath tablePath, CatalogTable table) {
		Preconditions.checkNotNull(table);
		Preconditions.checkArgument(table instanceof CatalogTableImpl);

		boolean isGeneric = Boolean.valueOf(table.getProperties().get(CatalogConfig.IS_GENERIC));

		if (!isGeneric) {
			return createOutputFormatTableSink(tablePath, table);
		} else {
			return TableFactoryUtil.findAndCreateTableSink(table);
		}
	}

	/**
	 * Creates and configures a {@link org.apache.flink.table.sinks.OutputFormatTableSink} using the given {@link CatalogTable}.
	 */
	private OutputFormatTableSink<Row> createOutputFormatTableSink(ObjectPath tablePath, CatalogTable table) {
		return new HiveTableSink(new JobConf(hiveConf), tablePath, table);
	}

	@Override
	public FunctionDefinition createFunctionDefinition(String name, CatalogFunction catalogFunction) {
		String functionClassName = catalogFunction.getClassName();

		if (Boolean.valueOf(catalogFunction.getProperties().get(CatalogConfig.IS_GENERIC))) {
			throw new TableException(
				String.format("HiveFunctionDefinitionFactory does not support generic functions %s yet", name));
		}

		Class clazz;
		try {
			clazz = Thread.currentThread().getContextClassLoader().loadClass(functionClassName);

			LOG.info("Successfully loaded Hive udf '{}' with class '{}'", name, functionClassName);
		} catch (ClassNotFoundException e) {
			throw new TableException(
				String.format("Failed to initiate an instance of class %s.", functionClassName), e);
		}

		if (UDF.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveSimpleUDF", name);

			return new ScalarFunctionDefinition(
				name,
				new HiveSimpleUDF(new HiveFunctionWrapper<>(functionClassName))
			);
		} else if (GenericUDF.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveGenericUDF", name);

			return new ScalarFunctionDefinition(
				name,
				new HiveGenericUDF(new HiveFunctionWrapper<>(functionClassName))
			);
		} else if (GenericUDTF.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveGenericUDTF", name);

			HiveGenericUDTF udtf = new HiveGenericUDTF(new HiveFunctionWrapper<>(functionClassName));

			return new TableFunctionDefinition(
				name,
				udtf,
				GenericTypeInfo.of(Row.class)
			);
		} else if (GenericUDAFResolver2.class.isAssignableFrom(clazz) || UDAF.class.isAssignableFrom(clazz)) {
			HiveGenericUDAF udaf;

			if (GenericUDAFResolver2.class.isAssignableFrom(clazz)) {
				LOG.info(
					"Transforming Hive function '{}' into a HiveGenericUDAF with no UDAF bridging and Hive version %s",
					name, hiveVersion);

				udaf = new HiveGenericUDAF(new HiveFunctionWrapper<>(functionClassName), false, hiveVersion);
			} else {
				LOG.info(
					"Transforming Hive function '{}' into a HiveGenericUDAF with UDAF bridging and Hive version %s",
					name, hiveVersion);

				udaf = new HiveGenericUDAF(new HiveFunctionWrapper<>(functionClassName), true, hiveVersion);
			}

			return new AggregateFunctionDefinition(
				name,
				udaf,
				GenericTypeInfo.of(Object.class),
				GenericTypeInfo.of(GenericUDAFEvaluator.AggregationBuffer.class)
			);
		} else {
			throw new IllegalArgumentException(
				String.format("HiveFunctionDefinitionFactory cannot initiate FunctionDefinition for class %s", functionClassName));
		}
	}
}
