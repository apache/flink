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

package org.apache.flink.table.api.java;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.descriptors.BatchTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.sinks.TableSink;

import java.lang.reflect.Constructor;

/**
 * The {@link TableEnvironment} for a Java batch {@link ExecutionEnvironment} that works
 * with {@link DataSet}s.
 *
 * <p>A TableEnvironment can be used to:
 * <ul>
 *     <li>convert a {@link DataSet} to a {@link Table}</li>
 *     <li>register a {@link DataSet} in the {@link TableEnvironment}'s catalog</li>
 *     <li>register a {@link Table} in the {@link TableEnvironment}'s catalog</li>
 *     <li>scan a registered table to obtain a {@link Table}</li>
 *     <li>specify a SQL query on registered tables to obtain a {@link Table}</li>
 *     <li>convert a {@link Table} into a {@link DataSet}</li>
 *     <li>explain the AST and execution plan of a {@link Table}</li>
 * </ul>
 */
@PublicEvolving
public interface BatchTableEnvironment extends TableEnvironment {

	/**
	 * Registers a {@link TableFunction} under a unique name in the TableEnvironment's catalog.
	 * Registered functions can be referenced in Table API and SQL queries.
	 *
	 * @param name The name under which the function is registered.
	 * @param tableFunction The TableFunction to register.
	 * @param <T> The type of the output row.
	 */
	<T> void registerFunction(String name, TableFunction<T> tableFunction);

	/**
	 * Registers an {@link AggregateFunction} under a unique name in the TableEnvironment's catalog.
	 * Registered functions can be referenced in Table API and SQL queries.
	 *
	 * @param name The name under which the function is registered.
	 * @param aggregateFunction The AggregateFunction to register.
	 * @param <T> The type of the output value.
	 * @param <ACC> The type of aggregate accumulator.
	 */
	<T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction);

	/**
	 * Converts the given {@link DataSet} into a {@link Table}.
	 *
	 * <p>The field names of the {@link Table} are automatically derived from the type of the
	 * {@link DataSet}.
	 *
	 * @param dataSet The {@link DataSet} to be converted.
	 * @param <T> The type of the {@link DataSet}.
	 * @return The converted {@link Table}.
	 */
	<T> Table fromDataSet(DataSet<T> dataSet);

	/**
	 * Converts the given {@link DataSet} into a {@link Table} with specified field names.
	 *
	 * Example:
	 *
	 * <pre>
	 * {@code
	 *   DataSet<Tuple2<String, Long>> set = ...
	 *   Table tab = tableEnv.fromDataSet(set, "a, b");
	 * }
	 * </pre>
	 *
	 * @param dataSet The {@link DataSet} to be converted.
	 * @param fields The field names of the resulting {@link Table}.
	 * @param <T> The type of the {@link DataSet}.
	 * @return The converted {@link Table}.
	 */
	<T> Table fromDataSet(DataSet<T> dataSet, String fields);

	/**
	 * Registers the given {@link DataSet} as table in the
	 * {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * The field names of the {@link Table} are automatically derived from the type of the{@link DataSet}.
	 *
	 * @param name The name under which the {@link DataSet} is registered in the catalog.
	 * @param dataSet The {@link DataSet} to register.
	 * @param <T> The type of the {@link DataSet} to register.
	 */
	<T> void registerDataSet(String name, DataSet<T> dataSet);

	/**
	 * Registers the given {@link DataSet} as table with specified field names in the
	 * {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * Example:
	 *
	 * <pre>
	 * {@code
	 *   DataSet<Tuple2<String, Long>> set = ...
	 *   tableEnv.registerDataSet("myTable", set, "a, b");
	 * }
	 * </pre>
	 *
	 * @param name The name under which the {@link DataSet} is registered in the catalog.
	 * @param dataSet The {@link DataSet} to register.
	 * @param fields The field names of the registered table.
	 * @param <T> The type of the {@link DataSet} to register.
	 */
	<T> void registerDataSet(String name, DataSet<T> dataSet, String fields);

	/**
	 * Converts the given {@link Table} into a {@link DataSet} of a specified type.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataSet} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataSet} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param clazz The class of the type of the resulting {@link DataSet}.
	 * @param <T> The type of the resulting {@link DataSet}.
	 * @return The converted {@link DataSet}.
	 */
	<T> DataSet<T> toDataSet(Table table, Class<T> clazz);

	/**
	 * Converts the given {@link Table} into a {@link DataSet} of a specified type.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataSet} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataSet} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param typeInfo The {@link TypeInformation} that specifies the type of the resulting {@link DataSet}.
	 * @param <T> The type of the resulting {@link DataSet}.
	 * @return The converted {@link DataSet}.
	 */
	<T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo);

	/**
	 * Converts the given {@link Table} into a {@link DataSet} of a specified type.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataSet} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataSet} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param clazz The class of the type of the resulting {@link DataSet}.
	 * @param queryConfig The configuration for the query to generate.
	 * @param <T> The type of the resulting {@link DataSet}.
	 * @return The converted {@link DataSet}.
	 */
	<T> DataSet<T> toDataSet(Table table, Class<T> clazz, BatchQueryConfig queryConfig);

	/**
	 * Converts the given {@link Table} into a {@link DataSet} of a specified type.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataSet} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataSet} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param typeInfo The {@link TypeInformation} that specifies the type of the resulting {@link DataSet}.
	 * @param queryConfig The configuration for the query to generate.
	 * @param <T> The type of the resulting {@link DataSet}.
	 * @return The converted {@link DataSet}.
	 */
	<T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo, BatchQueryConfig queryConfig);

	/**
	 * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
	 * NOTE: Currently only SQL INSERT statements are supported.
	 *
	 * <p>All tables referenced by the query must be registered in the TableEnvironment.
	 * A {@link Table} is automatically registered when its {@link Table#toString()} method is
	 * called, for example when it is embedded into a String.
	 * Hence, SQL queries can directly reference a {@link Table} as follows:
	 *
	 * <pre>
	 * {@code
	 *   // register the configured table sink into which the result is inserted.
	 *   tEnv.registerTableSink("sinkTable", configuredSink);
	 *   Table sourceTable = ...
	 *   String tableName = sourceTable.toString();
	 *   // sourceTable is not registered to the table environment
	 *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM tableName", config);
	 * }
	 * </pre>
	 *
	 * @param stmt The SQL statement to evaluate.
	 * @param config The {@link BatchQueryConfig} to use.
	 */
	void sqlUpdate(String stmt, BatchQueryConfig config);

	/**
	 * Writes the {@link Table} to a {@link TableSink} that was registered under the specified name.
	 *
	 * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or
	 * {@link TableEnvironment#useCatalog(String)} for the rules on the path resolution.
	 *
	 * @param table The Table to write to the sink.
	 * @param queryConfig The {@link BatchQueryConfig} to use.
	 * @param sinkPath The first part of the path of the registered {@link TableSink} to which the {@link Table} is
	 *        written. This is to ensure at least the name of the {@link TableSink} is provided.
	 * @param sinkPathContinued The remaining part of the path of the registered {@link TableSink} to which the
	 *        {@link Table} is written.
	 */
	void insertInto(Table table, BatchQueryConfig queryConfig, String sinkPath, String... sinkPathContinued);

	/**
	 * Creates a table source and/or table sink from a descriptor.
	 *
	 * <p>Descriptors allow for declaring the communication to external systems in an
	 * implementation-agnostic way. The classpath is scanned for suitable table factories that match
	 * the desired configuration.
	 *
	 * <p>The following example shows how to read from a connector using a JSON format and
	 * registering a table source as "MyTable":
	 *
	 * <pre>
	 * {@code
	 *
	 * tableEnv
	 *   .connect(
	 *     new ExternalSystemXYZ()
	 *       .version("0.11"))
	 *   .withFormat(
	 *     new Json()
	 *       .jsonSchema("{...}")
	 *       .failOnMissingField(false))
	 *   .withSchema(
	 *     new Schema()
	 *       .field("user-name", "VARCHAR").from("u_name")
	 *       .field("count", "DECIMAL")
	 *   .registerSource("MyTable")
	 * }
	 * </pre>
	 *
	 * @param connectorDescriptor connector descriptor describing the external system
	 */
	@Override
	BatchTableDescriptor connect(ConnectorDescriptor connectorDescriptor);

	/**
	 * Returns a {@link TableEnvironment} for a Java batch {@link ExecutionEnvironment} that works
	 * with {@link DataSet}s.
	 *
	 * <p>A TableEnvironment can be used to:
	 * <ul>
	 *     <li>convert a {@link DataSet} to a {@link Table}</li>
	 *     <li>register a {@link DataSet} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>register a {@link Table} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>scan a registered table to obtain a {@link Table}</li>
	 *     <li>specify a SQL query on registered tables to obtain a {@link Table}</li>
	 *     <li>convert a {@link Table} into a {@link DataSet}</li>
	 *     <li>explain the AST and execution plan of a {@link Table}</li>
	 * </ul>
	 *
	 * @param executionEnvironment The Java batch {@link ExecutionEnvironment} of the TableEnvironment.
	 */
	static BatchTableEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	/**
	 * Returns a {@link TableEnvironment} for a Java batch {@link ExecutionEnvironment} that works
	 * with {@link DataSet}s.
	 *
	 * <p>A TableEnvironment can be used to:
	 * <ul>
	 *     <li>convert a {@link DataSet} to a {@link Table}</li>
	 *     <li>register a {@link DataSet} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>register a {@link Table} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>scan a registered table to obtain a {@link Table}</li>
	 *     <li>specify a SQL query on registered tables to obtain a {@link Table}</li>
	 *     <li>convert a {@link Table} into a {@link DataSet}</li>
	 *     <li>explain the AST and execution plan of a {@link Table}</li>
	 * </ul>
	 *
	 * @param executionEnvironment The Java batch {@link ExecutionEnvironment} of the TableEnvironment.
	 * @param tableConfig The configuration of the TableEnvironment.
	 */
	static BatchTableEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		try {
			Class<?> clazz = Class.forName("org.apache.flink.table.api.java.internal.BatchTableEnvironmentImpl");
			Constructor con = clazz.getConstructor(ExecutionEnvironment.class, TableConfig.class, CatalogManager.class, ModuleManager.class);
			String defaultCatalog = "default_catalog";
			CatalogManager catalogManager = new CatalogManager(
				defaultCatalog,
				new GenericInMemoryCatalog(defaultCatalog, "default_database")
			);
			ModuleManager moduleManager = new ModuleManager();
			return (BatchTableEnvironment) con.newInstance(executionEnvironment, tableConfig, catalogManager, moduleManager);
		} catch (Throwable t) {
			throw new TableException("Create BatchTableEnvironment failed.", t);
		}
	}
}
