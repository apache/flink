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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sinks.TableSink;

/**
 * This table environment is the entry point and central context for creating Table & SQL
 * API programs that integrate with the Java-specific {@link DataStream} API.
 *
 * <p>It is unified for bounded and unbounded data processing.
 *
 * <p>A stream table environment is responsible for:
 * <ul>
 *     <li>Convert a {@link DataStream} into {@link Table} and vice-versa.</li>
 *     <li>Connecting to external systems.</li>
 *     <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.</li>
 *     <li>Executing SQL statements.</li>
 *     <li>Offering further configuration options.</li>
 * </ul>
 *
 * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is meant
 * for pure table programs.
 */
@PublicEvolving
public interface StreamTableEnvironment extends TableEnvironment {

	/**
	 * Creates a table environment that is the entry point and central context for creating Table & SQL
	 * API programs that integrate with the Java-specific {@link DataStream} API.
	 *
	 * <p>It is unified for bounded and unbounded data processing.
	 *
	 * <p>A stream table environment is responsible for:
	 * <ul>
	 *     <li>Convert a {@link DataStream} into {@link Table} and vice-versa.</li>
	 *     <li>Connecting to external systems.</li>
	 *     <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.</li>
	 *     <li>Executing SQL statements.</li>
	 *     <li>Offering further configuration options.</li>
	 * </ul>
	 *
	 * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is meant
	 * for pure table programs.
	 *
	 * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the {@link TableEnvironment}.
	 */
	static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment) {
		return create(
			executionEnvironment,
			EnvironmentSettings.newInstance().build());
	}

	/**
	 * Creates a table environment that is the entry point and central context for creating Table & SQL
	 * API programs that integrate with the Java-specific {@link DataStream} API.
	 *
	 * <p>It is unified for bounded and unbounded data processing.
	 *
	 * <p>A stream table environment is responsible for:
	 * <ul>
	 *     <li>Convert a {@link DataStream} into {@link Table} and vice-versa.</li>
	 *     <li>Connecting to external systems.</li>
	 *     <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.</li>
	 *     <li>Executing SQL statements.</li>
	 *     <li>Offering further configuration options.</li>
	 * </ul>
	 *
	 * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is meant
	 * for pure table programs.
	 *
	 * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the {@link TableEnvironment}.
	 * @param settings The environment settings used to instantiate the {@link TableEnvironment}.
	 */
	static StreamTableEnvironment create(
			StreamExecutionEnvironment executionEnvironment,
			EnvironmentSettings settings) {
		return StreamTableEnvironmentImpl.create(
			executionEnvironment,
			settings,
			new TableConfig()
		);
	}

	/**
	 * Creates a table environment that is the entry point and central context for creating Table & SQL
	 * API programs that integrate with the Java-specific {@link DataStream} API.
	 *
	 * <p>It is unified for bounded and unbounded data processing.
	 *
	 * <p>A stream table environment is responsible for:
	 * <ul>
	 *     <li>Convert a {@link DataStream} into {@link Table} and vice-versa.</li>
	 *     <li>Connecting to external systems.</li>
	 *     <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.</li>
	 *     <li>Executing SQL statements.</li>
	 *     <li>Offering further configuration options.</li>
	 * </ul>
	 *
	 * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is meant
	 * for pure table programs.
	 *
	 * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the {@link TableEnvironment}.
	 * @param tableConfig The configuration of the {@link TableEnvironment}.
	 * @deprecated Use {@link #create(StreamExecutionEnvironment)} and {@link #getConfig()}
	 * for manipulating {@link TableConfig}.
	 */
	@Deprecated
	static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		return StreamTableEnvironmentImpl.create(
			executionEnvironment,
			EnvironmentSettings.newInstance().build(),
			tableConfig);
	}

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
	 * @tparam ACC The type of aggregate accumulator.
	 */
	<T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction);

	/**
	 * Registers an {@link TableAggregateFunction} under a unique name in the TableEnvironment's
	 * catalog. Registered functions can only be referenced in Table API.
	 *
	 * @param name The name under which the function is registered.
	 * @param tableAggregateFunction The TableAggregateFunction to register.
	 * @param <T> The type of the output value.
	 * @tparam ACC The type of aggregate accumulator.
	 */
	<T, ACC> void registerFunction(String name, TableAggregateFunction<T, ACC> tableAggregateFunction);

	/**
	 * Converts the given {@link DataStream} into a {@link Table}.
	 *
	 * The field names of the {@link Table} are automatically derived from the type of the
	 * {@link DataStream}.
	 *
	 * @param dataStream The {@link DataStream} to be converted.
	 * @param <T> The type of the {@link DataStream}.
	 * @return The converted {@link Table}.
	 */
	<T> Table fromDataStream(DataStream<T> dataStream);

	/**
	 * Converts the given {@link DataStream} into a {@link Table} with specified field names.
	 *
	 * Example:
	 *
	 * <pre>
	 * {@code
	 *   DataStream<Tuple2<String, Long>> stream = ...
	 *   Table tab = tableEnv.fromDataStream(stream, "a, b");
	 * }
	 * </pre>
	 *
	 * @param dataStream The {@link DataStream} to be converted.
	 * @param fields The field names of the resulting {@link Table}.
	 * @param <T> The type of the {@link DataStream}.
	 * @return The converted {@link Table}.
	 */
	<T> Table fromDataStream(DataStream<T> dataStream, String fields);

	/**
	 * Registers the given {@link DataStream} as table in the {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * The field names of the {@link Table} are automatically derived
	 * from the type of the {@link DataStream}.
	 *
	 * @param name The name under which the {@link DataStream} is registered in the catalog.
	 * @param dataStream The {@link DataStream} to register.
	 * @param <T> The type of the {@link DataStream} to register.
	 */
	<T> void registerDataStream(String name, DataStream<T> dataStream);

	/**
	 * Registers the given {@link DataStream} as table with specified field names in the
	 * {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * Example:
	 *
	 * <pre>
	 * {@code
	 *   DataStream<Tuple2<String, Long>> set = ...
	 *   tableEnv.registerDataStream("myTable", set, "a, b")
	 * }
	 * </pre>
	 *
	 * @param name The name under which the {@link DataStream} is registered in the catalog.
	 * @param dataStream The {@link DataStream} to register.
	 * @param fields The field names of the registered table.
	 * @param <T> The type of the {@link DataStream} to register.
	 */
	<T> void registerDataStream(String name, DataStream<T> dataStream, String fields);

	/**
	 * Converts the given {@link Table} into an append {@link DataStream} of a specified type.
	 *
	 * The {@link Table} must only have insert (append) changes. If the {@link Table} is also modified
	 * by update or delete changes, the conversion will fail.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param clazz The class of the type of the resulting {@link DataStream}.
	 * @param <T> The type of the resulting {@link DataStream}.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<T> toAppendStream(Table table, Class<T> clazz);

	/**
	 * Converts the given {@link Table} into an append {@link DataStream} of a specified type.
	 *
	 * The {@link Table} must only have insert (append) changes. If the {@link Table} is also modified
	 * by update or delete changes, the conversion will fail.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param typeInfo The {@link TypeInformation} that specifies the type of the {@link DataStream}.
	 * @param <T> The type of the resulting {@link DataStream}.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo);

	/**
	 * Converts the given {@link Table} into an append {@link DataStream} of a specified type.
	 *
	 * The {@link Table} must only have insert (append) changes. If the {@link Table} is also modified
	 * by update or delete changes, the conversion will fail.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param clazz The class of the type of the resulting {@link DataStream}.
	 * @param queryConfig The configuration of the query to generate.
	 * @param <T> The type of the resulting {@link DataStream}.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<T> toAppendStream(Table table, Class<T> clazz, StreamQueryConfig queryConfig);

	/**
	 * Converts the given {@link Table} into an append {@link DataStream} of a specified type.
	 *
	 * The {@link Table} must only have insert (append) changes. If the {@link Table} is also modified
	 * by update or delete changes, the conversion will fail.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param typeInfo The {@link TypeInformation} that specifies the type of the {@link DataStream}.
	 * @param queryConfig The configuration of the query to generate.
	 * @param <T> The type of the resulting {@link DataStream}.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo, StreamQueryConfig queryConfig);

	/**
	 * Converts the given {@link Table} into a {@link DataStream} of add and retract messages.
	 * The message will be encoded as {@link Tuple2}. The first field is a {@link Boolean} flag,
	 * the second field holds the record of the specified type {@link T}.
	 *
	 * A true {@link Boolean} flag indicates an add message, a false flag indicates a retract message.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param clazz The class of the requested record type.
	 * @param <T> The type of the requested record type.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz);

	/**
	 * Converts the given {@link Table} into a {@link DataStream} of add and retract messages.
	 * The message will be encoded as {@link Tuple2}. The first field is a {@link Boolean} flag,
	 * the second field holds the record of the specified type {@link T}.
	 *
	 * A true {@link Boolean} flag indicates an add message, a false flag indicates a retract message.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param typeInfo The {@link TypeInformation} of the requested record type.
	 * @param <T> The type of the requested record type.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo);

	/**
	 * Converts the given {@link Table} into a {@link DataStream} of add and retract messages.
	 * The message will be encoded as {@link Tuple2}. The first field is a {@link Boolean} flag,
	 * the second field holds the record of the specified type {@link T}.
	 *
	 * A true {@link Boolean} flag indicates an add message, a false flag indicates a retract message.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param clazz The class of the requested record type.
	 * @param queryConfig The configuration of the query to generate.
	 * @param <T> The type of the requested record type.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz, StreamQueryConfig queryConfig);

	/**
	 * Converts the given {@link Table} into a {@link DataStream} of add and retract messages.
	 * The message will be encoded as {@link Tuple2}. The first field is a {@link Boolean} flag,
	 * the second field holds the record of the specified type {@link T}.
	 *
	 * A true {@link Boolean} flag indicates an add message, a false flag indicates a retract message.
	 *
	 * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
	 * <ul>
	 *     <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
	 *     types: Fields are mapped by position, field types must match.</li>
	 *     <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.</li>
	 * </ul>
	 *
	 * @param table The {@link Table} to convert.
	 * @param typeInfo The {@link TypeInformation} of the requested record type.
	 * @param queryConfig The configuration of the query to generate.
	 * @param <T> The type of the requested record type.
	 * @return The converted {@link DataStream}.
	 */
	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo, StreamQueryConfig queryConfig);

	/**
	 * Creates a table source and/or table sink from a descriptor.
	 *
	 * <p>Descriptors allow for declaring the communication to external systems in an
	 * implementation-agnostic way. The classpath is scanned for suitable table factories that match
	 * the desired configuration.
	 *
	 * <p>The following example shows how to read from a Kafka connector using a JSON format and
	 * registering a table source "MyTable" in append mode:
	 *
	 * <pre>
	 * {@code
	 *
	 * tableEnv
	 *   .connect(
	 *     new Kafka()
	 *       .version("0.11")
	 *       .topic("clicks")
	 *       .property("zookeeper.connect", "localhost")
	 *       .property("group.id", "click-group")
	 *       .startFromEarliest())
	 *   .withFormat(
	 *     new Json()
	 *       .jsonSchema("{...}")
	 *       .failOnMissingField(false))
	 *   .withSchema(
	 *     new Schema()
	 *       .field("user-name", "VARCHAR").from("u_name")
	 *       .field("count", "DECIMAL")
	 *       .field("proc-time", "TIMESTAMP").proctime())
	 *   .inAppendMode()
	 *   .registerSource("MyTable")
	 * }
	 * </pre>
	 *
	 * @param connectorDescriptor connector descriptor describing the external system
	 */
	@Override
	StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor);

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
	 * @param config The {@link QueryConfig} to use.
	 */
	void sqlUpdate(String stmt, StreamQueryConfig config);

	/**
	 * Writes the {@link Table} to a {@link TableSink} that was registered under the specified name.
	 *
	 * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or
	 * {@link TableEnvironment#useCatalog(String)} for the rules on the path resolution.
	 *
	 * @param table The Table to write to the sink.
	 * @param queryConfig The {@link StreamQueryConfig} to use.
	 * @param sinkPath The first part of the path of the registered {@link TableSink} to which the {@link Table} is
	 *        written. This is to ensure at least the name of the {@link TableSink} is provided.
	 * @param sinkPathContinued The remaining part of the path of the registered {@link TableSink} to which the
	 *        {@link Table} is written.
	 */
	void insertInto(Table table, StreamQueryConfig queryConfig, String sinkPath, String... sinkPathContinued);

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program.
	 *
	 * <p>The program execution will be logged and displayed with the provided name
	 *
	 * <p>It calls the {@link StreamExecutionEnvironment#execute(String)} on the underlying
	 * {@link StreamExecutionEnvironment}. In contrast to the {@link TableEnvironment} this
	 * environment translates queries eagerly. Therefore the values in {@link QueryConfig}
	 * parameter are ignored.
	 *
	 * @param jobName Desired name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	@Override
	JobExecutionResult execute(String jobName) throws Exception;
}
