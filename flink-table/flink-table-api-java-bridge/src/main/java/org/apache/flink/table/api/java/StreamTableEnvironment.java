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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;

import java.lang.reflect.Constructor;

/**
 * The {@link TableEnvironment} for a Java {@link StreamExecutionEnvironment} that works with
 * {@link DataStream}s.
 *
 * <p>A TableEnvironment can be used to:
 * <ul>
 *     <li>convert a {@link DataStream} to a {@link Table}</li>
 *     <li>register a {@link DataStream} in the {@link TableEnvironment}'s catalog</li>
 *     <li>register a {@link Table} in the {@link TableEnvironment}'s catalog</li>
 *     <li>scan a registered table to obtain a {@link Table}</li>
 *     <li>specify a SQL query on registered tables to obtain a {@link Table}</li>
 *     <li>convert a {@link Table} into a {@link DataStream}</li>
 *     <li>explain the AST and execution plan of a {@link Table}</li>
 * </ul>
 */
@PublicEvolving
public interface StreamTableEnvironment extends TableEnvironment {

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
	 * {@ocde
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
	 * The {@link TableEnvironment} for a Java {@link StreamExecutionEnvironment} that works with
	 * {@link DataStream}s.
	 *
	 * <p>A TableEnvironment can be used to:
	 * <ul>
	 *     <li>convert a {@link DataStream} to a {@link Table}</li>
	 *     <li>register a {@link DataStream} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>register a {@link Table} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>scan a registered table to obtain a {@link Table}</li>
	 *     <li>specify a SQL query on registered tables to obtain a {@link Table}</li>
	 *     <li>convert a {@link Table} into a {@link DataStream}</li>
	 *     <li>explain the AST and execution plan of a {@link Table}</li>
	 * </ul>
	 *
	 * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the TableEnvironment.
	 */
	static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	/**
	 * The {@link TableEnvironment} for a Java {@link StreamExecutionEnvironment} that works with
	 * {@link DataStream}s.
	 *
	 * <p>A TableEnvironment can be used to:
	 * <ul>
	 *     <li>convert a {@link DataStream} to a {@link Table}</li>
	 *     <li>register a {@link DataStream} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>register a {@link Table} in the {@link TableEnvironment}'s catalog</li>
	 *     <li>scan a registered table to obtain a {@link Table}</li>
	 *     <li>specify a SQL query on registered tables to obtain a {@link Table}</li>
	 *     <li>convert a {@link Table} into a {@link DataStream}</li>
	 *     <li>explain the AST and execution plan of a {@link Table}</li>
	 * </ul>
	 *
	 * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the TableEnvironment.
	 * @param tableConfig The configuration of the TableEnvironment.
	 */
	static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		try {
			Class clazz = Class.forName("org.apache.flink.table.api.java.StreamTableEnvImpl");
			Constructor con = clazz.getConstructor(StreamExecutionEnvironment.class, TableConfig.class);
			return (StreamTableEnvironment) con.newInstance(executionEnvironment, tableConfig);
		} catch (Throwable t) {
			throw new TableException("Create StreamTableEnvironment failed.", t);
		}
	}
}
