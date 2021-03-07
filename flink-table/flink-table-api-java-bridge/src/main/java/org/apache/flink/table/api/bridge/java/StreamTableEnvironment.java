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

package org.apache.flink.table.api.bridge.java;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * This table environment is the entry point and central context for creating Table and SQL API
 * programs that integrate with the Java-specific {@link DataStream} API.
 *
 * <p>It is unified for bounded and unbounded data processing.
 *
 * <p>A stream table environment is responsible for:
 *
 * <ul>
 *   <li>Convert a {@link DataStream} into {@link Table} and vice-versa.
 *   <li>Connecting to external systems.
 *   <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.
 *   <li>Executing SQL statements.
 *   <li>Offering further configuration options.
 * </ul>
 *
 * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is meant
 * for pure table programs.
 */
@PublicEvolving
public interface StreamTableEnvironment extends TableEnvironment {

    /**
     * Creates a table environment that is the entry point and central context for creating Table
     * and SQL API programs that integrate with the Java-specific {@link DataStream} API.
     *
     * <p>It is unified for bounded and unbounded data processing.
     *
     * <p>A stream table environment is responsible for:
     *
     * <ul>
     *   <li>Convert a {@link DataStream} into {@link Table} and vice-versa.
     *   <li>Connecting to external systems.
     *   <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.
     *   <li>Executing SQL statements.
     *   <li>Offering further configuration options.
     * </ul>
     *
     * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is
     * meant for pure table programs.
     *
     * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the {@link
     *     TableEnvironment}.
     */
    static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment) {
        return create(executionEnvironment, EnvironmentSettings.newInstance().build());
    }

    /**
     * Creates a table environment that is the entry point and central context for creating Table
     * and SQL API programs that integrate with the Java-specific {@link DataStream} API.
     *
     * <p>It is unified for bounded and unbounded data processing.
     *
     * <p>A stream table environment is responsible for:
     *
     * <ul>
     *   <li>Convert a {@link DataStream} into {@link Table} and vice-versa.
     *   <li>Connecting to external systems.
     *   <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.
     *   <li>Executing SQL statements.
     *   <li>Offering further configuration options.
     * </ul>
     *
     * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is
     * meant for pure table programs.
     *
     * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the {@link
     *     TableEnvironment}.
     * @param settings The environment settings used to instantiate the {@link TableEnvironment}.
     */
    static StreamTableEnvironment create(
            StreamExecutionEnvironment executionEnvironment, EnvironmentSettings settings) {
        TableConfig config = new TableConfig();
        config.addConfiguration(settings.toConfiguration());
        return StreamTableEnvironmentImpl.create(executionEnvironment, settings, config);
    }

    /**
     * Creates a table environment that is the entry point and central context for creating Table
     * and SQL API programs that integrate with the Java-specific {@link DataStream} API.
     *
     * <p>It is unified for bounded and unbounded data processing.
     *
     * <p>A stream table environment is responsible for:
     *
     * <ul>
     *   <li>Convert a {@link DataStream} into {@link Table} and vice-versa.
     *   <li>Connecting to external systems.
     *   <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.
     *   <li>Executing SQL statements.
     *   <li>Offering further configuration options.
     * </ul>
     *
     * <p>Note: If you don't intend to use the {@link DataStream} API, {@link TableEnvironment} is
     * meant for pure table programs.
     *
     * @param executionEnvironment The Java {@link StreamExecutionEnvironment} of the {@link
     *     TableEnvironment}.
     * @param tableConfig The configuration of the {@link TableEnvironment}.
     * @deprecated Use {@link #create(StreamExecutionEnvironment)} and {@link #getConfig()} for
     *     manipulating {@link TableConfig}.
     */
    @Deprecated
    static StreamTableEnvironment create(
            StreamExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
        return StreamTableEnvironmentImpl.create(
                executionEnvironment,
                EnvironmentSettings.fromConfiguration(tableConfig.getConfiguration()),
                tableConfig);
    }

    /**
     * Registers a {@link TableFunction} under a unique name in the TableEnvironment's catalog.
     * Registered functions can be referenced in Table API and SQL queries.
     *
     * @param name The name under which the function is registered.
     * @param tableFunction The TableFunction to register.
     * @param <T> The type of the output row.
     * @deprecated Use {@link #createTemporarySystemFunction(String, UserDefinedFunction)} instead.
     *     Please note that the new method also uses the new type system and reflective extraction
     *     logic. It might be necessary to update the function implementation as well. See the
     *     documentation of {@link TableFunction} for more information on the new function design.
     */
    @Deprecated
    <T> void registerFunction(String name, TableFunction<T> tableFunction);

    /**
     * Registers an {@link AggregateFunction} under a unique name in the TableEnvironment's catalog.
     * Registered functions can be referenced in Table API and SQL queries.
     *
     * @param name The name under which the function is registered.
     * @param aggregateFunction The AggregateFunction to register.
     * @param <T> The type of the output value.
     * @param <ACC> The type of aggregate accumulator.
     * @deprecated Use {@link #createTemporarySystemFunction(String, UserDefinedFunction)} instead.
     *     Please note that the new method also uses the new type system and reflective extraction
     *     logic. It might be necessary to update the function implementation as well. See the
     *     documentation of {@link AggregateFunction} for more information on the new function
     *     design.
     */
    @Deprecated
    <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction);

    /**
     * Registers an {@link TableAggregateFunction} under a unique name in the TableEnvironment's
     * catalog. Registered functions can only be referenced in Table API.
     *
     * @param name The name under which the function is registered.
     * @param tableAggregateFunction The TableAggregateFunction to register.
     * @param <T> The type of the output value.
     * @param <ACC> The type of aggregate accumulator.
     * @deprecated Use {@link #createTemporarySystemFunction(String, UserDefinedFunction)} instead.
     *     Please note that the new method also uses the new type system and reflective extraction
     *     logic. It might be necessary to update the function implementation as well. See the
     *     documentation of {@link TableAggregateFunction} for more information on the new function
     *     design.
     */
    @Deprecated
    <T, ACC> void registerFunction(
            String name, TableAggregateFunction<T, ACC> tableAggregateFunction);

    /**
     * Converts the given {@link DataStream} into a {@link Table}.
     *
     * <p>The field names of the {@link Table} are automatically derived from the type of the {@link
     * DataStream}.
     *
     * @param dataStream The {@link DataStream} to be converted.
     * @param <T> The type of the {@link DataStream}.
     * @return The converted {@link Table}.
     */
    <T> Table fromDataStream(DataStream<T> dataStream);

    /**
     * Converts the given {@link DataStream} into a {@link Table} with specified field names.
     *
     * <p>There are two modes for mapping original fields to the fields of the {@link Table}:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
     * attributes at arbitrary positions using arbitrary names (except those that exist in the
     * result schema). In this mode, fields can be reordered and projected out. This mode can be
     * used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * // reorder the fields, rename the original 'f0' field to 'name' and add event-time
     * // attribute named 'rowtime'
     * Table table = tableEnv.fromDataStream(stream, "f1, rowtime.rowtime, f0 as 'name'");
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. Event-time
     * attributes can replace the field on their position in the input data (if it is of correct
     * type) or be appended at the end. Proctime attributes must be appended at the end. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * // rename the original fields to 'a' and 'b' and extract the internally attached timestamp into an event-time
     * // attribute named 'rowtime'
     * Table table = tableEnv.fromDataStream(stream, "a, b, rowtime.rowtime");
     * }</pre>
     *
     * @param dataStream The {@link DataStream} to be converted.
     * @param fields The fields expressions to map original fields of the DataStream to the fields
     *     of the {@link Table}.
     * @param <T> The type of the {@link DataStream}.
     * @return The converted {@link Table}.
     * @deprecated use {@link #fromDataStream(DataStream, Expression...)}
     */
    @Deprecated
    <T> Table fromDataStream(DataStream<T> dataStream, String fields);

    /**
     * Converts the given {@link DataStream} into a {@link Table} with specified field names.
     *
     * <p>There are two modes for mapping original fields to the fields of the {@link Table}:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
     * attributes at arbitrary positions using arbitrary names (except those that exist in the
     * result schema). In this mode, fields can be reordered and projected out. This mode can be
     * used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * Table table = tableEnv.fromDataStream(
     *    stream,
     *    $("f1"), // reorder and use the original field
     *    $("rowtime").rowtime(), // extract the internally attached timestamp into an event-time
     *                            // attribute named 'rowtime'
     *    $("f0").as("name") // reorder and give the original field a better name
     * );
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. Event-time
     * attributes can replace the field on their position in the input data (if it is of correct
     * type) or be appended at the end. Proctime attributes must be appended at the end. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * Table table = tableEnv.fromDataStream(
     *    stream,
     *    $("a"), // rename the first field to 'a'
     *    $("b"), // rename the second field to 'b'
     *    $("rowtime").rowtime() // extract the internally attached timestamp into an event-time
     *                           // attribute named 'rowtime'
     * );
     * }</pre>
     *
     * @param dataStream The {@link DataStream} to be converted.
     * @param fields The fields expressions to map original fields of the DataStream to the fields
     *     of the {@code Table}.
     * @param <T> The type of the {@link DataStream}.
     * @return The converted {@link Table}.
     */
    <T> Table fromDataStream(DataStream<T> dataStream, Expression... fields);

    /**
     * Creates a view from the given {@link DataStream}. Registered views can be referenced in SQL
     * queries.
     *
     * <p>The field names of the {@link Table} are automatically derived from the type of the {@link
     * DataStream}.
     *
     * <p>The view is registered in the namespace of the current catalog and database. To register
     * the view in a different catalog use {@link #createTemporaryView(String, DataStream)}.
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param name The name under which the {@link DataStream} is registered in the catalog.
     * @param dataStream The {@link DataStream} to register.
     * @param <T> The type of the {@link DataStream} to register.
     * @deprecated use {@link #createTemporaryView(String, DataStream)}
     */
    @Deprecated
    <T> void registerDataStream(String name, DataStream<T> dataStream);

    /**
     * Creates a view from the given {@link DataStream} in a given path. Registered views can be
     * referenced in SQL queries.
     *
     * <p>The field names of the {@link Table} are automatically derived from the type of the {@link
     * DataStream}.
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param path The path under which the {@link DataStream} is created. See also the {@link
     *     TableEnvironment} class description for the format of the path.
     * @param dataStream The {@link DataStream} out of which to create the view.
     * @param <T> The type of the {@link DataStream}.
     */
    <T> void createTemporaryView(String path, DataStream<T> dataStream);

    /**
     * Creates a view from the given {@link DataStream} in a given path with specified field names.
     * Registered views can be referenced in SQL queries.
     *
     * <p>There are two modes for mapping original fields to the fields of the View:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
     * attributes at arbitrary positions using arbitrary names (except those that exist in the
     * result schema). In this mode, fields can be reordered and projected out. This mode can be
     * used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * // reorder the fields, rename the original 'f0' field to 'name' and add event-time
     * // attribute named 'rowtime'
     * tableEnv.registerDataStream("myTable", stream, "f1, rowtime.rowtime, f0 as 'name'");
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. Event-time
     * attributes can replace the field on their position in the input data (if it is of correct
     * type) or be appended at the end. Proctime attributes must be appended at the end. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * // rename the original fields to 'a' and 'b' and extract the internally attached timestamp into an event-time
     * // attribute named 'rowtime'
     * tableEnv.registerDataStream("myTable", stream, "a, b, rowtime.rowtime");
     * }</pre>
     *
     * <p>The view is registered in the namespace of the current catalog and database. To register
     * the view in a different catalog use {@link #createTemporaryView(String, DataStream)}.
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param name The name under which the {@link DataStream} is registered in the catalog.
     * @param dataStream The {@link DataStream} to register.
     * @param fields The fields expressions to map original fields of the DataStream to the fields
     *     of the View.
     * @param <T> The type of the {@link DataStream} to register.
     * @deprecated use {@link #createTemporaryView(String, DataStream, Expression...)}
     */
    @Deprecated
    <T> void registerDataStream(String name, DataStream<T> dataStream, String fields);

    /**
     * Creates a view from the given {@link DataStream} in a given path with specified field names.
     * Registered views can be referenced in SQL queries.
     *
     * <p>There are two modes for mapping original fields to the fields of the View:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
     * attributes at arbitrary positions using arbitrary names (except those that exist in the
     * result schema). In this mode, fields can be reordered and projected out. This mode can be
     * used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * // reorder the fields, rename the original 'f0' field to 'name' and add event-time
     * // attribute named 'rowtime'
     * tableEnv.createTemporaryView("cat.db.myTable", stream, "f1, rowtime.rowtime, f0 as 'name'");
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. Event-time
     * attributes can replace the field on their position in the input data (if it is of correct
     * type) or be appended at the end. Proctime attributes must be appended at the end. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * // rename the original fields to 'a' and 'b' and extract the internally attached timestamp into an event-time
     * // attribute named 'rowtime'
     * tableEnv.createTemporaryView("cat.db.myTable", stream, "a, b, rowtime.rowtime");
     * }</pre>
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param path The path under which the {@link DataStream} is created. See also the {@link
     *     TableEnvironment} class description for the format of the path.
     * @param dataStream The {@link DataStream} out of which to create the view.
     * @param fields The fields expressions to map original fields of the DataStream to the fields
     *     of the View.
     * @param <T> The type of the {@link DataStream}.
     * @deprecated use {@link #createTemporaryView(String, DataStream, Expression...)}
     */
    @Deprecated
    <T> void createTemporaryView(String path, DataStream<T> dataStream, String fields);

    /**
     * Creates a view from the given {@link DataStream} in a given path with specified field names.
     * Registered views can be referenced in SQL queries.
     *
     * <p>There are two modes for mapping original fields to the fields of the View:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
     * attributes at arbitrary positions using arbitrary names (except those that exist in the
     * result schema). In this mode, fields can be reordered and projected out. This mode can be
     * used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * tableEnv.createTemporaryView(
     *    "cat.db.myTable",
     *    stream,
     *    $("f1"), // reorder and use the original field
     *    $("rowtime").rowtime(), // extract the internally attached timestamp into an event-time
     *                            // attribute named 'rowtime'
     *    $("f0").as("name") // reorder and give the original field a better name
     * );
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. Event-time
     * attributes can replace the field on their position in the input data (if it is of correct
     * type) or be appended at the end. Proctime attributes must be appended at the end. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Long>> stream = ...
     * tableEnv.createTemporaryView(
     *    "cat.db.myTable",
     *    stream,
     *    $("a"), // rename the first field to 'a'
     *    $("b"), // rename the second field to 'b'
     *    $("rowtime").rowtime() // adds an event-time attribute named 'rowtime'
     * );
     * }</pre>
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param path The path under which the {@link DataStream} is created. See also the {@link
     *     TableEnvironment} class description for the format of the path.
     * @param dataStream The {@link DataStream} out of which to create the view.
     * @param fields The fields expressions to map original fields of the DataStream to the fields
     *     of the View.
     * @param <T> The type of the {@link DataStream}.
     */
    <T> void createTemporaryView(String path, DataStream<T> dataStream, Expression... fields);

    /**
     * Converts the given {@link Table} into an append {@link DataStream} of a specified type.
     *
     * <p>The {@link Table} must only have insert (append) changes. If the {@link Table} is also
     * modified by update or delete changes, the conversion will fail.
     *
     * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
     *
     * <ul>
     *   <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
     *       types: Fields are mapped by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
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
     * <p>The {@link Table} must only have insert (append) changes. If the {@link Table} is also
     * modified by update or delete changes, the conversion will fail.
     *
     * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
     *
     * <ul>
     *   <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
     *       types: Fields are mapped by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param typeInfo The {@link TypeInformation} that specifies the type of the {@link
     *     DataStream}.
     * @param <T> The type of the resulting {@link DataStream}.
     * @return The converted {@link DataStream}.
     */
    <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo);

    /**
     * Converts the given {@link Table} into a {@link DataStream} of add and retract messages. The
     * message will be encoded as {@link Tuple2}. The first field is a {@link Boolean} flag, the
     * second field holds the record of the specified type {@link T}.
     *
     * <p>A true {@link Boolean} flag indicates an add message, a false flag indicates a retract
     * message.
     *
     * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
     *
     * <ul>
     *   <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
     *       types: Fields are mapped by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param clazz The class of the requested record type.
     * @param <T> The type of the requested record type.
     * @return The converted {@link DataStream}.
     */
    <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz);

    /**
     * Converts the given {@link Table} into a {@link DataStream} of add and retract messages. The
     * message will be encoded as {@link Tuple2}. The first field is a {@link Boolean} flag, the
     * second field holds the record of the specified type {@link T}.
     *
     * <p>A true {@link Boolean} flag indicates an add message, a false flag indicates a retract
     * message.
     *
     * <p>The fields of the {@link Table} are mapped to {@link DataStream} fields as follows:
     *
     * <ul>
     *   <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
     *       types: Fields are mapped by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param typeInfo The {@link TypeInformation} of the requested record type.
     * @param <T> The type of the requested record type.
     * @return The converted {@link DataStream}.
     */
    <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo);

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
     * <pre>{@code
     * tableEnv
     *   .connect(
     *     new Kafka()
     *       .version("0.11")
     *       .topic("clicks")
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
     *   .createTemporaryTable("MyTable")
     * }</pre>
     *
     * @param connectorDescriptor connector descriptor describing the external system
     * @deprecated The SQL {@code CREATE TABLE} DDL is richer than this part of the API. This method
     *     might be refactored in the next versions. Please use {@link #executeSql(String)
     *     executeSql(ddl)} to register a table instead.
     */
    @Override
    @Deprecated
    StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor);

    /**
     * Triggers the program execution. The environment will execute all parts of the program.
     *
     * <p>The program execution will be logged and displayed with the provided name
     *
     * <p>It calls the {@link StreamExecutionEnvironment#execute(String)} on the underlying {@link
     * StreamExecutionEnvironment}. In contrast to the {@link TableEnvironment} this environment
     * translates queries eagerly.
     *
     * @param jobName Desired name of the job
     * @return The result of the job execution, containing elapsed time and accumulators.
     * @throws Exception which occurs during job execution.
     */
    @Override
    JobExecutionResult execute(String jobName) throws Exception;
}
