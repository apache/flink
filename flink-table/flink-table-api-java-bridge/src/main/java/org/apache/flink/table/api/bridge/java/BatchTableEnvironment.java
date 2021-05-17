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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.descriptors.BatchTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.ModuleManager;

import java.lang.reflect.Constructor;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;

/**
 * The {@link TableEnvironment} for a Java batch {@link ExecutionEnvironment} that works with {@link
 * DataSet}s.
 *
 * <p>A TableEnvironment can be used to:
 *
 * <ul>
 *   <li>convert a {@link DataSet} to a {@link Table}
 *   <li>register a {@link DataSet} in the {@link TableEnvironment}'s catalog
 *   <li>register a {@link Table} in the {@link TableEnvironment}'s catalog
 *   <li>scan a registered table to obtain a {@link Table}
 *   <li>specify a SQL query on registered tables to obtain a {@link Table}
 *   <li>convert a {@link Table} into a {@link DataSet}
 *   <li>explain the AST and execution plan of a {@link Table}
 * </ul>
 *
 * @deprecated {@link BatchTableEnvironment} will be dropped in Flink 1.14 because it only supports
 *     the old planner. Use the unified {@link TableEnvironment} instead, which supports both batch
 *     and streaming. More advanced operations previously covered by the DataSet API can now use the
 *     DataStream API in BATCH execution mode.
 */
@Deprecated
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
     * <p>The field names of the {@link Table} are automatically derived from the type of the {@link
     * DataSet}.
     *
     * @param dataSet The {@link DataSet} to be converted.
     * @param <T> The type of the {@link DataSet}.
     * @return The converted {@link Table}.
     */
    <T> Table fromDataSet(DataSet<T> dataSet);

    /**
     * Converts the given {@link DataSet} into a {@link Table} with specified field names.
     *
     * <p>There are two modes for mapping original fields to the fields of the {@link Table}:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). In this mode, fields can be reordered and
     * projected out. This mode can be used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * // use the original 'f0' field and give a better name to the 'f1' field
     * Table table = tableEnv.fromTable(set, "f0, f1 as name");
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * // renames the original fields as 'a' and 'b'
     * Table table = tableEnv.fromDataSet(set, "a, b");
     * }</pre>
     *
     * @param dataSet The {@link DataSet} to be converted.
     * @param fields The fields expressions to map original fields of the DataSet to the fields of
     *     the {@link Table}.
     * @param <T> The type of the {@link DataSet}.
     * @return The converted {@link Table}.
     * @deprecated use {@link #fromDataSet(DataSet, Expression...)}
     */
    @Deprecated
    <T> Table fromDataSet(DataSet<T> dataSet, String fields);

    /**
     * Converts the given {@link DataSet} into a {@link Table} with specified field names.
     *
     * <p>There are two modes for mapping original fields to the fields of the {@link Table}:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). In this mode, fields can be reordered and
     * projected out. This mode can be used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * Table table = tableEnv.fromDataSet(
     *    set,
     *    $("f1"), // reorder and use the original field
     *    $("f0").as("name") // reorder and give the original field a better name
     * );
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * Table table = tableEnv.fromDataSet(
     *    set,
     *    $("a"), // renames the first field to 'a'
     *    $("b") // renames the second field to 'b'
     * );
     * }</pre>
     *
     * @param dataSet The {@link DataSet} to be converted.
     * @param fields The fields expressions to map original fields of the DataSet to the fields of
     *     the {@link Table}.
     * @param <T> The type of the {@link DataSet}.
     * @return The converted {@link Table}.
     */
    <T> Table fromDataSet(DataSet<T> dataSet, Expression... fields);

    /**
     * Creates a view from the given {@link DataSet}. Registered views can be referenced in SQL
     * queries.
     *
     * <p>The field names of the {@link Table} are automatically derived from the type of the {@link
     * DataSet}.
     *
     * <p>The view is registered in the namespace of the current catalog and database. To register
     * the view in a different catalog use {@link #createTemporaryView(String, DataSet)}.
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param name The name under which the {@link DataSet} is registered in the catalog.
     * @param dataSet The {@link DataSet} to register.
     * @param <T> The type of the {@link DataSet} to register.
     * @deprecated use {@link #createTemporaryView(String, DataSet)}
     */
    @Deprecated
    <T> void registerDataSet(String name, DataSet<T> dataSet);

    /**
     * Creates a view from the given {@link DataSet} in a given path. Registered views can be
     * referenced in SQL queries.
     *
     * <p>The field names of the {@link Table} are automatically derived from the type of the {@link
     * DataSet}.
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param path The path under which the view is created. See also the {@link TableEnvironment}
     *     class description for the format of the path.
     * @param dataSet The {@link DataSet} out of which to create the view.
     * @param <T> The type of the {@link DataSet}.
     */
    <T> void createTemporaryView(String path, DataSet<T> dataSet);

    /**
     * Creates a view from the given {@link DataSet} in a given path with specified field names.
     * Registered views can be referenced in SQL queries.
     *
     * <p>There are two modes for mapping original fields to the fields of the View:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). In this mode, fields can be reordered and
     * projected out. This mode can be used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * // use the original 'f0' field and give a better name to the 'f1' field
     * tableEnv.registerDataSet("myTable", set, "f0, f1 as name");
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * // renames the original fields as 'a' and 'b'
     * tableEnv.registerDataSet("myTable", set, "a, b");
     * }</pre>
     *
     * <p>The view is registered in the namespace of the current catalog and database. To register
     * the view in a different catalog use {@link #createTemporaryView(String, DataSet)}.
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param name The name under which the {@link DataSet} is registered in the catalog.
     * @param dataSet The {@link DataSet} to register.
     * @param fields The fields expressions to map original fields of the DataSet to the fields of
     *     the View.
     * @param <T> The type of the {@link DataSet} to register.
     * @deprecated use {@link #createTemporaryView(String, DataSet, String)}
     */
    @Deprecated
    <T> void registerDataSet(String name, DataSet<T> dataSet, String fields);

    /**
     * Creates a view from the given {@link DataSet} in a given path with specified field names.
     * Registered views can be referenced in SQL queries.
     *
     * <p>There are two modes for mapping original fields to the fields of the View:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). In this mode, fields can be reordered and
     * projected out. This mode can be used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * // use the original 'f0' field and give a better name to the 'f1' field
     * tableEnv.createTemporaryView("cat.db.myTable", set, "f0, f1 as name");
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * // renames the original fields as 'a' and 'b'
     * tableEnv.createTemporaryView("cat.db.myTable", set, "a, b");
     * }</pre>
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param path The path under which the view is created. See also the {@link TableEnvironment}
     *     class description for the format of the path.
     * @param dataSet The {@link DataSet} out of which to create the view.
     * @param fields The fields expressions to map original fields of the DataSet to the fields of
     *     the View.
     * @param <T> The type of the {@link DataSet}.
     * @deprecated use {@link #createTemporaryView(String, DataSet, Expression...)}
     */
    @Deprecated
    <T> void createTemporaryView(String path, DataSet<T> dataSet, String fields);

    /**
     * Creates a view from the given {@link DataSet} in a given path with specified field names.
     * Registered views can be referenced in SQL queries.
     *
     * <p>There are two modes for mapping original fields to the fields of the View:
     *
     * <p>1. Reference input fields by name: All fields in the schema definition are referenced by
     * name (and possibly renamed using an alias (as). In this mode, fields can be reordered and
     * projected out. This mode can be used for any input type, including POJOs.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * tableEnv.createTemporaryView(
     *    "cat.db.myTable",
     *    set,
     *    $("f1"), // reorder and use the original field
     *    $("f0").as("name") // reorder and give the original field a better name
     * );
     * }</pre>
     *
     * <p>2. Reference input fields by position: In this mode, fields are simply renamed. This mode
     * can only be used if the input type has a defined field order (tuple, case class, Row) and
     * none of the {@code fields} references a field of the input type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DataSet<Tuple2<String, Long>> set = ...
     * tableEnv.createTemporaryView(
     *    "cat.db.myTable",
     *    set,
     *    $("a"), // renames the first field to 'a'
     *    $("b") // renames the second field to 'b'
     * );
     * }</pre>
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param path The path under which the view is created. See also the {@link TableEnvironment}
     *     class description for the format of the path.
     * @param dataSet The {@link DataSet} out of which to create the view.
     * @param fields The fields expressions to map original fields of the DataSet to the fields of
     *     the View.
     * @param <T> The type of the {@link DataSet}.
     */
    <T> void createTemporaryView(String path, DataSet<T> dataSet, Expression... fields);

    /**
     * Converts the given {@link Table} into a {@link DataSet} of a specified type.
     *
     * <p>The fields of the {@link Table} are mapped to {@link DataSet} fields as follows:
     *
     * <ul>
     *   <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
     *       types: Fields are mapped by position, field types must match.
     *   <li>POJO {@link DataSet} types: Fields are mapped by field name, field types must match.
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
     *
     * <ul>
     *   <li>{@link org.apache.flink.types.Row} and {@link org.apache.flink.api.java.tuple.Tuple}
     *       types: Fields are mapped by position, field types must match.
     *   <li>POJO {@link DataSet} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param typeInfo The {@link TypeInformation} that specifies the type of the resulting {@link
     *     DataSet}.
     * @param <T> The type of the resulting {@link DataSet}.
     * @return The converted {@link DataSet}.
     */
    <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo);

    /**
     * Creates a temporary table from a descriptor.
     *
     * <p>Descriptors allow for declaring the communication to external systems in an
     * implementation-agnostic way. The classpath is scanned for suitable table factories that match
     * the desired configuration.
     *
     * <p>The following example shows how to read from a connector using a JSON format and
     * registering a temporary table as "MyTable":
     *
     * <pre>{@code
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
    BatchTableDescriptor connect(ConnectorDescriptor connectorDescriptor);

    /**
     * Returns a {@link TableEnvironment} for a Java batch {@link ExecutionEnvironment} that works
     * with {@link DataSet}s.
     *
     * <p>A TableEnvironment can be used to:
     *
     * <ul>
     *   <li>convert a {@link DataSet} to a {@link Table}
     *   <li>register a {@link DataSet} in the {@link TableEnvironment}'s catalog
     *   <li>register a {@link Table} in the {@link TableEnvironment}'s catalog
     *   <li>scan a registered table to obtain a {@link Table}
     *   <li>specify a SQL query on registered tables to obtain a {@link Table}
     *   <li>convert a {@link Table} into a {@link DataSet}
     *   <li>explain the AST and execution plan of a {@link Table}
     * </ul>
     *
     * @param executionEnvironment The Java batch {@link ExecutionEnvironment} of the
     *     TableEnvironment.
     */
    static BatchTableEnvironment create(ExecutionEnvironment executionEnvironment) {
        Configuration configuration = new Configuration();
        configuration.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        TableConfig config = new TableConfig();
        config.addConfiguration(configuration);
        return create(executionEnvironment, config);
    }

    /**
     * Returns a {@link TableEnvironment} for a Java batch {@link ExecutionEnvironment} that works
     * with {@link DataSet}s.
     *
     * <p>A TableEnvironment can be used to:
     *
     * <ul>
     *   <li>convert a {@link DataSet} to a {@link Table}
     *   <li>register a {@link DataSet} in the {@link TableEnvironment}'s catalog
     *   <li>register a {@link Table} in the {@link TableEnvironment}'s catalog
     *   <li>scan a registered table to obtain a {@link Table}
     *   <li>specify a SQL query on registered tables to obtain a {@link Table}
     *   <li>convert a {@link Table} into a {@link DataSet}
     *   <li>explain the AST and execution plan of a {@link Table}
     * </ul>
     *
     * @param executionEnvironment The Java batch {@link ExecutionEnvironment} of the
     *     TableEnvironment.
     * @param tableConfig The configuration of the TableEnvironment.
     */
    static BatchTableEnvironment create(
            ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
        try {
            // temporary solution until FLINK-15635 is fixed
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            ModuleManager moduleManager = new ModuleManager();

            String defaultCatalog = "default_catalog";
            CatalogManager catalogManager =
                    CatalogManager.newBuilder()
                            .classLoader(classLoader)
                            .config(tableConfig.getConfiguration())
                            .defaultCatalog(
                                    defaultCatalog,
                                    new GenericInMemoryCatalog(defaultCatalog, "default_database"))
                            .executionConfig(executionEnvironment.getConfig())
                            .build();

            Class<?> clazz =
                    Class.forName(
                            "org.apache.flink.table.api.bridge.java.internal.BatchTableEnvironmentImpl");
            Constructor<?> con =
                    clazz.getConstructor(
                            ExecutionEnvironment.class,
                            TableConfig.class,
                            CatalogManager.class,
                            ModuleManager.class);
            return (BatchTableEnvironment)
                    con.newInstance(
                            executionEnvironment, tableConfig, catalogManager, moduleManager);
        } catch (Throwable t) {
            throw new TableException("Create BatchTableEnvironment failed.", t);
        }
    }
}
