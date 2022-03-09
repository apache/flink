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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

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
        return StreamTableEnvironmentImpl.create(executionEnvironment, settings);
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
     * <p>Column names and types of the {@link Table} are automatically derived from the {@link
     * TypeInformation} of the {@link DataStream}. If the outermost record's {@link TypeInformation}
     * is a {@link CompositeType}, it will be flattened in the first level. {@link TypeInformation}
     * that cannot be represented as one of the listed {@link DataTypes} will be treated as a
     * black-box {@link DataTypes#RAW(Class, TypeSerializer)} type. Thus, composite nested fields
     * will not be accessible.
     *
     * <p>Since the DataStream API does not support changelog processing natively, this method
     * assumes append-only/insert-only semantics during the stream-to-table conversion. Records of
     * type {@link Row} must describe {@link RowKind#INSERT} changes.
     *
     * <p>By default, the stream record's timestamp and watermarks are not propagated unless
     * explicitly declared via {@link #fromDataStream(DataStream, Schema)}.
     *
     * @param dataStream The {@link DataStream} to be converted.
     * @param <T> The external type of the {@link DataStream}.
     * @return The converted {@link Table}.
     * @see #fromChangelogStream(DataStream)
     */
    <T> Table fromDataStream(DataStream<T> dataStream);

    /**
     * Converts the given {@link DataStream} into a {@link Table}.
     *
     * <p>Column names and types of the {@link Table} are automatically derived from the {@link
     * TypeInformation} of the {@link DataStream}. If the outermost record's {@link TypeInformation}
     * is a {@link CompositeType}, it will be flattened in the first level. {@link TypeInformation}
     * that cannot be represented as one of the listed {@link DataTypes} will be treated as a
     * black-box {@link DataTypes#RAW(Class, TypeSerializer)} type. Thus, composite nested fields
     * will not be accessible.
     *
     * <p>Since the DataStream API does not support changelog processing natively, this method
     * assumes append-only/insert-only semantics during the stream-to-table conversion. Records of
     * class {@link Row} must describe {@link RowKind#INSERT} changes.
     *
     * <p>By default, the stream record's timestamp and watermarks are not propagated unless
     * explicitly declared.
     *
     * <p>This method allows to declare a {@link Schema} for the resulting table. The declaration is
     * similar to a {@code CREATE TABLE} DDL in SQL and allows to:
     *
     * <ul>
     *   <li>enrich or overwrite automatically derived columns with a custom {@link DataType}
     *   <li>reorder columns
     *   <li>add computed or metadata columns next to the physical columns
     *   <li>access a stream record's timestamp
     *   <li>declare a watermark strategy or propagate the {@link DataStream} watermarks
     * </ul>
     *
     * <p>It is possible to declare a schema without physical/regular columns. In this case, those
     * columns will be automatically derived and implicitly put at the beginning of the schema
     * declaration.
     *
     * <p>The following examples illustrate common schema declarations and their semantics:
     *
     * <pre>
     *     // given a DataStream of Tuple2 < String , BigDecimal >
     *
     *     // === EXAMPLE 1 ===
     *
     *     // no physical columns defined, they will be derived automatically,
     *     // e.g. BigDecimal becomes DECIMAL(38, 18)
     *
     *     Schema.newBuilder()
     *         .columnByExpression("c1", "f1 + 42")
     *         .columnByExpression("c2", "f1 - 1")
     *         .build()
     *
     *     // equal to: CREATE TABLE (f0 STRING, f1 DECIMAL(38, 18), c1 AS f1 + 42, c2 AS f1 - 1)
     *
     *     // === EXAMPLE 2 ===
     *
     *     // physical columns defined, input fields and columns will be mapped by name,
     *     // columns are reordered and their data type overwritten,
     *     // all columns must be defined to show up in the final table's schema
     *
     *     Schema.newBuilder()
     *         .column("f1", "DECIMAL(10, 2)")
     *         .columnByExpression("c", "f1 - 1")
     *         .column("f0", "STRING")
     *         .build()
     *
     *     // equal to: CREATE TABLE (f1 DECIMAL(10, 2), c AS f1 - 1, f0 STRING)
     *
     *     // === EXAMPLE 3 ===
     *
     *     // timestamp and watermarks can be added from the DataStream API,
     *     // physical columns will be derived automatically
     *
     *     Schema.newBuilder()
     *         .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)") // extract timestamp into a column
     *         .watermark("rowtime", "SOURCE_WATERMARK()")  // declare watermarks propagation
     *         .build()
     *
     *     // equal to:
     *     //     CREATE TABLE (
     *     //        f0 STRING,
     *     //        f1 DECIMAL(38, 18),
     *     //        rowtime TIMESTAMP(3) METADATA,
     *     //        WATERMARK FOR rowtime AS SOURCE_WATERMARK()
     *     //     )
     * </pre>
     *
     * @param dataStream The {@link DataStream} to be converted.
     * @param schema The customized schema for the final table.
     * @param <T> The external type of the {@link DataStream}.
     * @return The converted {@link Table}.
     * @see #fromChangelogStream(DataStream, Schema)
     */
    <T> Table fromDataStream(DataStream<T> dataStream, Schema schema);

    /**
     * Converts the given {@link DataStream} of changelog entries into a {@link Table}.
     *
     * <p>Compared to {@link #fromDataStream(DataStream)}, this method consumes instances of {@link
     * Row} and evaluates the {@link RowKind} flag that is contained in every record during runtime.
     * The runtime behavior is similar to that of a {@link DynamicTableSource}.
     *
     * <p>This method expects a changelog containing all kinds of changes (enumerated in {@link
     * RowKind}) as the default {@link ChangelogMode}. Use {@link #fromChangelogStream(DataStream,
     * Schema, ChangelogMode)} to limit the kinds of changes (e.g. for upsert mode).
     *
     * <p>Column names and types of the {@link Table} are automatically derived from the {@link
     * TypeInformation} of the {@link DataStream}. If the outermost record's {@link TypeInformation}
     * is a {@link CompositeType}, it will be flattened in the first level. {@link TypeInformation}
     * that cannot be represented as one of the listed {@link DataTypes} will be treated as a
     * black-box {@link DataTypes#RAW(Class, TypeSerializer)} type. Thus, composite nested fields
     * will not be accessible.
     *
     * <p>By default, the stream record's timestamp and watermarks are not propagated unless
     * explicitly declared via {@link #fromChangelogStream(DataStream, Schema)}.
     *
     * @param dataStream The changelog stream of {@link Row}.
     * @return The converted {@link Table}.
     */
    Table fromChangelogStream(DataStream<Row> dataStream);

    /**
     * Converts the given {@link DataStream} of changelog entries into a {@link Table}.
     *
     * <p>Compared to {@link #fromDataStream(DataStream)}, this method consumes instances of {@link
     * Row} and evaluates the {@link RowKind} flag that is contained in every record during runtime.
     * The runtime behavior is similar to that of a {@link DynamicTableSource}.
     *
     * <p>This method expects a changelog containing all kinds of changes (enumerated in {@link
     * RowKind}) as the default {@link ChangelogMode}. Use {@link #fromChangelogStream(DataStream,
     * Schema, ChangelogMode)} to limit the kinds of changes (e.g. for upsert mode).
     *
     * <p>Column names and types of the {@link Table} are automatically derived from the {@link
     * TypeInformation} of the {@link DataStream}. If the outermost record's {@link TypeInformation}
     * is a {@link CompositeType}, it will be flattened in the first level. {@link TypeInformation}
     * that cannot be represented as one of the listed {@link DataTypes} will be treated as a
     * black-box {@link DataTypes#RAW(Class, TypeSerializer)} type. Thus, composite nested fields
     * will not be accessible.
     *
     * <p>By default, the stream record's timestamp and watermarks are not propagated unless
     * explicitly declared.
     *
     * <p>This method allows to declare a {@link Schema} for the resulting table. The declaration is
     * similar to a {@code CREATE TABLE} DDL in SQL and allows to:
     *
     * <ul>
     *   <li>enrich or overwrite automatically derived columns with a custom {@link DataType}
     *   <li>reorder columns
     *   <li>add computed or metadata columns next to the physical columns
     *   <li>access a stream record's timestamp
     *   <li>declare a watermark strategy or propagate the {@link DataStream} watermarks
     *   <li>declare a primary key
     * </ul>
     *
     * <p>See {@link #fromDataStream(DataStream, Schema)} for more information and examples on how
     * to declare a {@link Schema}.
     *
     * @param dataStream The changelog stream of {@link Row}.
     * @param schema The customized schema for the final table.
     * @return The converted {@link Table}.
     */
    Table fromChangelogStream(DataStream<Row> dataStream, Schema schema);

    /**
     * Converts the given {@link DataStream} of changelog entries into a {@link Table}.
     *
     * <p>Compared to {@link #fromDataStream(DataStream)}, this method consumes instances of {@link
     * Row} and evaluates the {@link RowKind} flag that is contained in every record during runtime.
     * The runtime behavior is similar to that of a {@link DynamicTableSource}.
     *
     * <p>This method requires an explicitly declared {@link ChangelogMode}. For example, use {@link
     * ChangelogMode#upsert()} if the stream will not contain {@link RowKind#UPDATE_BEFORE}, or
     * {@link ChangelogMode#insertOnly()} for non-updating streams.
     *
     * <p>Column names and types of the {@link Table} are automatically derived from the {@link
     * TypeInformation} of the {@link DataStream}. If the outermost record's {@link TypeInformation}
     * is a {@link CompositeType}, it will be flattened in the first level. {@link TypeInformation}
     * that cannot be represented as one of the listed {@link DataTypes} will be treated as a
     * black-box {@link DataTypes#RAW(Class, TypeSerializer)} type. Thus, composite nested fields
     * will not be accessible.
     *
     * <p>By default, the stream record's timestamp and watermarks are not propagated unless
     * explicitly declared.
     *
     * <p>This method allows to declare a {@link Schema} for the resulting table. The declaration is
     * similar to a {@code CREATE TABLE} DDL in SQL and allows to:
     *
     * <ul>
     *   <li>enrich or overwrite automatically derived columns with a custom {@link DataType}
     *   <li>reorder columns
     *   <li>add computed or metadata columns next to the physical columns
     *   <li>access a stream record's timestamp
     *   <li>declare a watermark strategy or propagate the {@link DataStream} watermarks
     *   <li>declare a primary key
     * </ul>
     *
     * <p>See {@link #fromDataStream(DataStream, Schema)} for more information and examples of how
     * to declare a {@link Schema}.
     *
     * @param dataStream The changelog stream of {@link Row}.
     * @param schema The customized schema for the final table.
     * @param changelogMode The expected kinds of changes in the incoming changelog.
     * @return The converted {@link Table}.
     */
    Table fromChangelogStream(
            DataStream<Row> dataStream, Schema schema, ChangelogMode changelogMode);

    /**
     * Creates a view from the given {@link DataStream} in a given path. Registered views can be
     * referenced in SQL queries.
     *
     * <p>See {@link #fromDataStream(DataStream)} for more information on how a {@link DataStream}
     * is translated into a table.
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
     * Creates a view from the given {@link DataStream} in a given path. Registered views can be
     * referenced in SQL queries.
     *
     * <p>See {@link #fromDataStream(DataStream, Schema)} for more information on how a {@link
     * DataStream} is translated into a table.
     *
     * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
     * it will be inaccessible in the current session. To make the permanent object available again
     * you can drop the corresponding temporary object.
     *
     * @param path The path under which the {@link DataStream} is created. See also the {@link
     *     TableEnvironment} class description for the format of the path.
     * @param schema The customized schema for the final table.
     * @param dataStream The {@link DataStream} out of which to create the view.
     * @param <T> The type of the {@link DataStream}.
     */
    <T> void createTemporaryView(String path, DataStream<T> dataStream, Schema schema);

    /**
     * Converts the given {@link Table} into a {@link DataStream}.
     *
     * <p>Since the DataStream API does not support changelog processing natively, this method
     * assumes append-only/insert-only semantics during the table-to-stream conversion. The records
     * of class {@link Row} will always describe {@link RowKind#INSERT} changes. Updating tables are
     * not supported by this method and will produce an exception.
     *
     * <p>If you want to convert the {@link Table} to a specific class or data type, use {@link
     * #toDataStream(Table, Class)} or {@link #toDataStream(Table, AbstractDataType)} instead.
     *
     * <p>Note that the type system of the table ecosystem is richer than the one of the DataStream
     * API. The table runtime will make sure to properly serialize the output records to the first
     * operator of the DataStream API. Afterwards, the {@link Types} semantics of the DataStream API
     * need to be considered.
     *
     * <p>If the input table contains a single rowtime column, it will be propagated into a stream
     * record's timestamp. Watermarks will be propagated as well.
     *
     * @param table The {@link Table} to convert. It must be insert-only.
     * @return The converted {@link DataStream}.
     * @see #toDataStream(Table, AbstractDataType)
     * @see #toChangelogStream(Table)
     */
    DataStream<Row> toDataStream(Table table);

    /**
     * Converts the given {@link Table} into a {@link DataStream} of the given {@link Class}.
     *
     * <p>See {@link #toDataStream(Table, AbstractDataType)} for more information on how a {@link
     * Table} is translated into a {@link DataStream}.
     *
     * <p>This method is a shortcut for:
     *
     * <pre>
     *     tableEnv.toDataStream(table, DataTypes.of(targetClass))
     * </pre>
     *
     * <p>Calling this method with a class of {@link Row} will redirect to {@link
     * #toDataStream(Table)}.
     *
     * @param table The {@link Table} to convert. It must be insert-only.
     * @param targetClass The {@link Class} that decides about the final external representation in
     *     {@link DataStream} records.
     * @param <T> External record.
     * @return The converted {@link DataStream}.
     * @see #toChangelogStream(Table, Schema)
     */
    <T> DataStream<T> toDataStream(Table table, Class<T> targetClass);

    /**
     * Converts the given {@link Table} into a {@link DataStream} of the given {@link DataType}.
     *
     * <p>The given {@link DataType} is used to configure the table runtime to convert columns and
     * internal data structures to the desired representation. The following example shows how to
     * convert the table columns into the fields of a POJO type.
     *
     * <pre>
     *     // given a Table of (name STRING, age INT)
     *
     *     public static class MyPojo {
     *         public String name;
     *         public Integer age;
     *
     *         // default constructor for DataStream API
     *         public MyPojo() {}
     *
     *         // fully assigning constructor for field order in Table API
     *         public MyPojo(String name, Integer age) {
     *             this.name = name;
     *             this.age = age;
     *         }
     *     }
     *
     *     tableEnv.toDataStream(table, DataTypes.of(MyPojo.class));
     * </pre>
     *
     * <p>Since the DataStream API does not support changelog processing natively, this method
     * assumes append-only/insert-only semantics during the table-to-stream conversion. Updating
     * tables are not supported by this method and will produce an exception.
     *
     * <p>Note that the type system of the table ecosystem is richer than the one of the DataStream
     * API. The table runtime will make sure to properly serialize the output records to the first
     * operator of the DataStream API. Afterwards, the {@link Types} semantics of the DataStream API
     * need to be considered.
     *
     * <p>If the input table contains a single rowtime column, it will be propagated into a stream
     * record's timestamp. Watermarks will be propagated as well.
     *
     * @param table The {@link Table} to convert. It must be insert-only.
     * @param targetDataType The {@link DataType} that decides about the final external
     *     representation in {@link DataStream} records.
     * @param <T> External record.
     * @return The converted {@link DataStream}.
     * @see #toDataStream(Table)
     * @see #toChangelogStream(Table, Schema)
     */
    <T> DataStream<T> toDataStream(Table table, AbstractDataType<?> targetDataType);

    /**
     * Converts the given {@link Table} into a {@link DataStream} of changelog entries.
     *
     * <p>Compared to {@link #toDataStream(Table)}, this method produces instances of {@link Row}
     * and sets the {@link RowKind} flag that is contained in every record during runtime. The
     * runtime behavior is similar to that of a {@link DynamicTableSink}.
     *
     * <p>This method can emit a changelog containing all kinds of changes (enumerated in {@link
     * RowKind}) that the given updating table requires as the default {@link ChangelogMode}. Use
     * {@link #toChangelogStream(Table, Schema, ChangelogMode)} to limit the kinds of changes (e.g.
     * for upsert mode).
     *
     * <p>Note that the type system of the table ecosystem is richer than the one of the DataStream
     * API. The table runtime will make sure to properly serialize the output records to the first
     * operator of the DataStream API. Afterwards, the {@link Types} semantics of the DataStream API
     * need to be considered.
     *
     * <p>If the input table contains a single rowtime column, it will be propagated into a stream
     * record's timestamp. Watermarks will be propagated as well.
     *
     * @param table The {@link Table} to convert. It can be updating or insert-only.
     * @return The converted changelog stream of {@link Row}.
     */
    DataStream<Row> toChangelogStream(Table table);

    /**
     * Converts the given {@link Table} into a {@link DataStream} of changelog entries.
     *
     * <p>Compared to {@link #toDataStream(Table)}, this method produces instances of {@link Row}
     * and sets the {@link RowKind} flag that is contained in every record during runtime. The
     * runtime behavior is similar to that of a {@link DynamicTableSink}.
     *
     * <p>This method can emit a changelog containing all kinds of changes (enumerated in {@link
     * RowKind}) that the given updating table requires as the default {@link ChangelogMode}. Use
     * {@link #toChangelogStream(Table, Schema, ChangelogMode)} to limit the kinds of changes (e.g.
     * for upsert mode).
     *
     * <p>The given {@link Schema} is used to configure the table runtime to convert columns and
     * internal data structures to the desired representation. The following example shows how to
     * convert a table column into a POJO type.
     *
     * <pre>
     *     // given a Table of (id BIGINT, payload ROW < name STRING , age INT >)
     *
     *     public static class MyPojo {
     *         public String name;
     *         public Integer age;
     *
     *         // default constructor for DataStream API
     *         public MyPojo() {}
     *
     *         // fully assigning constructor for field order in Table API
     *         public MyPojo(String name, Integer age) {
     *             this.name = name;
     *             this.age = age;
     *         }
     *     }
     *
     *     tableEnv.toChangelogStream(
     *         table,
     *         Schema.newBuilder()
     *             .column("id", DataTypes.BIGINT())
     *             .column("payload", DataTypes.of(MyPojo.class)) // force an implicit conversion
     *             .build());
     * </pre>
     *
     * <p>Note that the type system of the table ecosystem is richer than the one of the DataStream
     * API. The table runtime will make sure to properly serialize the output records to the first
     * operator of the DataStream API. Afterwards, the {@link Types} semantics of the DataStream API
     * need to be considered.
     *
     * <p>If the input table contains a single rowtime column, it will be propagated into a stream
     * record's timestamp. Watermarks will be propagated as well.
     *
     * <p>If the rowtime should not be a concrete field in the final {@link Row} anymore, or the
     * schema should be symmetrical for both {@link #fromChangelogStream} and {@link
     * #toChangelogStream}, the rowtime can also be declared as a metadata column that will be
     * propagated into a stream record's timestamp. It is possible to declare a schema without
     * physical/regular columns. In this case, those columns will be automatically derived and
     * implicitly put at the beginning of the schema declaration.
     *
     * <p>The following examples illustrate common schema declarations and their semantics:
     *
     * <pre>
     *     // given a Table of (id INT, name STRING, my_rowtime TIMESTAMP_LTZ(3))
     *
     *     // === EXAMPLE 1 ===
     *
     *     // no physical columns defined, they will be derived automatically,
     *     // the last derived physical column will be skipped in favor of the metadata column
     *
     *     Schema.newBuilder()
     *         .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
     *         .build()
     *
     *     // equal to: CREATE TABLE (id INT, name STRING, rowtime TIMESTAMP_LTZ(3) METADATA)
     *
     *     // === EXAMPLE 2 ===
     *
     *     // physical columns defined, all columns must be defined
     *
     *     Schema.newBuilder()
     *         .column("id", "INT")
     *         .column("name", "STRING")
     *         .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
     *         .build()
     *
     *     // equal to: CREATE TABLE (id INT, name STRING, rowtime TIMESTAMP_LTZ(3) METADATA)
     * </pre>
     *
     * @param table The {@link Table} to convert. It can be updating or insert-only.
     * @param targetSchema The {@link Schema} that decides about the final external representation
     *     in {@link DataStream} records.
     * @return The converted changelog stream of {@link Row}.
     */
    DataStream<Row> toChangelogStream(Table table, Schema targetSchema);

    /**
     * Converts the given {@link Table} into a {@link DataStream} of changelog entries.
     *
     * <p>Compared to {@link #toDataStream(Table)}, this method produces instances of {@link Row}
     * and sets the {@link RowKind} flag that is contained in every record during runtime. The
     * runtime behavior is similar to that of a {@link DynamicTableSink}.
     *
     * <p>This method requires an explicitly declared {@link ChangelogMode}. For example, use {@link
     * ChangelogMode#upsert()} if the stream will not contain {@link RowKind#UPDATE_BEFORE}, or
     * {@link ChangelogMode#insertOnly()} for non-updating streams.
     *
     * <p>Note that the type system of the table ecosystem is richer than the one of the DataStream
     * API. The table runtime will make sure to properly serialize the output records to the first
     * operator of the DataStream API. Afterwards, the {@link Types} semantics of the DataStream API
     * need to be considered.
     *
     * <p>If the input table contains a single rowtime column, it will be propagated into a stream
     * record's timestamp. Watermarks will be propagated as well. However, it is also possible to
     * write out the rowtime as a metadata column. See {@link #toChangelogStream(Table, Schema)} for
     * more information and examples on how to declare a {@link Schema}.
     *
     * @param table The {@link Table} to convert. It can be updating or insert-only.
     * @param targetSchema The {@link Schema} that decides about the final external representation
     *     in {@link DataStream} records.
     * @param changelogMode The required kinds of changes in the result changelog. An exception will
     *     be thrown if the given updating table cannot be represented in this changelog mode.
     * @return The converted changelog stream of {@link Row}.
     */
    DataStream<Row> toChangelogStream(
            Table table, Schema targetSchema, ChangelogMode changelogMode);

    /**
     * Returns a {@link StatementSet} that integrates with the Java-specific {@link DataStream} API.
     *
     * <p>It accepts pipelines defined by DML statements or {@link Table} objects. The planner can
     * optimize all added statements together and then either submit them as one job or attach them
     * to the underlying {@link StreamExecutionEnvironment}.
     *
     * @return statement set builder for the Java-specific {@link DataStream} API
     */
    StreamStatementSet createStatementSet();

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
     * @deprecated Use {@link #fromDataStream(DataStream, Schema)} instead. In most cases, {@link
     *     #fromDataStream(DataStream)} should already be sufficient. It integrates with the new
     *     type system and supports all kinds of {@link DataTypes} that the table runtime can
     *     consume. The semantics might be slightly different for raw and structured types.
     */
    @Deprecated
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
     * @deprecated Use {@link #createTemporaryView(String, DataStream, Schema)} instead. In most
     *     cases, {@link #createTemporaryView(String, DataStream)} should already be sufficient. It
     *     integrates with the new type system and supports all kinds of {@link DataTypes} that the
     *     table runtime can consume. The semantics might be slightly different for raw and
     *     structured types.
     */
    @Deprecated
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
     *   <li>{@link Row} and {@link org.apache.flink.api.java.tuple.Tuple} types: Fields are mapped
     *       by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param clazz The class of the type of the resulting {@link DataStream}.
     * @param <T> The type of the resulting {@link DataStream}.
     * @return The converted {@link DataStream}.
     * @deprecated Use {@link #toDataStream(Table, Class)} instead. It integrates with the new type
     *     system and supports all kinds of {@link DataTypes} that the table runtime can produce.
     *     The semantics might be slightly different for raw and structured types. Use {@code
     *     toDataStream(DataTypes.of(TypeInformation.of(Class)))} if {@link TypeInformation} should
     *     be used as source of truth.
     */
    @Deprecated
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
     *   <li>{@link Row} and {@link org.apache.flink.api.java.tuple.Tuple} types: Fields are mapped
     *       by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param typeInfo The {@link TypeInformation} that specifies the type of the {@link
     *     DataStream}.
     * @param <T> The type of the resulting {@link DataStream}.
     * @return The converted {@link DataStream}.
     * @deprecated Use {@link #toDataStream(Table, Class)} instead. It integrates with the new type
     *     system and supports all kinds of {@link DataTypes} that the table runtime can produce.
     *     The semantics might be slightly different for raw and structured types. Use {@code
     *     toDataStream(DataTypes.of(TypeInformation.of(Class)))} if {@link TypeInformation} should
     *     be used as source of truth.
     */
    @Deprecated
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
     *   <li>{@link Row} and {@link org.apache.flink.api.java.tuple.Tuple} types: Fields are mapped
     *       by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param clazz The class of the requested record type.
     * @param <T> The type of the requested record type.
     * @return The converted {@link DataStream}.
     * @deprecated Use {@link #toChangelogStream(Table, Schema)} instead. It integrates with the new
     *     type system and supports all kinds of {@link DataTypes} and every {@link ChangelogMode}
     *     that the table runtime can produce.
     */
    @Deprecated
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
     *   <li>{@link Row} and {@link org.apache.flink.api.java.tuple.Tuple} types: Fields are mapped
     *       by position, field types must match.
     *   <li>POJO {@link DataStream} types: Fields are mapped by field name, field types must match.
     * </ul>
     *
     * @param table The {@link Table} to convert.
     * @param typeInfo The {@link TypeInformation} of the requested record type.
     * @param <T> The type of the requested record type.
     * @return The converted {@link DataStream}.
     * @deprecated Use {@link #toChangelogStream(Table, Schema)} instead. It integrates with the new
     *     type system and supports all kinds of {@link DataTypes} and every {@link ChangelogMode}
     *     that the table runtime can produce.
     */
    @Deprecated
    <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo);
}
