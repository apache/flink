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
package org.apache.flink.table.api.bridge.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.{TableEnvironment, _}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction, TableFunction}
import org.apache.flink.table.types.{AbstractDataType, DataType}
import org.apache.flink.types.{Row, RowKind}

/**
  * This table environment is the entry point and central context for creating Table and SQL
  * API programs that integrate with the Scala-specific [[DataStream]] API.
  *
  * It is unified for bounded and unbounded data processing.
  *
  * A stream table environment is responsible for:
  *
  * - Convert a [[DataStream]] into [[Table]] and vice-versa.
  * - Connecting to external systems.
  * - Registering and retrieving [[Table]]s and other meta objects from a catalog.
  * - Executing SQL statements.
  * - Offering further configuration options.
  *
  * Note: If you don't intend to use the [[DataStream]] API, [[TableEnvironment]] is meant for pure
  * table programs.
  */
@PublicEvolving
trait StreamTableEnvironment extends TableEnvironment {

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register
    *
    * @deprecated Use [[createTemporarySystemFunction(String, UserDefinedFunction)]] instead. Please
    *             note that the new method also uses the new type system and reflective extraction
    *             logic. It might be necessary to update the function implementation as well. See
    *             the documentation of [[TableFunction]] for more information on the new function
    *             design.
    */
  @deprecated
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    *
    * @deprecated Use [[createTemporarySystemFunction(String, UserDefinedFunction)]] instead. Please
    *             note that the new method also uses the new type system and reflective extraction
    *             logic. It might be necessary to update the function implementation as well. See
    *             the documentation of [[AggregateFunction]] for more information on the new
    *             function design.
    */
  @deprecated
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
    name: String,
    f: AggregateFunction[T, ACC]): Unit

  /**
    * Registers an [[TableAggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can only be referenced in Table API.
    *
    * @param name The name under which the function is registered.
    * @param f The TableAggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    *
    * @deprecated Use [[createTemporarySystemFunction(String, UserDefinedFunction)]] instead. Please
    *             note that the new method also uses the new type system and reflective extraction
    *             logic. It might be necessary to update the function implementation as well. See
    *             the documentation of [[TableAggregateFunction]] for more information on the new
    *             function design.
    */
  @deprecated
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
    name: String,
    f: TableAggregateFunction[T, ACC]): Unit

  /**
   * Converts the given [[DataStream]] into a [[Table]].
   *
   * Column names and types of the [[Table]] are automatically derived from the [[TypeInformation]]
   * of the [[DataStream]]. If the outermost record's [[TypeInformation]] is a [[CompositeType]],
   * it will be flattened in the first level. [[TypeInformation]] that cannot be represented as one
   * of the listed [[DataTypes]] will be treated as a black-box
   * [[DataTypes.RAW(Class, TypeSerializer)]] type. Thus, composite nested fields will not be
   * accessible.
   *
   * Since the DataStream API does not support changelog processing natively, this method
   * assumes append-only/insert-only semantics during the stream-to-table conversion. Records of
   * type [[Row]] must describe [[RowKind.INSERT]] changes.
   *
   * By default, the stream record's timestamp and watermarks are not propagated unless
   * explicitly declared via [[fromDataStream(DataStream, Schema)]].
   *
   * @param dataStream The [[DataStream]] to be converted.
   * @tparam T The external type of the [[DataStream]].
   * @return The converted [[Table]].
   */
  def fromDataStream[T](dataStream: DataStream[T]): Table

  /**
   * Converts the given [[DataStream]] into a [[Table]].
   *
   * Column names and types of the [[Table]] are automatically derived from the [[TypeInformation]]
   * of the [[DataStream]]. If the outermost record's [[TypeInformation]] is a [[CompositeType]],
   * it will be flattened in the first level. [[TypeInformation]] that cannot be represented as one
   * of the listed [[DataTypes]] will be treated as a black-box
   * [[DataTypes.RAW(Class, TypeSerializer)]] type. Thus, composite nested fields will not be
   * accessible.
   *
   * Since the DataStream API does not support changelog processing natively, this method
   * assumes append-only/insert-only semantics during the stream-to-table conversion. Records of
   * type [[Row]] must describe [[RowKind.INSERT]] changes.
   *
   * By default, the stream record's timestamp and watermarks are not propagated unless
   * explicitly declared.
   *
   * This method allows to declare a [[Schema]] for the resulting table. The declaration is
   * similar to a `CREATE TABLE` DDL in SQL and allows to:
   *
   * - enrich or overwrite automatically derived columns with a custom [[DataType]]
   * - reorder columns
   * - add computed or metadata columns next to the physical columns
   * - access a stream record's timestamp
   * - declare a watermark strategy or propagate the [[DataStream]] watermarks
   *
   * It is possible to declare a schema without physical/regular columns. In this case, those
   * columns will be automatically derived and implicitly put at the beginning of the schema
   * declaration.
   *
   * The following examples illustrate common schema declarations and their semantics:
   *
   * {{{
   *     // given a DataStream of a case class with (f0: String, f1: BigDecimal)
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
   * }}}
   *
   * @param dataStream The [[DataStream]] to be converted.
   * @param schema The customized schema for the final table.
   * @tparam T The external type of the [[DataStream]].
   * @return The converted [[Table]].
   */
  def fromDataStream[T](dataStream: DataStream[T], schema: Schema): Table

  /**
   * Converts the given [[DataStream]] of changelog entries into a [[Table]].
   *
   * Compared to [[fromDataStream(DataStream)]], this method consumes instances of [[Row]] and
   * evaluates the [[RowKind]] flag that is contained in every record during runtime.
   * The runtime behavior is similar to that of a [[DynamicTableSource]].
   *
   * This method expects a changelog containing all kinds of changes (enumerated in [[RowKind]])
   * as the default [[ChangelogMode]]. Use [[fromChangelogStream(DataStream, Schema,
   * ChangelogMode)]] to limit the kinds of changes (e.g. for upsert mode).
   *
   * Column names and types of the [[Table]] are automatically derived from the [[TypeInformation]]
   * of the [[DataStream]]. If the outermost record's [[TypeInformation]] is a [[CompositeType]],
   * it will be flattened in the first level. [[TypeInformation]] that cannot be represented as one
   * of the listed [[DataTypes]] will be treated as a black-box
   * [[DataTypes.RAW(Class, TypeSerializer)]] type. Thus, composite nested fields will not be
   * accessible.
   *
   * By default, the stream record's timestamp and watermarks are not propagated unless
   * explicitly declared via [[fromChangelogStream(DataStream, Schema)]].
   *
   * @param dataStream The changelog stream of [[Row]].
   * @return The converted [[Table]].
   */
  def fromChangelogStream(dataStream: DataStream[Row]): Table

  /**
   * Converts the given [[DataStream]] of changelog entries into a [[Table]].
   *
   * Compared to [[fromDataStream(DataStream)]], this method consumes instances of [[Row]] and
   * evaluates the [[RowKind]] flag that is contained in every record during runtime.
   * The runtime behavior is similar to that of a [[DynamicTableSource]].
   *
   * This method expects a changelog containing all kinds of changes (enumerated in [[RowKind]]) as
   * the default [[ChangelogMode]]. Use [[fromChangelogStream(DataStream, Schema, ChangelogMode)]]
   * to limit the kinds of changes (e.g. for upsert mode).
   *
   * Column names and types of the [[Table]] are automatically derived from the [[TypeInformation]]
   * of the [[DataStream]]. If the outermost record's [[TypeInformation]] is a [[CompositeType]],
   * it will be flattened in the first level. [[TypeInformation]] that cannot be represented as one
   * of the listed [[DataTypes]] will be treated as a black-box
   * [[DataTypes.RAW(Class, TypeSerializer)]] type. Thus, composite nested fields will not be
   * accessible.
   *
   * By default, the stream record's timestamp and watermarks are not propagated unless explicitly
   * declared.
   *
   * This method allows to declare a [[Schema]] for the resulting table. The declaration is
   * similar to a `CREATE TABLE` DDL in SQL and allows to:
   *
   * - enrich or overwrite automatically derived columns with a custom [[DataType]]
   * - reorder columns
   * - add computed or metadata columns next to the physical columns
   * - access a stream record's timestamp
   * - declare a watermark strategy or propagate the [[DataStream]] watermarks
   * - declare a primary key
   *
   * See [[fromDataStream(DataStream, Schema)]] for more information and examples on how
   * to declare a [[Schema]].
   *
   * @param dataStream The changelog stream of [[Row]].
   * @param schema The customized schema for the final table.
   * @return The converted [[Table]].
   */
  def fromChangelogStream(dataStream: DataStream[Row], schema: Schema): Table

  /**
   * Converts the given [[DataStream]] of changelog entries into a [[Table]].
   *
   * Compared to [[fromDataStream(DataStream)]], this method consumes instances of [[Row]] and
   * evaluates the [[RowKind]] flag that is contained in every record during runtime.
   * The runtime behavior is similar to that of a [[DynamicTableSource]].
   *
   * This method requires an explicitly declared [[ChangelogMode]]. For example, use
   * [[ChangelogMode.upsert()]] if the stream will not contain [[RowKind.UPDATE_BEFORE]], or
   * [[ChangelogMode.insertOnly()]] for non-updating streams.
   *
   * Column names and types of the [[Table]] are automatically derived from the [[TypeInformation]]
   * of the [[DataStream]]. If the outermost record's [[TypeInformation]] is a [[CompositeType]],
   * it will be flattened in the first level. [[TypeInformation]] that cannot be represented as one
   * of the listed [[DataTypes]] will be treated as a black-box
   * [[DataTypes.RAW(Class, TypeSerializer)]] type. Thus, composite nested fields will not be
   * accessible.
   *
   * By default, the stream record's timestamp and watermarks are not propagated unless
   * explicitly declared.
   *
   * This method allows to declare a [[Schema]] for the resulting table. The declaration is
   * similar to a `CREATE TABLE` DDL in SQL and allows to:
   *
   * - enrich or overwrite automatically derived columns with a custom [[DataType]]
   * - reorder columns
   * - add computed or metadata columns next to the physical columns
   * - access a stream record's timestamp
   * - declare a watermark strategy or propagate the [[DataStream]] watermarks
   * - declare a primary key
   *
   * See [[fromDataStream(DataStream, Schema)]] for more information and examples on how
   * to declare a [[Schema]].
   *
   * @param dataStream The changelog stream of [[Row]].
   * @param schema The customized schema for the final table.
   * @param changelogMode The expected kinds of changes in the incoming changelog.
   * @return The converted [[Table]].
   */
  def fromChangelogStream(
      dataStream: DataStream[Row], schema: Schema,  changelogMode: ChangelogMode): Table

  /**
   * Creates a view from the given [[DataStream]] in a given path.
   * Registered tables can be referenced in SQL queries.
   *
   * See [[fromDataStream(DataStream)]] for more information on how a [[DataStream]] is translated
   * into a table.
   *
   * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
   * it will be inaccessible in the current session. To make the permanent object available again
   * you can drop the corresponding temporary object.
   *
   * @param path The path under which the [[DataStream]] is created.
   *             See also the [[TableEnvironment]] class description for the format of the path.
   * @param dataStream The [[DataStream]] out of which to create the view.
   * @tparam T The type of the [[DataStream]].
   */
  def createTemporaryView[T](path: String, dataStream: DataStream[T]): Unit

  /**
   * Creates a view from the given [[DataStream]] in a given path.
   * Registered tables can be referenced in SQL queries.
   *
   * See [[fromDataStream(DataStream, Schema)]] for more information on how a [[DataStream]] is
   * translated into a table.
   *
   * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
   * it will be inaccessible in the current session. To make the permanent object available again
   * you can drop the corresponding temporary object.
   *
   * @param path The path under which the [[DataStream]] is created.
   *             See also the [[TableEnvironment]] class description for the format of the path.
   * @param schema The customized schema for the final table.
   * @param dataStream The [[DataStream]] out of which to create the view.
   * @tparam T The type of the [[DataStream]].
   */
  def createTemporaryView[T](path: String, dataStream: DataStream[T], schema: Schema): Unit

  /**
   * Converts the given [[Table]] into a [[DataStream]].
   *
   * Since the DataStream API does not support changelog processing natively, this method
   * assumes append-only/insert-only semantics during the table-to-stream conversion. The records
   * of class [[Row]] will always describe [[RowKind.INSERT]] changes. Updating tables are
   * not supported by this method and will produce an exception.
   *
   * If you want to convert the [[Table]] to a specific class or data type, use
   * [[toDataStream(Table, Class)]] or [[toDataStream(Table, AbstractDataType)]] instead.
   *
   * Note that the type system of the table ecosystem is richer than the one of the DataStream
   * API. The table runtime will make sure to properly serialize the output records to the first
   * operator of the DataStream API. Afterwards, the [[org.apache.flink.api.common.typeinfo.Types]]
   * semantics of the DataStream API need to be considered.
   *
   * If the input table contains a single rowtime column, it will be propagated into a stream
   * record's timestamp. Watermarks will be propagated as well.
   *
   * @param table The [[Table]] to convert. It must be insert-only.
   * @return The converted [[DataStream]].
   * @see [[toDataStream(Table, AbstractDataType)]]
   */
  def toDataStream(table: Table): DataStream[Row]

  /**
   * Converts the given [[Table]] into a [[DataStream]] of the given [[Class]].
   *
   * See [[toDataStream(Table, AbstractDataType)]] for more information on how a
   * Table} is translated into a DataStream}.
   *
   * This method is a shortcut for:
   *
   * {{{
   *     tableEnv.toDataStream(table, DataTypes.of(targetClass))
   * }}}
   *
   * Calling this method with a class of [[Row]] will redirect to [[toDataStream(Table)]].
   *
   * @param table The [[Table]] to convert. It must be insert-only.
   * @param targetClass The [[Class]] that decides about the final external representation in
   *                    [[DataStream]] records.
   * @tparam T External record.
   * @return The converted [[DataStream]].
   */
  def toDataStream[T](table: Table, targetClass: Class[T]): DataStream[T]

  /**
   * Converts the given [[Table]] into a [[DataStream]] of the given [[DataType]].
   *
   * The given [[DataType]] is used to configure the table runtime to convert columns and
   * internal data structures to the desired representation. The following example shows how to
   * convert the table columns into the fields of a POJO type.
   *
   * {{{
   *     // given a Table of (name STRING, age INT)
   *
   *     case class MyPojo(var name: String, var age: java.lang.Integer)
   *
   *     tableEnv.toDataStream(table, DataTypes.of(MyPojo.class))
   * }}}
   *
   * Since the DataStream API does not support changelog processing natively, this method
   * assumes append-only/insert-only semantics during the table-to-stream conversion. Updating
   * tables are not supported by this method and will produce an exception.
   *
   * Note that the type system of the table ecosystem is richer than the one of the DataStream
   * API. The table runtime will make sure to properly serialize the output records to the first
   * operator of the DataStream API. Afterwards, the [[org.apache.flink.api.common.typeinfo.Types]]
   * semantics of the DataStream API need to be considered.
   *
   * If the input table contains a single rowtime column, it will be propagated into a stream
   * record's timestamp. Watermarks will be propagated as well.
   *
   * @param table The [[Table]] to convert. It must be insert-only.
   * @param targetDataType The [[DataType]] that decides about the final external
   *                       representation in [[DataStream]] records.
   * @tparam T External record.
   * @return The converted [[DataStream]].
   * @see [[toDataStream(Table)]]
   */
  def toDataStream[T](table: Table, targetDataType: AbstractDataType[_]): DataStream[T]

  /**
   * Converts the given [[Table]] into a [[DataStream]] of changelog entries.
   *
   * Compared to [[toDataStream(Table)]], this method produces instances of [[Row]]
   * and sets the [[RowKind]] flag that is contained in every record during runtime. The
   * runtime behavior is similar to that of a [[DynamicTableSink]].
   *
   * This method can emit a changelog containing all kinds of changes (enumerated in [[RowKind]])
   * that the given updating table requires as the default [[ChangelogMode]]. Use
   * [[toChangelogStream(Table, Schema, ChangelogMode)]] to limit the kinds of changes (e.g.
   * for upsert mode).
   *
   * Note that the type system of the table ecosystem is richer than the one of the DataStream
   * API. The table runtime will make sure to properly serialize the output records to the first
   * operator of the DataStream API. Afterwards, the [[org.apache.flink.api.common.typeinfo.Types]]
   * semantics of the DataStream API need to be considered.
   *
   * If the input table contains a single rowtime column, it will be propagated into a stream
   * record's timestamp. Watermarks will be propagated as well.
   *
   * @param table The [[Table]] to convert. It can be updating or insert-only.
   * @return The converted changelog stream of [[Row]].
   */
  def toChangelogStream(table: Table): DataStream[Row]

  /**
   * Converts the given [[Table]] into a [[DataStream]] of changelog entries.
   *
   * Compared to [[toDataStream(Table)]], this method produces instances of [[Row]]
   * and sets the [[RowKind]] flag that is contained in every record during runtime. The
   * runtime behavior is similar to that of a [[DynamicTableSink]].
   *
   * This method can emit a changelog containing all kinds of changes (enumerated in [[RowKind]])
   * that the given updating table requires as the default [[ChangelogMode]]. Use
   * [[toChangelogStream(Table, Schema, ChangelogMode)]] to limit the kinds of changes (e.g.
   * for upsert mode).
   *
   * The given [[Schema]] is used to configure the table runtime to convert columns and
   * internal data structures to the desired representation. The following example shows how to
   * convert a table column into a POJO type.
   *
   * {{{
   *     // given a Table of (id BIGINT, payload ROW < name STRING , age INT >)
   *
   *     case class MyPojo(var name: String, var age: java.lang.Integer)
   *
   *     tableEnv.toChangelogStream(
   *         table,
   *         Schema.newBuilder()
   *             .column("id", DataTypes.BIGINT())
   *             .column("payload", DataTypes.of(classOf[MyPojo])) // force an implicit conversion
   *             .build())
   * }}}
   *
   * Note that the type system of the table ecosystem is richer than the one of the DataStream
   * API. The table runtime will make sure to properly serialize the output records to the first
   * operator of the DataStream API. Afterwards, the [[org.apache.flink.api.common.typeinfo.Types]]
   * semantics of the DataStream API need to be considered.
   *
   * If the input table contains a single rowtime column, it will be propagated into a stream
   * record's timestamp. Watermarks will be propagated as well.
   *
   * If the rowtime should not be a concrete field in the final [[Row]] anymore, or the schema
   * should be symmetrical for both [[fromChangelogStream]] and [[toChangelogStream]], the rowtime
   * can also be declared as a metadata column that will be propagated into a stream record's
   * timestamp. It is possible to declare a schema without physical/regular columns. In this case,
   * those columns will be automatically derived and implicitly put at the beginning of the schema
   * declaration.
   *
   * The following examples illustrate common schema declarations and their semantics:
   *
   * {{{
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
   * }}}
   *
   * @param table The [[Table]] to convert. It can be updating or insert-only.
   * @param targetSchema The [[Schema]] that decides about the final external representation
   *                     in [[DataStream]] records.
   * @return The converted changelog stream of [[Row]].
   */
  def toChangelogStream(table: Table, targetSchema: Schema): DataStream[Row]

  /**
   * Converts the given [[Table]] into a [[DataStream]] of changelog entries.
   *
   * Compared to [[toDataStream(Table)]], this method produces instances of [[Row]]
   * and sets the [[RowKind]] flag that is contained in every record during runtime. The
   * runtime behavior is similar to that of a [[DynamicTableSink]].
   *
   * This method requires an explicitly declared [[ChangelogMode]]. For example, use
   * [[ChangelogMode.upsert()]] if the stream will not contain [[RowKind.UPDATE_BEFORE]], or
   * [[ChangelogMode.insertOnly()]] for non-updating streams.
   *
   * Note that the type system of the table ecosystem is richer than the one of the DataStream
   * API. The table runtime will make sure to properly serialize the output records to the first
   * operator of the DataStream API. Afterwards, the [[org.apache.flink.api.common.typeinfo.Types]]
   * semantics of the DataStream API need to be considered.
   *
   * If the input table contains a single rowtime column, it will be propagated into a stream
   * record's timestamp. Watermarks will be propagated as well. However, it is also possible to
   * write out the rowtime as a metadata column. See [[toChangelogStream(Table, Schema)]] for
   * more information and examples on how to declare a [[Schema]].
   *
   * @param table The [[Table]] to convert. It can be updating or insert-only.
   * @param targetSchema The [[Schema]] that decides about the final external representation
   *                     in [[DataStream]] records.
   * @param changelogMode The required kinds of changes in the result changelog. An exception will
   *                      be thrown if the given updating table cannot be represented in this
   *                      changelog mode.
   * @return The converted changelog stream of [[Row]].
   */
   def toChangelogStream(
        table: Table, targetSchema: Schema, changelogMode: ChangelogMode): DataStream[Row]

  /**
   * Returns a [[StatementSet]] that integrates with the Scala-specific [[DataStream]] API.
   *
   * It accepts pipelines defined by DML statements or [[Table]] objects. The planner can
   * optimize all added statements together and then either submit them as one job or attach them
   * to the underlying [[StreamExecutionEnvironment]].
   *
   * @return statement set builder for the Scala-specific [[DataStream]] API
   */
  def createStatementSet(): StreamStatementSet

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * There are two modes for mapping original fields to the fields of the [[Table]]:
    *
    * 1. Reference input fields by name:
    * All fields in the schema definition are referenced by name
    * (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
    * attributes at arbitrary positions using arbitrary names (except those that exist in the
    * result schema). In this mode, fields can be reordered and projected out. This mode can
    * be used for any input type, including POJOs.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val table: Table = tableEnv.fromDataStream(
    *      stream,
    *      $"_2", // reorder and use the original field
    *      $"rowtime".rowtime, // extract the internally attached timestamp into an event-time
    *                          // attribute named 'rowtime'
    *      $"_1" as "name" // reorder and give the original field a better name
    *   )
    * }}}
    *
    * 2. Reference input fields by position:
    * In this mode, fields are simply renamed. Event-time attributes can
    * replace the field on their position in the input data (if it is of correct type) or be
    * appended at the end. Proctime attributes must be appended at the end. This mode can only be
    * used if the input type has a defined field order (tuple, case class, Row) and none of
    * the `fields` references a field of the input type.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val table: Table = tableEnv.fromDataStream(
    *      stream,
    *      $"a", // rename the first field to 'a'
    *      $"b" // rename the second field to 'b'
    *      $"rowtime".rowtime // extract the internally attached timestamp
    *                         // into an event-time attribute named 'rowtime'
    *   )
    * }}}
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @param fields The fields expressions to map original fields of the DataStream to the fields of
    *               the [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    * @deprecated Use [[fromDataStream(DataStream, Schema)]] instead. In most cases,
    *             [[fromDataStream(DataStream)]] should already be sufficient. It integrates with
    *             the new type system and supports all kinds of [[DataTypes]] that the table runtime
    *             can consume. The semantics might be slightly different for raw and structured
    *             types.
    */
  @deprecated
  def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table

  /**
    * Creates a view from the given [[DataStream]].
    * Registered views can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * The view is registered in the namespace of the current catalog and database. To register the
    * view in a different catalog use [[createTemporaryView]].
    *
    * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
    * it will be inaccessible in the current session. To make the permanent object available again
    * you can drop the corresponding temporary object.
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    * @deprecated use [[createTemporaryView]]
    */
  @deprecated
  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit

  /**
    * Creates a view from the given [[DataStream]] in a given path with specified field names.
    * Registered views can be referenced in SQL queries.
    *
    * There are two modes for mapping original fields to the fields of the View:
    *
    * 1. Reference input fields by name:
    * All fields in the schema definition are referenced by name
    * (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
    * attributes at arbitrary positions using arbitrary names (except those that exist in the
    * result schema). In this mode, fields can be reordered and projected out. This mode can
    * be used for any input type, including POJOs.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   tableEnv.registerDataStream(
    *      "myTable",
    *      stream,
    *      $"_2", // reorder and use the original field
    *      $"rowtime".rowtime, // extract the internally attached timestamp into an event-time
    *                          // attribute named 'rowtime'
    *      $"_1" as "name" // reorder and give the original field a better name
    *   )
    * }}}
    *
    * 2. Reference input fields by position:
    * In this mode, fields are simply renamed. Event-time attributes can
    * replace the field on their position in the input data (if it is of correct type) or be
    * appended at the end. Proctime attributes must be appended at the end. This mode can only be
    * used if the input type has a defined field order (tuple, case class, Row) and none of
    * the `fields` references a field of the input type.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   tableEnv.registerDataStream(
    *      "myTable",
    *      stream,
    *      $"a", // rename the first field to 'a'
    *      $"b" // rename the second field to 'b'
    *      $"rowtime".rowtime // adds an event-time attribute named 'rowtime'
    *   )
    * }}}
    *
    * The view is registered in the namespace of the current catalog and database. To register the
    * view in a different catalog use [[createTemporaryView]].
    *
    * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
    * it will be inaccessible in the current session. To make the permanent object available again
    * you can drop the corresponding temporary object.
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @param fields The fields expressions to map original fields of the DataStream to the fields of
    *               the View.
    * @tparam T The type of the [[DataStream]] to register.
    * @deprecated use [[createTemporaryView]]
    */
  @deprecated
  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: Expression*): Unit

  /**
    * Creates a view from the given [[DataStream]] in a given path with specified field names.
    * Registered views can be referenced in SQL queries.
    *
    * There are two modes for mapping original fields to the fields of the View:
    *
    * 1. Reference input fields by name:
    * All fields in the schema definition are referenced by name
    * (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
    * attributes at arbitrary positions using arbitrary names (except those that exist in the
    * result schema). In this mode, fields can be reordered and projected out. This mode can
    * be used for any input type, including POJOs.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   tableEnv.createTemporaryView(
    *      "cat.db.myTable",
    *      stream,
    *      $"_2", // reorder and use the original field
    *      $"rowtime".rowtime, // extract the internally attached timestamp into an event-time
    *                          // attribute named 'rowtime'
    *      $"_1" as "name" // reorder and give the original field a better name
    *   )
    * }}}
    *
    * 2. Reference input fields by position:
    * In this mode, fields are simply renamed. Event-time attributes can
    * replace the field on their position in the input data (if it is of correct type) or be
    * appended at the end. Proctime attributes must be appended at the end. This mode can only be
    * used if the input type has a defined field order (tuple, case class, Row) and none of
    * the `fields` references a field of the input type.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   tableEnv.createTemporaryView(
    *      "cat.db.myTable",
    *      stream,
    *      $"a", // rename the first field to 'a'
    *      $"b" // rename the second field to 'b'
    *      $"rowtime".rowtime // adds an event-time attribute named 'rowtime'
    *   )
    * }}}
    *
    * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
    * it will be inaccessible in the current session. To make the permanent object available again
    * you can drop the corresponding temporary object.
    *
    * @param path The path under which the [[DataStream]] is created.
    *             See also the [[TableEnvironment]] class description for the format of the path.
    * @param dataStream The [[DataStream]] out of which to create the view.
    * @param fields The fields expressions to map original fields of the DataStream to the fields of
    *               the View.
    * @tparam T The type of the [[DataStream]].
    * @deprecated Use [[createTemporaryView(String, DataStream, Schema)]] instead. In most cases,
    *             [[createTemporaryView(String, DataStream)]] should already be sufficient. It
    *             integrates with the new type system and supports all kinds of [[DataTypes]] that
    *             the table runtime can consume. The semantics might be slightly different for raw
    *             and structured types.
    */
  @deprecated
  def createTemporaryView[T](path: String, dataStream: DataStream[T], fields: Expression*): Unit

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[Row]] and Scala Tuple types: Fields are mapped by position, field
    * types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    * @deprecated Use [[toDataStream(Table, Class)]] instead. It integrates with the new type
    *             system and supports all kinds of [[DataTypes]] that the table runtime can produce.
    *             The semantics might be slightly different for raw and structured types. Use
    *             `toDataStream(DataTypes.of(Types.of[Class]))` if [[TypeInformation]]
    *             should be used as source of truth.
    */
  @deprecated
  def toAppendStream[T: TypeInformation](table: Table): DataStream[T]

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the requested data type.
    * @return The converted [[DataStream]].
    * @deprecated Use [[toChangelogStream(Table, Schema)]] instead. It integrates with the new
    *             type system and supports all kinds of [[DataTypes]] and every [[ChangelogMode]]
    *             that the table runtime can produce.
    */
  @deprecated
  def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)]
}

object StreamTableEnvironment {

  /**
    * Creates a table environment that is the entry point and central context for creating Table
    * and SQL API programs that integrate with the Scala-specific [[DataStream]] API.
    *
    * It is unified for bounded and unbounded data processing.
    *
    * A stream table environment is responsible for:
    *
    * - Convert a [[DataStream]] into [[Table]] and vice-versa.
    * - Connecting to external systems.
    * - Registering and retrieving [[Table]]s and other meta objects from a catalog.
    * - Executing SQL statements.
    * - Offering further configuration options.
    *
    * Note: If you don't intend to use the [[DataStream]] API, [[TableEnvironment]] is meant for
    * pure table programs.
    *
    * @param executionEnvironment The Scala [[StreamExecutionEnvironment]] of the
    *                             [[TableEnvironment]].
    */
  def create(executionEnvironment: StreamExecutionEnvironment): StreamTableEnvironment = {
    create(executionEnvironment, EnvironmentSettings.newInstance().build)
  }

  /**
    * Creates a table environment that is the entry point and central context for creating Table and
    * SQL API programs that integrate with the Scala-specific [[DataStream]] API.
    *
    * It is unified for bounded and unbounded data processing.
    *
    * A stream table environment is responsible for:
    *
    * - Convert a [[DataStream]] into [[Table]] and vice-versa.
    * - Connecting to external systems.
    * - Registering and retrieving [[Table]]s and other meta objects from a catalog.
    * - Executing SQL statements.
    * - Offering further configuration options.
    *
    * Note: If you don't intend to use the [[DataStream]] API, [[TableEnvironment]] is meant for
    * pure table programs.
    *
    * @param executionEnvironment The Scala [[StreamExecutionEnvironment]] of the
    *                             [[TableEnvironment]].
    * @param settings The environment settings used to instantiate the [[TableEnvironment]].
    */
  def create(
      executionEnvironment: StreamExecutionEnvironment,
      settings: EnvironmentSettings)
    : StreamTableEnvironment = {
    StreamTableEnvironmentImpl.create(executionEnvironment, settings)
  }
}
