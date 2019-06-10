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

package org.apache.flink.table.runtime.utils

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.functions.{AsyncTableFunction, FunctionContext, TableFunction}
import org.apache.flink.table.runtime.utils.InMemoryLookupableTableSource.{InMemoryAsyncLookupFunction, InMemoryLookupFunction}
import org.apache.flink.table.sources.TableIndex.IndexType
import org.apache.flink.table.sources._
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{Consumer, Supplier}

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A [[LookupableTableSource]] which stores table in memory, this is mainly used for testing.
  */
class InMemoryLookupableTableSource(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    data: List[Row],
    primaryKey: Option[Array[String]],
    tableIndexes: Array[TableIndex],
    lookupConfig: LookupConfig)
  extends LookupableTableSource[Row]
  with StreamTableSource[Row]
  with DefinedPrimaryKey
  with DefinedIndexes {

  lazy val uniqueKeys: Array[Array[String]] = {
    val keys = new mutable.ArrayBuffer[Array[String]]()
    if (getPrimaryKeyColumns != null) {
      keys += getPrimaryKeyColumns.asScala.toArray
    }
    getIndexes.asScala
      .filter(_.getIndexType == IndexType.UNIQUE)
      .foreach(keys += _.getIndexedColumns.asScala.toArray)
    keys.toArray
  }

  val resourceCounter = new AtomicInteger(0)

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[Row] = {
    new InMemoryLookupFunction(convertDataToMap(lookupKeys), resourceCounter)
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[Row] = {
    new InMemoryAsyncLookupFunction(convertDataToMap(lookupKeys), resourceCounter)
  }

  private def convertDataToMap(lookupKeys: Array[String]): Map[Row, List[Row]] = {
    val isUniqueKey = uniqueKeys.contains(lookupKeys)
    val lookupFieldIndexes = lookupKeys.map(fieldNames.indexOf(_))
    val map = mutable.HashMap[Row, List[Row]]()
    if (isUniqueKey) {
      data.foreach { row =>
        val key = Row.of(lookupFieldIndexes.map(row.getField): _*)
        val oldValue = map.put(key, List(row))
        if (oldValue.isDefined) {
          throw new IllegalStateException("data contains duplicate keys.")
        }
      }
    } else {
      data.foreach { row =>
        val key = Row.of(lookupFieldIndexes.map(row.getField): _*)
        val oldValue = map.get(key)
        if (oldValue.isDefined) {
          map.put(key, oldValue.get ++ List(row))
        } else {
          map.put(key, List(row))
        }
      }
    }
    map.toMap
  }

  override def getLookupConfig: LookupConfig = lookupConfig

  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)

  override def getPrimaryKeyColumns: util.List[String] = primaryKey match {
    case Some(pk) => pk.toList.asJava
    case None => null // return null to indicate no primary key is defined.
  }

  override def getIndexes: util.Collection[TableIndex] = tableIndexes.toList.asJava

  @VisibleForTesting
  def getResourceCounter: Int = resourceCounter.get()

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    throw new UnsupportedOperationException("This should never be called.")
  }
}

object InMemoryLookupableTableSource {

  /**
    * Return a new builder that builds a [[InMemoryLookupableTableSource]].
    *
    * For example:
    *
    * {{{
    *     val data = (
    *       (11, 1L, "Julian"),
    *       (22, 2L, "Jark"),
    *       (33, 3L, "Fabian"))
    *
    *     val source = InMemoryLookupableTableSource.builder()
    *       .data(data)
    *       .field("age", Types.INT)
    *       .field("id", Types.LONG)
    *       .field("name", Types.STRING)
    *       .primaryKey("id")
    *       .addNormalIndex("name")
    *       .enableAsync()
    *       .build()
    * }}}
    *
    * @return a new builder to build a [[InMemoryLookupableTableSource]]
    */
  def builder(): Builder = new Builder


  /**
    * A builder for creating [[InMemoryLookupableTableSource]] instances.
    *
    * For example:
    *
    * {{{
    *     val data = (
    *       (11, 1L, "Julian"),
    *       (22, 2L, "Jark"),
    *       (33, 3L, "Fabian"))
    *
    *     val source = InMemoryLookupableTableSource.builder()
    *       .data(data)
    *       .field("age", Types.INT)
    *       .field("id", Types.LONG)
    *       .field("name", Types.STRING)
    *       .primaryKey("id")
    *       .addNormalIndex("name")
    *       .enableAsync()
    *       .build()
    * }}}
    */
  class Builder {
    private val schema = new mutable.LinkedHashMap[String, TypeInformation[_]]()
    private val tableIndexes = new mutable.ArrayBuffer[TableIndex]()
    private var primaryKey: Option[Array[String]] = None
    private var data: List[Product] = _
    private val lookupConfigBuilder: LookupConfig.Builder = LookupConfig.builder()

    /**
      * Sets table data for the table source.
      */
    def data(data: List[Product]): Builder = {
      this.data = data
      this
    }

    /**
      * Adds a field with the field name and the type information. Required.
      * This method can be called multiple times. The call order of this method defines
      * also the order of the fields in a row.
      *
      * @param fieldName the field name
      * @param fieldType the type information of the field
      */
    def field(fieldName: String, fieldType: TypeInformation[_]): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      schema += (fieldName -> fieldType)
      this
    }

    /**
      * Sets primary key for the table source.
      */
    def primaryKey(fields: String*): Builder = {
      if (fields.isEmpty) {
        throw new IllegalArgumentException("fields should not be empty.")
      }
      if (primaryKey != null && primaryKey.isDefined) {
        throw new IllegalArgumentException("primary key has been set.")
      }
      this.primaryKey = Some(fields.toArray)
      this
    }

    /**
      * Adds a normal [[TableIndex]] for the table source
      */
    def addNormalIndex(fields: String*): Builder = {
      if (fields.isEmpty) {
        throw new IllegalArgumentException("fields should not be empty.")
      }
      val index = TableIndex.builder()
        .normalIndex()
        .indexedColumns(fields: _*)
        .build()
      tableIndexes += index
      this
    }

    /**
      * Adds an unique [[TableIndex]] for the table source
      */
    def addUniqueIndex(fields: String*): Builder = {
      if (fields.isEmpty) {
        throw new IllegalArgumentException("fields should not be empty.")
      }
      val index = TableIndex.builder()
        .uniqueIndex()
        .indexedColumns(fields: _*)
        .build()
      tableIndexes += index
      this
    }

    /**
      * Enables async lookup for the table source
      */
    def enableAsync(): Builder = {
      lookupConfigBuilder.setAsyncEnabled(true)
      this
    }

    /**
      * Sets async buffer capacity.
      */
    def asyncBufferCapacity(capacity: Int): Builder = {
      lookupConfigBuilder.setAsyncBufferCapacity(capacity)
      this
    }

    /**
      * Sets async time out milli-second.
      */
    def asyncTimeoutMs(ms: Long): Builder = {
      lookupConfigBuilder.setAsyncTimeoutMs(ms)
      this
    }

    /**
      * Apply the current values and constructs a newly-created [[InMemoryLookupableTableSource]].
      *
      * @return a newly-created [[InMemoryLookupableTableSource]].
      */
    def build(): InMemoryLookupableTableSource = {
      val fieldNames = schema.keys.toArray
      val fieldTypes = schema.values.toArray
      Preconditions.checkNotNull(data)
      // convert
      val rowData = data.map { entry =>
        Row.of((0 until entry.productArity).map(entry.productElement(_).asInstanceOf[Object]): _*)
      }
      new InMemoryLookupableTableSource(
        fieldNames,
        fieldTypes,
        rowData,
        primaryKey,
        tableIndexes.toArray,
        lookupConfigBuilder.build()
      )
    }
  }

  /**
    * A lookup function which find matched rows with the given fields.
    */
  private class InMemoryLookupFunction(
      data: Map[Row, List[Row]],
      resourceCounter: AtomicInteger)
    extends TableFunction[Row] {

    override def open(context: FunctionContext): Unit = {
      resourceCounter.incrementAndGet()
    }

    @varargs
    def eval(inputs: AnyRef*): Unit = {
      val key = Row.of(inputs: _*)
      data.get(key) match {
        case Some(list) => list.foreach(result => collect(result))
        case None => // do nothing
      }
    }

    override def close(): Unit = {
      resourceCounter.decrementAndGet()
    }
  }

  /**
    * An async lookup function which find matched rows with the given fields.
    */
  private class InMemoryAsyncLookupFunction(
      data: Map[Row, List[Row]],
      resourceCounter: AtomicInteger,
      delayedReturn: Int = 0)
    extends AsyncTableFunction[Row] {

    @transient
    var executor: ExecutorService = _

    override def open(context: FunctionContext): Unit = {
      resourceCounter.incrementAndGet()
      executor = Executors.newSingleThreadExecutor()
    }

    @varargs
    def eval(resultFuture: ResultFuture[Row], inputs: AnyRef*): Unit = {
      CompletableFuture
        .supplyAsync(new CollectionSupplier(data, Row.of(inputs: _*)), executor)
        .thenAccept(new CollectionConsumer(resultFuture))
    }

    override def close(): Unit = {
      resourceCounter.decrementAndGet()
      if (null != executor && !executor.isShutdown) {
        executor.shutdown()
      }
    }

    private class CollectionSupplier(data: Map[Row, List[Row]], key: Row)
        extends Supplier[util.Collection[Row]] {

      override def get(): util.Collection[Row] = {
        val list = data.get(key)
        if (list.isDefined && list.get.nonEmpty) {
          list.get.asJavaCollection
        } else {
          Collections.emptyList()
        }
      }
    }

    private class CollectionConsumer(resultFuture: ResultFuture[Row])
        extends Consumer[util.Collection[Row]] {

      override def accept(results: util.Collection[Row]): Unit = {
        resultFuture.complete(results)
      }
    }
  }

}
