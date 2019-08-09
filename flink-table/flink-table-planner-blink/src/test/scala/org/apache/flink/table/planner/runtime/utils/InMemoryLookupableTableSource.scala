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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.functions.{AsyncTableFunction, FunctionContext, TableFunction}
import org.apache.flink.table.planner.runtime.utils.InMemoryLookupableTableSource.{InMemoryAsyncLookupFunction, InMemoryLookupFunction}
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
    asyncEnabled: Boolean)
  extends LookupableTableSource[Row] {

  val resourceCounter = new AtomicInteger(0)

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[Row] = {
    new InMemoryLookupFunction(convertDataToMap(lookupKeys), resourceCounter)
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[Row] = {
    new InMemoryAsyncLookupFunction(convertDataToMap(lookupKeys), resourceCounter)
  }

  private def convertDataToMap(lookupKeys: Array[String]): Map[Row, List[Row]] = {
    val lookupFieldIndexes = lookupKeys.map(fieldNames.indexOf(_))
    val map = mutable.HashMap[Row, List[Row]]()
    data.foreach { row =>
      val key = Row.of(lookupFieldIndexes.map(row.getField): _*)
      val oldValue = map.get(key)
      if (oldValue.isDefined) {
        map.put(key, oldValue.get ++ List(row))
      } else {
        map.put(key, List(row))
      }
    }
    map.toMap
  }

  override def isAsyncEnabled: Boolean = asyncEnabled

  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)

  @VisibleForTesting
  def getResourceCounter: Int = resourceCounter.get()

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
    *       .enableAsync()
    *       .build()
    * }}}
    */
  class Builder {
    private val schema = new mutable.LinkedHashMap[String, TypeInformation[_]]()
    private var data: List[Product] = _
    private var asyncEnabled: Boolean = false

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
      * Enables async lookup for the table source
      */
    def enableAsync(): Builder = {
      asyncEnabled = true
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
        asyncEnabled
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
      Preconditions.checkArgument(!inputs.contains(null),
        s"Lookup key %s contains null value, which would not happen.", key)
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
  @SerialVersionUID(1L)
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
    def eval(resultFuture: CompletableFuture[util.Collection[Row]], inputs: AnyRef*): Unit = {
      val key = Row.of(inputs: _*)
      Preconditions.checkArgument(!inputs.contains(null),
        s"Lookup key %s contains null value, which would not happen.", key)
      CompletableFuture
        .supplyAsync(new CollectionSupplier(data, key), executor)
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

    private class CollectionConsumer(resultFuture: CompletableFuture[util.Collection[Row]])
        extends Consumer[util.Collection[Row]] {

      override def accept(results: util.Collection[Row]): Unit = {
        resultFuture.complete(results)
      }
    }
  }

}
