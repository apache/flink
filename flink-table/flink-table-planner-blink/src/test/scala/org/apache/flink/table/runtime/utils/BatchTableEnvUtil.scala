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
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{BatchTableEnvironment, Table, TableEnvironment}
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sinks.CollectTableSink
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.util.AbstractID
import _root_.java.util.{ArrayList => JArrayList}

import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

object BatchTableEnvUtil {

  def collect[T](
      tEnv: BatchTableEnvironment,
      table: Table,
      sink: CollectTableSink[T],
      jobName: Option[String]): Seq[T] = {
    val typeSerializer = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[T]]
      .createSerializer(tEnv.streamEnv.getConfig)
    val id = new AbstractID().toString
    sink.init(typeSerializer.asInstanceOf[TypeSerializer[T]], id)
    tEnv.writeToSink(table, sink)

    val res = tEnv.execute()
    val accResult: JArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    SerializedListAccumulator.deserializeList(accResult, typeSerializer)
  }

  def parseFieldNames(fields: String): Array[String] = {
    fields.replace(" ", "").split(",")
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param typeInfo information of [[Iterable]].
    * @param fieldNames field names expressions, eg: 'a, 'b, 'c
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](tEnv: BatchTableEnvironment,
      tableName: String, data: Iterable[T], typeInfo: TypeInformation[T],
      fieldNames: String): Unit = {
    registerCollection(
      tEnv, tableName, data, typeInfo, Some(parseFieldNames(fieldNames)), None, None)
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param typeInfo information of [[Iterable]].
    * @param fieldNames field names, eg: "a, b, c"
    * @param fieldNullables The field isNullables attributes of data.
    * @param statistic statistics of current Table
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](
      tEnv: BatchTableEnvironment,
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fieldNames: String,
      fieldNullables: Array[Boolean],
      statistic: Option[FlinkStatistic]): Unit = {
    registerCollection(tEnv, tableName, data, typeInfo,
      Some(parseFieldNames(fieldNames)), Option(fieldNullables), statistic)
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param typeInfo information of [[Iterable]].
    * @param fieldNames field names.
    * @param fieldNullables The field isNullables attributes of data.
    * @param statistic statistics of current Table
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  @VisibleForTesting
  private [table] def registerCollection[T](
      tEnv: BatchTableEnvironment,
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fieldNames: Option[Array[String]],
      fieldNullables: Option[Array[Boolean]],
      statistic: Option[FlinkStatistic]): Unit = {
    val boundedStream = tEnv.streamEnv.createInput(new CollectionInputFormat[T](
      data.asJavaCollection,
      typeInfo.createSerializer(tEnv.streamEnv.getConfig)),
      typeInfo)
    boundedStream.forceNonParallel()
    registerBoundedStreamInternal(
      tEnv, tableName, boundedStream, fieldNames, fieldNullables, statistic)
  }

  /**
    * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
    * catalog.
    *
    * @param name     The name under which the table is registered in the catalog.
    * @param boundedStream The [[DataStream]] to register as table in the catalog.
    * @tparam T the type of the [[DataStream]].
    */
  protected def registerBoundedStreamInternal[T](
      tEnv: BatchTableEnvironment,
      name: String,
      boundedStream: DataStream[T],
      fieldNames: Option[Array[String]],
      fieldNullables: Option[Array[Boolean]],
      statistic: Option[FlinkStatistic]): Unit = {
    val (typeFieldNames, fieldIdxs) =
      tEnv.getFieldInfo(fromLegacyInfoToDataType(boundedStream.getTransformation.getOutputType))
    val boundedStreamTable = new DataStreamTable[T](
      boundedStream, fieldIdxs, fieldNames.getOrElse(typeFieldNames), fieldNullables)
    val withStatistic = boundedStreamTable.copy(statistic.getOrElse(FlinkStatistic.UNKNOWN))
    tEnv.registerTableInternal(name, withStatistic)
  }

  /**
    * Create a [[Table]] from a scala [[Iterable]]. Would infer table schema from the passed in
    * typeInfo.
    */
  private[table] def fromCollection[T](
      tEnv: BatchTableEnvironment,
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fieldNames: Array[String],
      statistic: Option[FlinkStatistic]): Table = {
    CollectionInputFormat.checkCollection(data.asJavaCollection, typeInfo.getTypeClass)
    val boundedStream = tEnv.streamEnv.createInput(new CollectionInputFormat[T](
      data.asJavaCollection,
      typeInfo.createSerializer(tEnv.streamEnv.getConfig)),
      typeInfo)
    boundedStream.setParallelism(1)
    val name = if (tableName == null) tEnv.createUniqueTableName() else tableName
    registerBoundedStreamInternal(tEnv, name, boundedStream, Option(fieldNames), None, statistic)
    tEnv.scan(name)
  }

  /**
    * Create a [[Table]] from a scala [[Iterable]]. Would infer table schema from the passed in
    * typeInfo.
    */
  def fromCollection[T](tEnv: BatchTableEnvironment,
      data: Iterable[T], typeInfo: TypeInformation[T], fields: String): Table = {
    fromCollection(tEnv, null, data, typeInfo, parseFieldNames(fields), None)
  }
}
