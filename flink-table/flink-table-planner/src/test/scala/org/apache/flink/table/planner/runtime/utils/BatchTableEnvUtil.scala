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
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.legacy.io.CollectionInputFormat
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.TableTestUtil

import _root_.java.util.UUID
import _root_.scala.collection.JavaConverters._

import scala.reflect.ClassTag

object BatchTableEnvUtil {

  def parseFieldNames(fields: String): Array[Expression] = {
    if (fields != null) {
      fields.replace(" ", "").split(",").map($)
    } else {
      null
    }
  }

  /**
   * Registers the given [[Iterable]] as table in the [[TableEnvironment]]'s catalog.
   *
   * @param tableName
   *   name of table.
   * @param data
   *   The [[Iterable]] to be converted.
   * @param typeInfo
   *   information of [[Iterable]].
   * @param fieldNames
   *   field names expressions, eg: 'a, 'b, 'c
   * @tparam T
   *   The type of the [[Iterable]].
   * @return
   *   The converted [[Table]].
   */
  def registerCollection[T](
      tEnv: TableEnvironment,
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fieldNames: String): Unit = {
    registerCollection(
      tEnv,
      tableName,
      data,
      typeInfo,
      Some(parseFieldNames(fieldNames)),
      None,
      None)
  }

  /**
   * Registers the given [[Iterable]] as table in the [[TableEnvironment]]'s catalog.
   *
   * @param tableName
   *   name of table.
   * @param data
   *   The [[Iterable]] to be converted.
   * @param typeInfo
   *   information of [[Iterable]].
   * @param fieldNames
   *   field names, eg: "a, b, c"
   * @param fieldNullables
   *   The field isNullables attributes of data.
   * @param statistic
   *   statistics of current Table
   * @tparam T
   *   The type of the [[Iterable]].
   * @return
   *   The converted [[Table]].
   */
  def registerCollection[T](
      tEnv: TableEnvironment,
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fieldNames: String,
      fieldNullables: Array[Boolean],
      statistic: Option[FlinkStatistic]): Unit = {
    registerCollection(
      tEnv,
      tableName,
      data,
      typeInfo,
      Some(parseFieldNames(fieldNames)),
      Option(fieldNullables),
      statistic)
  }

  /**
   * Registers the given [[Iterable]] as table in the [[TableEnvironment]]'s catalog.
   *
   * @param tableName
   *   name of table.
   * @param data
   *   The [[Iterable]] to be converted.
   * @param fieldNames
   *   field names expressions, eg: 'a, 'b, 'c
   * @tparam T
   *   The type of the [[Iterable]].
   * @return
   *   The converted [[Table]].
   */
  def registerCollection[T: ClassTag: TypeInformation](
      tEnv: TableEnvironment,
      tableName: String,
      data: Iterable[T],
      fieldNames: String): Unit = {
    val typeInfo = implicitly[TypeInformation[T]]
    BatchTableEnvUtil.registerCollection(
      tEnv,
      tableName,
      data,
      typeInfo,
      Some(parseFieldNames(fieldNames)),
      None,
      None)
  }

  /**
   * Registers the given [[Iterable]] as table in the [[TableEnvironment]]'s catalog.
   *
   * @param tableName
   *   name of table.
   * @param data
   *   The [[Iterable]] to be converted.
   * @param fieldNames
   *   field names, eg: "a, b, c"
   * @param fieldNullables
   *   The field isNullables attributes of data.
   * @param statistic
   *   statistics of current Table
   * @tparam T
   *   The type of the [[Iterable]].
   * @return
   *   The converted [[Table]].
   */
  def registerCollection[T: ClassTag: TypeInformation](
      tEnv: TableEnvironment,
      tableName: String,
      data: Iterable[T],
      fieldNames: String,
      fieldNullables: Array[Boolean],
      statistic: Option[FlinkStatistic]): Unit = {
    val typeInfo = implicitly[TypeInformation[T]]
    BatchTableEnvUtil.registerCollection(
      tEnv,
      tableName,
      data,
      typeInfo,
      Some(parseFieldNames(fieldNames)),
      Option(fieldNullables),
      statistic)
  }

  /**
   * Create a [[Table]] from sequence of elements. Typical, user can pass in a sequence of tuples,
   * the table schema type would be inferred from the tuple type: e.g.
   * {{{
   *   tEnv.fromElements((1, 2, "abc"), (3, 4, "def"))
   * }}}
   * Then the schema type would be (_1:int, _2:int, _3:varchar)
   *
   * Caution that use must pass a ''Scala'' type data elements, or the inferred type would be
   * unexpected.
   *
   * @param data
   *   row data sequence
   * @tparam T
   *   row data class type
   * @return
   *   table from the data with default fields names
   */
  def fromElements[T: ClassTag: TypeInformation](tEnv: TableEnvironment, data: T*): Table = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    fromCollection(tEnv, data)(implicitly[ClassTag[T]], typeInfo)
  }

  /**
   * Create a [[Table]] from a scala [[Iterable]]. The default fields names would be like _1, _2, _3
   * and so on. The table schema type would be inferred from the [[Iterable]] element type.
   */
  def fromCollection[T: ClassTag: TypeInformation](
      tEnv: TableEnvironment,
      data: Iterable[T]): Table = {
    val typeInfo = implicitly[TypeInformation[T]]
    BatchTableEnvUtil.fromCollection(tEnv, null, data, typeInfo, null, None)
  }

  /**
   * Create a [[Table]] from a scala [[Iterable]]. The table schema type would be inferred from the
   * [[Iterable]] element type.
   */
  def fromCollection[T: ClassTag: TypeInformation](
      tEnv: TableEnvironment,
      data: Iterable[T],
      fields: String): Table = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    BatchTableEnvUtil.fromCollection(tEnv, data, typeInfo, fields)
  }

  /**
   * Registers the given [[Iterable]] as table in the [[TableEnvironment]]'s catalog.
   *
   * @param tableName
   *   name of table.
   * @param data
   *   The [[Iterable]] to be converted.
   * @param typeInfo
   *   information of [[Iterable]].
   * @param fieldNames
   *   field names.
   * @param fieldNullables
   *   The field isNullables attributes of data.
   * @param statistic
   *   statistics of current Table
   * @param forceNonParallel
   *   sets the operator with only one parallelism
   * @tparam T
   *   The type of the [[Iterable]].
   * @return
   *   The converted [[Table]].
   */
  @VisibleForTesting
  private[table] def registerCollection[T](
      tEnv: TableEnvironment,
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: Option[Array[Expression]],
      fieldNullables: Option[Array[Boolean]],
      statistic: Option[FlinkStatistic],
      forceNonParallel: Boolean = true): Unit = {
    val execEnv = getPlanner(tEnv).getExecEnv
    val boundedStream = execEnv.createInput(
      new CollectionInputFormat[T](
        data.asJavaCollection,
        typeInfo.createSerializer(execEnv.getConfig.getSerializerConfig)),
      typeInfo)
    if (forceNonParallel) {
      boundedStream.forceNonParallel()
    }
    registerBoundedStreamInternal(tEnv, tableName, boundedStream, fields, fieldNullables, statistic)
  }

  /**
   * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s catalog.
   *
   * @param name
   *   The name under which the table is registered in the catalog.
   * @param boundedStream
   *   The [[DataStream]] to register as table in the catalog.
   * @tparam T
   *   the type of the [[DataStream]].
   */
  private[flink] def registerBoundedStreamInternal[T](
      tEnv: TableEnvironment,
      name: String,
      boundedStream: DataStream[T],
      fields: Option[Array[Expression]],
      fieldNullables: Option[Array[Boolean]],
      statistic: Option[FlinkStatistic]): Unit = {
    // for tests we know that this stream is definitely bounded
    ExecNodeUtil.makeLegacySourceTransformationsBounded(boundedStream.getTransformation)
    TableTestUtil.createTemporaryView(
      tEnv,
      name,
      boundedStream,
      fields,
      fieldNullables,
      statistic
    )
  }

  /**
   * Create a [[Table]] from a scala [[Iterable]]. Would infer table schema from the passed in
   * typeInfo.
   */
  private[table] def fromCollection[T](
      tEnv: TableEnvironment,
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: Array[Expression],
      statistic: Option[FlinkStatistic]): Table = {
    CollectionInputFormat.checkCollection(data.asJavaCollection, typeInfo.getTypeClass)
    val execEnv = getPlanner(tEnv).getExecEnv
    val boundedStream = execEnv.createInput(
      new CollectionInputFormat[T](
        data.asJavaCollection,
        typeInfo.createSerializer(execEnv.getConfig.getSerializerConfig)),
      typeInfo)
    boundedStream.setParallelism(1)
    val name = if (tableName == null) UUID.randomUUID().toString else tableName
    registerBoundedStreamInternal(tEnv, name, boundedStream, Option(fields), None, statistic)
    tEnv.from("`" + name + "`")
  }

  /**
   * Create a [[Table]] from a scala [[Iterable]]. Would infer table schema from the passed in
   * typeInfo.
   */
  def fromCollection[T](
      tEnv: TableEnvironment,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: String): Table = {
    fromCollection(tEnv, null, data, typeInfo, parseFieldNames(fields), None)
  }

  private def getPlanner(tEnv: TableEnvironment): PlannerBase = {
    tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
  }
}
