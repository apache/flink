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

package org.apache.flink.table.api.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.internal.TableImpl
import org.apache.flink.table.api.{BatchQueryConfig, StreamQueryConfig, Table, TableException, ValidationException}

/**
  * Holds methods to convert a [[Table]] into a [[DataSet]] or a [[DataStream]].
  *
  * @param table The table to convert.
  */
@PublicEvolving
class TableConversions(table: Table) {

  private val internalTable = table.asInstanceOf[TableImpl]

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T: TypeInformation]: DataSet[T] = {

    internalTable.getTableEnvironment match {
      case tEnv: BatchTableEnvironment =>
        tEnv.toDataSet(table)
      case _ =>
        throw new ValidationException(
          "Only tables that originate from Scala DataSets can be converted to Scala DataSets.")
    }
  }

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T: TypeInformation](queryConfig: BatchQueryConfig): DataSet[T] = {

    internalTable.getTableEnvironment match {
      case tEnv: BatchTableEnvironment =>
        tEnv.toDataSet(table, queryConfig)
      case _ =>
        throw new ValidationException(
          "Only tables that originate from Scala DataSets can be converted to Scala DataSets.")
    }
  }

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and Scala Tuple types: Fields are mapped by position, field
    * types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T: TypeInformation]: DataStream[T] = {

    internalTable.getTableEnvironment match {
      case tEnv: StreamTableEnvironment =>
        tEnv.toAppendStream(table)
      case _ =>
        throw new ValidationException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and Scala Tuple types: Fields are mapped by position, field
    * types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T: TypeInformation](queryConfig: StreamQueryConfig): DataStream[T] = {
    internalTable.getTableEnvironment match {
      case tEnv: StreamTableEnvironment =>
        tEnv.toAppendStream(table, queryConfig)
      case _ =>
        throw new ValidationException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }

  /** Converts the [[Table]] to a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
    *
    */
  def toRetractStream[T: TypeInformation]: DataStream[(Boolean, T)] = {

    internalTable.getTableEnvironment match {
      case tEnv: StreamTableEnvironment =>
        tEnv.toRetractStream(table)
      case _ =>
        throw new TableException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }

  /** Converts the [[Table]] to a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
    *
    * @param queryConfig The configuration for the generated query.
    *
    */
  def toRetractStream[T: TypeInformation](
      queryConfig: StreamQueryConfig): DataStream[(Boolean, T)] = {

    internalTable.getTableEnvironment match {
      case tEnv: StreamTableEnvironment =>
        tEnv.toRetractStream(table, queryConfig)
      case _ =>
        throw new TableException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }
}

