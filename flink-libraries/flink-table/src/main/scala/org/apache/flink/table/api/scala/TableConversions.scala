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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, Table, TableException}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv}
import org.apache.flink.table.api.scala.{StreamTableEnvironment => ScalaStreamTableEnv}

/**
  * Holds methods to convert a [[Table]] into a [[DataSet]] or a [[DataStream]].
  *
  * @param table The table to convert.
  */
class TableConversions(table: Table) {

  /** Converts the [[Table]] to a [[DataSet]] of the specified type. */
  def toDataSet[T: TypeInformation]: DataSet[T] = {

    table.tableEnv match {
      case tEnv: ScalaBatchTableEnv =>
        tEnv.toDataSet(table)
      case _ =>
        throw new TableException(
          "Only tables that originate from Scala DataSets can be converted to Scala DataSets.")
    }
  }

  /** Converts the [[Table]] to a [[DataStream]] of the specified type. */
  def toDataStream[T: TypeInformation]: DataStream[T] = {

    table.tableEnv match {
      case tEnv: ScalaStreamTableEnv =>
        tEnv.toDataStream(table)
      case _ =>
        throw new TableException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }

  /** Converts the [[Table]] to a [[DataStream]] of the specified type.
    *
    * @param queryConfig The configuration for the generated query.
    */
  def toDataStream[T: TypeInformation](queryConfig: StreamQueryConfig): DataStream[T] = {
    table.tableEnv match {
      case tEnv: ScalaStreamTableEnv =>
        tEnv.toDataStream(table, queryConfig)
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
    */
  def toRetractStream[T: TypeInformation]: DataStream[(Boolean, T)] = {

    table.tableEnv match {
      case tEnv: ScalaStreamTableEnv =>
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

    table.tableEnv match {
      case tEnv: ScalaStreamTableEnv =>
        tEnv.toRetractStream(table, queryConfig)
      case _ =>
        throw new TableException(
          "Only tables that originate from Scala DataStreams " +
            "can be converted to Scala DataStreams.")
    }
  }



}

