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

package org.apache.flink.api.scala.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

import org.apache.flink.api.table.{Table, TableException}
import org.apache.flink.api.scala.table.{BatchTableEnvironment => ScalaBatchTableEnv}
import org.apache.flink.api.scala.table.{StreamTableEnvironment => ScalaStreamTableEnv}

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
          "Only tables that orginate from Scala DataSets can be converted to Scala DataSets.")
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

}

