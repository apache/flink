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
import org.apache.flink.api.table._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Methods for converting a [[Table]] to a [[DataSet]] or a [[DataStream]]. A [[Table]] is
 * wrapped in this by the implicit conversions in [[org.apache.flink.api.scala.table]].
 */
class TableConversions(table: Table) {

  /**
   * Converts the [[Table]] to a [[DataSet]] using the default configuration.
   */
  def toDataSet[T: TypeInformation]: DataSet[T] = {
     new ScalaBatchTranslator().translate[T](table.relNode)
  }

  /**
   * Converts the [[Table]] to a [[DataSet]] using a custom configuration.
   */
  def toDataSet[T: TypeInformation](config: TableConfig): DataSet[T] = {
     new ScalaBatchTranslator(config).translate[T](table.relNode)
  }

  /**
   * Converts the [[Table]] to a [[DataStream]] using the default configuration.
   */
  def toDataStream[T: TypeInformation]: DataStream[T] = {
     new ScalaStreamTranslator().translate[T](table.relNode)
  }

  /**
   * Converts the [[Table]] to a [[DataStream]] using a custom configuration.
   */
  def toDataStream[T: TypeInformation](config: TableConfig): DataStream[T] = {
     new ScalaStreamTranslator(config).translate[T](table.relNode)
  }
}

