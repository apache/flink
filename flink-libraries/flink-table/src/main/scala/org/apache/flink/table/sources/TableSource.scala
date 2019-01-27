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

package org.apache.flink.table.sources

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.util.TableConnectorUtil

/** Defines an external table by providing schema information and used to produce a
  * [[org.apache.flink.api.scala.DataSet]] or [[org.apache.flink.streaming.api.scala.DataStream]].
  * Schema information consists of a data type, field names, and corresponding indices of
  * these names in the data type.
  *
  * To define a TableSource one needs to implement [[TableSource#getReturnType]]. In this case
  * field names and field indices are derived from the returned type.
  *
  * In case if custom field names are required one need to additionally implement
  * the [[DefinedFieldMapping]] trait.
  */
trait TableSource {

  /** Returns the [[DataType]] for the return type of the [[TableSource]]. */
  def getReturnType: DataType

  /** Returns the table schema of the table source */
  def getTableSchema: TableSchema

  /** Describes the table source, it will be used for computing qualified names of
    * [[org.apache.flink.table.plan.schema.FlinkRelOptTable]].
    *
    * Naming Convention that developers MUST follow:
    *
    * "<connector_type_name>-<stream_table_name>"
    *
    * Note: After project push down or filter push down, the explainSource value of new TableSource
    * should be different with the original one.
    *
    *  */
  def explainSource(): String =
    TableConnectorUtil.generateRuntimeName(getClass, getTableSchema.getColumnNames)

  /** Returns the statistics of the table. */
  def getTableStats: TableStats = null

}
