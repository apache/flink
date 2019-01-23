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

import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.util.TableConnectorUtil

/**
  * Defines an external table with the schema that is provided by [[TableSource#getTableSchema]].
  *
  * The data of a [[TableSource]] is produced as a [[DataSet]] in case of a [[BatchTableSource]] or
  * as a [[DataStream]] in case of a [[StreamTableSource]].
  * The type of ths produced [[DataSet]] or [[DataStream]] is specified by the
  * [[TableSource#getReturnType]] method.
  *
  * By default, the fields of the [[TableSchema]] are implicitly mapped by name to the fields of the
  * return type [[TypeInformation]]. An explicit mapping can be defined by implementing the
  * [[DefinedFieldMapping]] interface.
  *
  * @tparam T The return type of the [[TableSource]].
  */
trait TableSource[T] {

  /** Returns the [[TypeInformation]] for the return type of the [[TableSource]].
    * The fields of the return type are mapped to the table schema based on their name.
    *
    * @return The type of the returned [[DataSet]] or [[DataStream]].
    */
  def getReturnType: TypeInformation[T]

  /**
    * Returns the schema of the produced table.
    *
    * @return The [[TableSchema]] of the produced table.
    */
  def getTableSchema: TableSchema

  /**
    * Describes the table source.
    *
    * @return A String explaining the [[TableSource]].
    */
  def explainSource(): String =
    TableConnectorUtil.generateRuntimeName(getClass, getTableSchema.getFieldNames)
}
