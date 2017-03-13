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

import org.apache.flink.api.common.typeinfo.TypeInformation

/** Defines an external table by providing schema information and used to produce a
  * [[org.apache.flink.api.scala.DataSet]] or [[org.apache.flink.streaming.api.scala.DataStream]].
  * Schema information consists of a data type, field names, and corresponding indices of
  * these names in the data type.
  *
  * To define a TableSource one need to implement [[TableSource#getReturnType]]. In this case
  * field names and field indices are derived from the returned type.
  *
  * In case if custom field names are required one need to additionally implement
  * the [[DefinedFieldNames]] trait.
  *
  * @tparam T The return type of the [[TableSource]].
  */
trait TableSource[T] {

  /** Returns the [[TypeInformation]] for the return type of the [[TableSource]]. */
  def getReturnType: TypeInformation[T]

  /** Describes the table source */
  def explainSource(): String = ""
}
