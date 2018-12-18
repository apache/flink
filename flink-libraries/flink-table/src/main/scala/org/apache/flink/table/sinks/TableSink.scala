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

package org.apache.flink.table.sinks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Table

/** A [[TableSink]] specifies how to emit a [[Table]] to an external
  * system or location.
  *
  * The interface is generic such that it can support different storage locations and formats.
  *
  * @tparam T The return type of the [[TableSink]].
  */
trait TableSink[T] {

  /**
    * Returns the type expected by this [[TableSink]].
    *
    * This type should depend on the types returned by [[getFieldNames]].
    *
    * @return The type expected by this [[TableSink]].
    */
  def getOutputType: TypeInformation[T]

  /** Returns the names of the table fields. */
  def getFieldNames: Array[String]

  /** Returns the types of the table fields. */
  def getFieldTypes: Array[TypeInformation[_]]

  /**
    * Return a copy of this [[TableSink]] configured with the field names and types of the
    * [[Table]] to emit.
    *
    * @param fieldNames The field names of the table to emit.
    * @param fieldTypes The field types of the table to emit.
    * @return A copy of this [[TableSink]] configured with the field names and types of the
    *         [[Table]] to emit.
    */
  def configure(fieldNames: Array[String],
                fieldTypes: Array[TypeInformation[_]]): TableSink[T]
}
