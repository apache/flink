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

package org.apache.flink.api.table.sinks

import org.apache.flink.api.common.typeinfo.TypeInformation

/** A [[TableSink]] specifies how to emit a [[org.apache.flink.api.table.Table]] to an external
  * system or location.
  *
  * The interface is generic such that it can support different storage locations and formats.
  *
  * @tparam T The return type of the [[TableSink]].
  */
trait TableSink[T] {

  private var fieldNames: Option[Array[String]] = None
  private var fieldTypes: Option[Array[TypeInformation[_]]] = None

  /**
    * Return the type expected by this [[TableSink]].
    *
    * This type should depend on the types returned by [[getFieldNames]].
    *
    * @return The type expected by this [[TableSink]].
    */
  def getOutputType: TypeInformation[T]

  /** Return a deep copy of the [[TableSink]]. */
  protected def copy: TableSink[T]

  /**
    * Return the field names of the [[org.apache.flink.api.table.Table]] to emit. */
  protected final def getFieldNames: Array[String] = {
    fieldNames match {
      case Some(n) => n
      case None => throw new IllegalStateException(
        "TableSink must be configured to retrieve field names.")
    }
  }

  /** Return the field types of the [[org.apache.flink.api.table.Table]] to emit. */
  protected final def getFieldTypes: Array[TypeInformation[_]] = {
    fieldTypes match {
      case Some(t) => t
      case None => throw new IllegalStateException(
        "TableSink must be configured to retrieve field types.")
    }
  }

  /**
    * Return a copy of this [[TableSink]] configured with the field names and types of the
    * [[org.apache.flink.api.table.Table]] to emit.
    *
    * @param fieldNames The field names of the table to emit.
    * @param fieldTypes The field types of the table to emit.
    * @return A copy of this [[TableSink]] configured with the field names and types of the
    *         [[org.apache.flink.api.table.Table]] to emit.
    */
  private[flink] final def configure(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]]): TableSink[T] = {

    val configuredSink = this.copy
    configuredSink.fieldNames = Some(fieldNames)
    configuredSink.fieldTypes = Some(fieldTypes)

    configuredSink
  }

}
