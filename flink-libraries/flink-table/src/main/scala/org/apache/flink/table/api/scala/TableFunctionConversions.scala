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
import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.plan.logical.LogicalTableFunctionCall

/**
  * Holds methods to convert a [[TableFunction]] (provided by scala user) into a [[Table]]
  *
  * @param tf The tableFunction to convert.
  */
class TableFunctionConversions[T](tf: TableFunction[T]) {

  /**
    * Creates a Table to a [[TableFunction]] in Scala Table API.
    *
    * @param params parameters of table function
    * @return a [[Table]] with [[LogicalTableFunctionCall]]
    */
  final def apply(params: Expression*)(implicit typeInfo: TypeInformation[T]): Table = {

    val resultType = if (tf.getResultType == null) typeInfo else tf.getResultType

    new Table(
      tableEnv = null,
      LogicalTableFunctionCall(
        tf.getClass.getCanonicalName,
        tf,
        params.toList,
        resultType,
        Array.empty,
        child = null
      )
    )
  }
}
