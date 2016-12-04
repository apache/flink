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
import org.apache.flink.api.table.expressions.{Expression, TableFunctionCall}
import org.apache.flink.api.table.functions.TableFunction

case class TableFunctionCallBuilder[T: TypeInformation](udtf: TableFunction[T]) {
  /**
    * Creates a call to a [[TableFunction]] in Scala Table API.
    *
    * @param params actual parameters of function
    * @return [[TableFunctionCall]]
    */
  def apply(params: Expression*): Expression = {
    val resultType = if (udtf.getResultType == null) {
      implicitly[TypeInformation[T]]
    } else {
      udtf.getResultType
    }
    TableFunctionCall(udtf.getClass.getSimpleName, udtf, params, resultType)
  }
}
