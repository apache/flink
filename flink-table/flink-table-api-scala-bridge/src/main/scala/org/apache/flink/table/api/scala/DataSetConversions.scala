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

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.Expression

/**
  * Holds methods to convert a [[DataSet]] into a [[Table]].
  *
  * @param dataSet The [[DataSet]] to convert.
  * @param inputType The [[TypeInformation]] for the type of the [[DataSet]].
  * @tparam T The type of the [[DataSet]].
  */
@PublicEvolving
class DataSetConversions[T](dataSet: DataSet[T], inputType: TypeInformation[T]) {

  /**
    * Converts the [[DataSet]] into a [[Table]].
    *
    * The field names of the new [[Table]] can be specified like this:
    *
    * {{{
    *   val env = ExecutionEnvironment.getExecutionEnvironment
    *   val tEnv = BatchTableEnvironment.create(env)
    *
    *   val set: DataSet[(String, Int)] = ...
    *   val table = set.toTable(tEnv, 'name, 'amount)
    * }}}
    *
    * If not explicitly specified, field names are automatically extracted from the type of
    * the [[DataSet]].
    *
    * @param tableEnv The [[BatchTableEnvironment]] in which the new [[Table]] is created.
    * @param fields The field names of the new [[Table]] (optional).
    * @return The resulting [[Table]].
    */
  def toTable(tableEnv: BatchTableEnvironment, fields: Expression*): Table = {
    if (fields.isEmpty) {
      tableEnv.fromDataSet(dataSet)
    } else {
      tableEnv.fromDataSet(dataSet, fields: _*)
    }
  }

}

