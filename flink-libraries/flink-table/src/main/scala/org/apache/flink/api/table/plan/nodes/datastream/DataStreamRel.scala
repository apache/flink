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

package org.apache.flink.api.table.plan.nodes.datastream

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.{StreamTableEnvironment, TableConfig}
import org.apache.flink.api.table.plan.nodes.FlinkRel
import org.apache.flink.streaming.api.datastream.DataStream

trait DataStreamRel extends RelNode with FlinkRel {

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @param expectedType specifies the type the Flink operator should return. The type must
    *                     have the same arity as the result. For instance, if the
    *                     expected type is a RowTypeInfo this method will return a DataSet of
    *                     type Row. If the expected type is Tuple2, the operator will return
    *                     a Tuple2 if possible. Row otherwise.
    * @return DataStream of type expectedType or RowTypeInfo
    */
  def translateToPlan(
    tableEnv: StreamTableEnvironment,
    expectedType: Option[TypeInformation[Any]] = None) : DataStream[Any]

}

