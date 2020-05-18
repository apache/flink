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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.TableTestUtil

object StreamTableEnvUtil {

  //  TODO unify BatchTableEnvUtil and StreamTableEnvUtil
  /**
    * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
    * catalog.
    *
    * @param name     The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @tparam T the type of the [[DataStream]].
    */
  def createTemporaryViewInternal[T](
      tEnv: StreamTableEnvironment,
      name: String,
      dataStream: DataStream[T],
      fieldNames: Option[Array[String]],
      fieldNullables: Option[Array[Boolean]],
      statistic: Option[FlinkStatistic]): Unit = {
    val fields: Option[Array[Expression]] = fieldNames match {
      case Some(names) => Some(names.map(ExpressionParser.parseExpression))
      case _ => None
    }
    TableTestUtil.createTemporaryView(tEnv, name, dataStream, fields, fieldNullables, statistic)
  }

}
