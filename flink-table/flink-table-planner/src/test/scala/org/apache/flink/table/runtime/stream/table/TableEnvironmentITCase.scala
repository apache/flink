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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.junit.Test
import org.apache.flink.api.scala._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.UDFWithJobParameterChecking
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.test.util.AbstractTestBase

class TableEnvironmentITCase extends AbstractTestBase {

  @Test
  def testMergeParametersInStreamTableEnvironment(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    val t = env.fromCollection(Seq(1, 2, 3)).toTable(tEnv, 'a)
    val udfForChecking = new UDFWithJobParameterChecking(Map("testConf" -> "1"))
    t.select(udfForChecking('a)).toAppendStream[Int].print()

    tEnv.getConfig.getConfiguration.setString("testConf", "1")
    tEnv.execute("test")
  }

  @Test
  def testMergeParametersInUnifiedTableEnvironment(): Unit = {
    val tEnv = TableEnvironment.create(
      EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build())
    val env = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner
      .asInstanceOf[StreamPlanner].getExecutionEnvironment
    env.setParallelism(1)

    tEnv.registerTableSink("sink",
      new TestAppendSink().configure(Array("a"), Array(Types.INT())))

    val csvTable = CommonTestData.getCsvTableSource
    val udfForChecking = new UDFWithJobParameterChecking(Map("testConf" -> "1"))
    tEnv.fromTableSource(csvTable).select(udfForChecking('id)).insertInto("sink")

    tEnv.getConfig.getConfiguration.setString("testConf", "1")
    tEnv.execute("test")

    // clear the result stored in RowCollector
    RowCollector.getAndClearValues
  }
}
