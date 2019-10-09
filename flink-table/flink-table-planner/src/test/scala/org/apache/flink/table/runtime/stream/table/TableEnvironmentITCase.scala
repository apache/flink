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

import java.io.File

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.junit.Assert.assertEquals
import org.junit.Test
import org.apache.flink.api.scala._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.utils.CommonTestData

class TableEnvironmentITCase {

  @Test
  def testMergeParametersInStreamTableEnvironment(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val t = env.fromCollection(Seq(1, 2, 3)).toTable(tEnv, 'a)

    tEnv.getConfig.getConfiguration.setString("testConf", "1")

    assertEquals(null, env.getConfig.getGlobalJobParameters.toMap.get("testConf"))

    t.select('a).toAppendStream[Int]

    assertEquals("1", env.getConfig.getGlobalJobParameters.toMap.get("testConf"))
  }

  @Test
  def testMergeParametersInUnifiedTableEnvironment(): Unit = {
    val tEnv = TableEnvironment.create(
      EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build())

    val csvTable = CommonTestData.getCsvTableSource

    val tmpFile = File.createTempFile("flink-table-environment-test", ".tmp")
    tmpFile.deleteOnExit()
    tmpFile.delete()
    val path = tmpFile.toURI.toString
    println(path)

    tEnv.connect(new FileSystem().path(path))
      .withFormat(new OldCsv().field("id", "INT"))
      .withSchema(new Schema().field("id", "INT"))
      .inAppendMode()
      .registerTableSink("sink")

    tEnv.fromTableSource(csvTable).select('id).insertInto("sink")

    val env = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner
      .asInstanceOf[StreamPlanner].getExecutionEnvironment

    env.setParallelism(1)

    tEnv.getConfig.getConfiguration.setString("testConf", "1")

    assertEquals(null, env.getConfig.getGlobalJobParameters.toMap.get("testConf"))

    tEnv.execute("test")

    assertEquals("1", env.getConfig.getGlobalJobParameters.toMap.get("testConf"))
  }
}
