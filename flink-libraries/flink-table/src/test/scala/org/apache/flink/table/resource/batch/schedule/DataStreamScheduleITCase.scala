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

package org.apache.flink.table.resource.batch.schedule

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfigOptions, TableEnvironment}

import org.junit.{After, Before, Test}

import java.io.File
import java.nio.file.Files

/**
  * ITCase for schedule DataStreamSource sql.
  */
class DataStreamScheduleITCase {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment;
  private val tEnv = TableEnvironment.getBatchTableEnvironment(env)
  private val file = File.createTempFile("csvTest", ".csv")

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    file.deleteOnExit()
    Files.write(file.toPath, "test".getBytes("UTF-8"))
  }

  @After
  def after(): Unit = {
    if (file.exists()) {
      file.delete()
    }
  }

  @Test
  def testJoin(): Unit = {
    env.setParallelism(10)
    val bankText = env.readTextFile(file.getAbsolutePath)
    val bank = bankText.map(new MapFunction[String, String] {
      override def map(value: String) = value
    })

   tEnv.registerBoundedStream("bank", bank, "age")
   tEnv.sqlQuery("select count(1) from bank").collect()
  }
}
