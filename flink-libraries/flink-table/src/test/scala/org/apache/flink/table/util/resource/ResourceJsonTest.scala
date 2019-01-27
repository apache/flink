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

package org.apache.flink.table.util.resource

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.resource.batch.BatchExecResourceTest
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.TestData.{nullablesOfSmallData3, smallData3, type3}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.util.FileUtils

import org.junit.{Assert, Before, Test}

import java.io.File

class ResourceJsonTest extends BatchTestBase {

  @Before
  def before(): Unit = {
    registerCollection("SmallTable3", smallData3, type3, nullablesOfSmallData3, 'a, 'b, 'c)
  }

  @Test
  def testGenerateJson(): Unit = {
    BatchExecResourceTest.setResourceConfig(tEnv.getConfig)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_DIRECT_MEM, 0)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_DIRECT_MEM, 0)
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
    val table = tEnv.sqlQuery(sqlQuery)
    val tmpFile = new File("/tmp/test")
    val sink = new CsvTableSink(tmpFile.getPath, "|")
    table.writeToSink(sink)
    val streamGraph = tEnv.generateStreamGraph()
    val property = StreamGraphPropertyGenerator.generateProperties(streamGraph)
    val expectedJson = FileUtils.readFileUtf8(
      new File(getClass.getClassLoader.getResource("test-resource/resource.json").toURI)).trim
    val json = property.toString
    Assert.assertEquals(expectedJson, json)
    val adjustJson = FileUtils.readFileUtf8(new File(
      getClass.getClassLoader.getResource("test-resource/resource-adjust.json").toURI)).trim
    StreamGraphConfigurer.configure(streamGraph, StreamGraphProperty.fromJson(adjustJson))
    val resultProperty = StreamGraphPropertyGenerator.generateProperties(streamGraph)
    val resultJson = resultProperty.toString
    val expectedResultJson = FileUtils.readFileUtf8(new File(
      getClass.getClassLoader.getResource("test-resource/resource-result.json").toURI)).trim
    Assert.assertEquals(expectedResultJson, resultJson)
  }
}
