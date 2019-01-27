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
package org.apache.flink.table.util

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.plan.util.RelDigestWriterImpl
import org.apache.flink.table.runtime.utils.CommonTestData

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import scala.io.Source

class RelDigestWriterImplTest {

  @Before
  def before(): Unit = {
    RelDigestWriterImpl.nonDeterministicIdCounter.set(0)
  }

  @Test
  def testDynamicFunction(): Unit = {
    val conf = new TableConfig()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    tableEnv.registerTableSource("MyTable", CommonTestData.getCsvTableSource)
    val table = tableEnv.sqlQuery(
      """
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
      """.stripMargin)
    val rel = table.getRelNode
    val expected = readFromResource("testDynamicFunction.out")
    assertEquals(expected, RelDigestWriterImpl.getDigest(rel))
  }

  private def readFromResource(name: String): String = {
    val inputStream = getClass.getResource("/digest/" + name).getFile
    Source.fromFile(inputStream).mkString
  }

}
