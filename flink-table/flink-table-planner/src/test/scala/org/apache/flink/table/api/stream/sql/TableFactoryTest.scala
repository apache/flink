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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectIdentifier}
import org.apache.flink.table.factories.TableFactory
import org.apache.flink.table.utils.{TableTestBase, TestContextTableFactory}

import org.junit.{Assert, Test}

import java.util.Optional

class TableFactoryTest extends TableTestBase {

  private val util = streamTestUtil()

  @Test
  def testTableSourceSinkFactory(): Unit = {
    val factory = new TestContextTableFactory(
      ObjectIdentifier.of("cat", "default", "t1"),
      ObjectIdentifier.of("cat", "default", "t2"))
    util.tableEnv.getConfig.getConfiguration.setBoolean(TestContextTableFactory.REQUIRED_KEY, true)
    util.tableEnv.registerCatalog("cat", new GenericInMemoryCatalog("default") {
      override def getTableFactory: Optional[TableFactory] = Optional.of(factory)
    })
    util.tableEnv.useCatalog("cat")

    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.c from t1
      """.stripMargin
    util.tableEnv.sqlUpdate(sourceDDL)
    util.tableEnv.sqlUpdate(sinkDDL)
    util.tableEnv.sqlUpdate(query)
    // trigger translating
    util.tableEnv.execute("job name")
    Assert.assertTrue(factory.hasInvokedSource)
    Assert.assertTrue(factory.hasInvokedSink)
  }
}
