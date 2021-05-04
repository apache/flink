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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.TestData.smallData3
import org.apache.flink.table.planner.runtime.utils.{BatchAbstractTestBase, BatchTestBase}
import org.apache.flink.table.planner.utils.TableTestUtil

import org.junit.{Assert, Test}

class TableSinkITCase extends BatchTestBase {

  @Test
  def testTableHints(): Unit = {
    val dataId = TestValuesTableFactory.registerData(smallData3)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'true',
         |  'data-id' = '$dataId'
         |)
       """.stripMargin)

    tEnv.getConfig.getConfiguration.setBoolean(
      TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true)
    val resultPath = BatchAbstractTestBase.TEMPORARY_FOLDER.newFolder().getAbsolutePath
    tEnv.executeSql(
      s"""
         |CREATE TABLE MySink (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'filesystem',
         |  'format' = 'testcsv',
         |  'path' = '$resultPath'
         |)
       """.stripMargin)
    val stmtSet= tEnv.createStatementSet()
    val newPath1 =  BatchAbstractTestBase.TEMPORARY_FOLDER.newFolder().getAbsolutePath
    stmtSet.addInsertSql(
      s"insert into MySink /*+ OPTIONS('path' = '$newPath1') */ select * from MyTable")
    val newPath2 =  BatchAbstractTestBase.TEMPORARY_FOLDER.newFolder().getAbsolutePath
    stmtSet.addInsertSql(
      s"insert into MySink /*+ OPTIONS('path' = '$newPath2') */ select * from MyTable")
    stmtSet.execute().await()

    Assert.assertTrue(TableTestUtil.readFromFile(resultPath).isEmpty)
    val expected = Seq("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    val result1 = TableTestUtil.readFromFile(newPath1)
    Assert.assertEquals(expected.sorted, result1.sorted)
    val result2 = TableTestUtil.readFromFile(newPath2)
    Assert.assertEquals(expected.sorted, result2.sorted)
  }

}
