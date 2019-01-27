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

package org.apache.flink.table.sources.csv

import java.io.File

import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.util.TableTestBase
import org.junit.{Assert, Test}
import org.powermock.reflect.Whitebox

class CsvTableSourceTest extends TableTestBase {

  private val util = batchTestUtil()
  private val csvSource = CsvTableSource.builder()
      .quoteCharacter('"')
      .path(createTmpCsvFile("test"))
      .field("id", DataTypes.INT)
      .field("first", DataTypes.STRING)
      .field("last", DataTypes.STRING)
      .field("score", DataTypes.STRING)
      .build()
  util.addTable("csvSource", csvSource)

  def createTmpCsvFile(content: String): String = {
    val file = File.createTempFile("csvTest", ".csv")
    file.deleteOnExit()
    val filePath = file.getAbsolutePath
    import java.nio.file._
    Files.write(Paths.get(filePath), content.getBytes("UTF-8"))
    filePath
  }

  @Test
  def testCsvTableSource(): Unit = {
    val sql = "SELECT score FROM csvSource"
    util.verifyPlan(sql)
  }

  @Test
  def testCsvTableSourceWithLimitPushdown(): Unit = {
    val sql = "SELECT * FROM csvSource limit 2"
    util.verifyPlan(sql)
  }

  @Test
  def testCsvTableSourceWithLimitPushdownAndProjectPushDown() = {
    val s1 = csvSource.projectFields(Array(1)).applyLimit(10).asInstanceOf[CsvTableSource]
    val s2 = csvSource.applyLimit(10).asInstanceOf[CsvTableSource].projectFields(Array(1))
    Assert.assertEquals(Whitebox.getInternalState(s1, "limit"), 10)
    Assert.assertEquals(Whitebox.getInternalState(s2, "limit"), 10)
    Assert.assertArrayEquals(Whitebox.getInternalState(s1, "selectedFields"), Array(1))
    Assert.assertArrayEquals(Whitebox.getInternalState(s2, "selectedFields"), Array(1))
  }
}
