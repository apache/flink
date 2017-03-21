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

package org.apache.flink.table.api.scala.batch

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.utils.{BatchTableTestUtil, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit._
import org.mockito.Mockito.{mock, when}

class TableEnvironmentTest() extends TableTestBase {

  @Test
  def testSimpleRegister(): Unit = {

    val tableName = "MyTable"
    val util = batchTestUtil()
    val tEnv = util.tEnv

    val ds = getMockDS[(Int, Long, String)]
    tEnv.registerDataSet(tableName, ds)
    val t = tEnv.scan(tableName).select('_1, '_2, '_3)

    val expected = values(
      "DataSetScan",
      term("table", "[MyTable]")
    )

    util.verifyTable(t, expected)
  }

  @Test
  def testRegisterWithFields(): Unit = {

    val tableName = "MyTable"
    val util = batchTestUtil()
    val tEnv = util.tEnv

    val ds = getMockDS[(Int, Long, String)]
    tEnv.registerDataSet(tableName, ds, 'a, 'b, 'c)
    val t = tEnv.scan(tableName).select('a, 'b)

    val expected = unaryNode(
      "DataSetCalc",
      values("DataSetScan", term("table", "[MyTable]")),
      term("select", "a", "b")
    )

    util.verifyTable(t, expected)
  }

  @Test(expected = classOf[TableException])
  def testRegisterExistingDataSet(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    val ds1 = getMockDS[(Int, Long, String)]
    tEnv.registerDataSet("MyTable", ds1)
    val ds2 = getMockDS[(Int, Long, Int, String, Long)]
    // Must fail. Name is already in use.
    tEnv.registerDataSet("MyTable", ds2)
  }

  @Test(expected = classOf[TableException])
  def testScanUnregisteredTable(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv
    // Must fail. No table registered under that name.
    tEnv.scan("someTable")
  }

  @Test
  def testTableRegister(): Unit = {

    val tableName = "MyTable"
    val util = batchTestUtil()
    val tEnv = util.tEnv

    val t = getMockDS[(Int, Long, String)].toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable(tableName, t)

    val regT = tEnv.scan(tableName).select('a, 'b).filter('a > 8)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a", "b"),
      term("where", ">(a, 8)")
    )

    util.verifyTable(regT, expected)
  }

  @Test(expected = classOf[TableException])
  def testRegisterExistingTable(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    val t1 = getMockDS[(Int, Long, String)].toTable(tEnv)
    tEnv.registerTable("MyTable", t1)
    val t2 = getMockDS[(Int, Long, Int, String, Long)].toTable(tEnv)
    // Must fail. Name is already in use.
    tEnv.registerTable("MyTable", t2)
  }

  @Test(expected = classOf[TableException])
  def testRegisterTableFromOtherEnv(): Unit = {
    val util = batchTestUtil()
    val util2 = batchTestUtil()
    val tEnv1 = util.tEnv
    val tEnv2 = util2.tEnv

    val t1 = getMockDS[(Int, Long, String)].toTable(tEnv1)
    // Must fail. Table is bound to different TableEnvironment.
    tEnv2.registerTable("MyTable", t1)
  }

  @Test
  def testToTable(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    val t = getMockDS[(Int, Long, String)]
      .toTable(tEnv, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val expected = batchTableNode(0)

    util.verifyTable(t, expected)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithToFewFields(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    getMockDS[(Int, Long, String)]
      // Must fail. Number of fields does not match.
      .toTable(tEnv, 'a, 'b)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithToManyFields(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    getMockDS[(Int, Long, String)]
      // Must fail. Number of fields does not match.
      .toTable(tEnv, 'a, 'b, 'c, 'd)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithAmbiguousFields(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    getMockDS[(Int, Long, String)]
      // Must fail. Field names not unique.
      .toTable(tEnv, 'a, 'b, 'b)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithNonFieldReference1(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    // Must fail. as() can only have field references
    getMockDS[(Int, Long, String)]
      .toTable(tEnv, 'a + 1, 'b, 'c)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithNonFieldReference2(): Unit = {
    val util = batchTestUtil()
    val tEnv = util.tEnv

    // Must fail. as() can only have field references
    getMockDS[(Int, Long, String)]
      .toTable(tEnv, 'a as 'foo, 'b, 'c)
  }

  def getMockDS[T: TypeInformation]: DataSet[T] = {
    val ds = mock(classOf[DataSet[T]])
    val jDs = mock(classOf[JDataSet[T]])
    when(ds.javaSet).thenReturn(jDs)
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    when(jDs.getType).thenReturn(typeInfo)
    ds
  }
}

case class SomeCaseClass(name: String, age: Int, salary: Double, department: String) {
  def this() { this("", 0, 0.0, "") }
}
