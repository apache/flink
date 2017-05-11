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

package org.apache.flink.table.api.scala.batch.sql

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.types.Row
import org.junit.Test
import org.mockito.Mockito.{mock, when}

class ComplexSchemaTest extends TableTestBase {

  @Test
  def testBatchTableSchema(): Unit = {
    val util = batchTestUtil()
    val ds = mock(classOf[DataSet[Row]])
    val jDs = mock(classOf[JDataSet[Row]])
    when(ds.javaSet).thenReturn(jDs)

    val nestedInfo : TypeInformation[_] = new RowTypeInfo(
      Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO),
      Array("bar")
    )
    val typeInfo = new RowTypeInfo(Array[TypeInformation[_]](nestedInfo), Array("foo"))

    when(jDs.getType).thenReturn(typeInfo)
    val t = ds.toTable(util.tEnv)
    util.tEnv.registerTable("MyTable", t)

    val sqlQuery =
      "SELECT foo.bar FROM MyTable"

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "foo.bar AS bar")
    )

    util.verifySql(sqlQuery, expected)
  }
}
