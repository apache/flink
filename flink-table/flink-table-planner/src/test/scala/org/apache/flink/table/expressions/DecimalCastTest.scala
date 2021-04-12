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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row

import org.junit.Test

import scala.util.Random

class DecimalCastTest extends ExpressionTestBase {

  val rnd = new Random()

  @Test
  def testCastFromTinyInt(): Unit = {
    def tinyInt(i: Any) = s"CAST($i AS TINYINT)"

    testSqlApi(s"CAST(${tinyInt(null)} AS DECIMAL)", "null")

    testSqlApi(s"CAST(${tinyInt(0)} AS DECIMAL)", "0")
    testSqlApi(s"CAST(${tinyInt(12)} AS DECIMAL)", "12")
    testSqlApi(s"CAST(${tinyInt(-12)} AS DECIMAL)", "-12")
    testSqlApi(s"CAST(${tinyInt(Byte.MaxValue)} AS DECIMAL)", Byte.MaxValue.toString)
    testSqlApi(s"CAST(${tinyInt(Byte.MinValue)} AS DECIMAL)", Byte.MinValue.toString)

    val v = rnd.nextInt().toByte
    testSqlApi(s"CAST(${tinyInt(v)} AS DECIMAL)", v.toString)

    testSqlApi(s"CAST(${tinyInt(100)} AS DECIMAL(2, 0))", Byte.MinValue.toString)
  }

  override def testData: Any = new Row(0)

  override def typeInfo: TypeInformation[Any] =
    new RowTypeInfo().asInstanceOf[TypeInformation[Any]]
}
