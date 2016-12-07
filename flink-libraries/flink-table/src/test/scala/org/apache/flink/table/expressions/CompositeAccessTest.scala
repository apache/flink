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
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.types.Row
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.CompositeAccessTest.{MyCaseClass, MyCaseClass2, MyPojo}
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.junit.Test


class CompositeAccessTest extends ExpressionTestBase {

  @Test
  def testGetField(): Unit = {

    // single field by string key
    testAllApis(
      'f0.get("intField"),
      "f0.get('intField')",
      "testTable.f0.intField",
      "42")

    testSqlApi("testTable.f0.stringField", "Bob")

    testSqlApi("testTable.f0.booleanField", "true")

    // single field by int key
    testTableApi(
      'f0.get(0),
      "f0.get(0)",
      "42")

    // nested single field
    testAllApis(
      'f1.get("objectField").get("intField"),
      "f1.get('objectField').get('intField')",
      "testTable.f1.objectField.intField",
      "25")

    testSqlApi("testTable.f1.objectField.stringField", "Timo")

    testSqlApi("testTable.f1.objectField.booleanField", "false")

    testAllApis(
      'f2.get(0),
      "f2.get(0)",
      "testTable.f2._1",
      "a")

    testSqlApi("testTable.f3.f1", "b")

    testSqlApi("testTable.f4.myString", "Hello")

    testSqlApi("testTable.f5", "13")

    testAllApis(
      'f7.get("_1"),
      "get(f7, '_1')",
      "testTable.f7._1",
      "true")

    // composite field return type
    testSqlApi("testTable.f6", "MyCaseClass2(null)")

    testAllApis(
      'f1.get("objectField"),
      "f1.get('objectField')",
      "testTable.f1.objectField",
      "MyCaseClass(25,Timo,false)")

    testAllApis(
      'f0,
      "f0",
      "testTable.f0",
      "MyCaseClass(42,Bob,true)")

    // flattening (test base only returns first column)
    testAllApis(
      'f1.get("objectField").flatten(),
      "f1.get('objectField').flatten()",
      "testTable.f1.objectField.*",
      "25")

    testAllApis(
      'f0.flatten(),
      "flatten(f0)",
      "testTable.f0.*",
      "42")

    testTableApi(12.flatten(), "12.flatten()", "12")

    testTableApi('f5.flatten(), "f5.flatten()", "13")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongSqlField(): Unit = {
    testSqlApi("testTable.f5.test", "13")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongIntKeyField(): Unit = {
    testTableApi('f0.get(555), "'fail'", "fail")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongIntKeyField2(): Unit = {
    testTableApi("fail", "f0.get(555)", "fail")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongStringKeyField(): Unit = {
    testTableApi('f0.get("fghj"), "'fail'", "fail")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongStringKeyField2(): Unit = {
    testTableApi("fail", "f0.get('fghj')", "fail")
  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = new Row(8)
    testData.setField(0, MyCaseClass(42, "Bob", booleanField = true))
    testData.setField(1, MyCaseClass2(MyCaseClass(25, "Timo", booleanField = false)))
    testData.setField(2, ("a", "b"))
    testData.setField(3, new org.apache.flink.api.java.tuple.Tuple2[String, String]("a", "b"))
    testData.setField(4, new MyPojo())
    testData.setField(5, 13)
    testData.setField(6, MyCaseClass2(null))
    testData.setField(7, Tuple1(true))
    testData
  }

  def typeInfo = {
    new RowTypeInfo(
      createTypeInformation[MyCaseClass],
      createTypeInformation[MyCaseClass2],
      createTypeInformation[(String, String)],
      new TupleTypeInfo(Types.STRING, Types.STRING),
      TypeExtractor.createTypeInfo(classOf[MyPojo]),
      Types.INT,
      createTypeInformation[MyCaseClass2],
      createTypeInformation[Tuple1[Boolean]]
      ).asInstanceOf[TypeInformation[Any]]
  }

}

object CompositeAccessTest {
  case class MyCaseClass(intField: Int, stringField: String, booleanField: Boolean)

  case class MyCaseClass2(objectField: MyCaseClass)

  class MyPojo {
    private var myInt: Int = 0
    private var myString: String = "Hello"

    def getMyInt = myInt

    def setMyInt(value: Int) = {
      myInt = value
    }

    def getMyString = myString

    def setMyString(value: String) = {
      myString = myString
    }
  }
}
