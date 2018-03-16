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

import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.CompositeTypeTestBase
import org.junit.Test

class CompositeAccessTest extends CompositeTypeTestBase {

  @Test
  def testGetField(): Unit = {

    // single field by string key
    testAllApis(
      'f0.get("intField"),
      "f0.get('intField')",
      "testTable.f0.intField",
      "42")
    testSqlApi("f0.intField", "42")

    testSqlApi("testTable.f0.stringField", "Bob")
    testSqlApi("f0.stringField", "Bob")

    testSqlApi("testTable.f0.booleanField", "true")
    testSqlApi("f0.booleanField", "true")

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
    testSqlApi("f1.objectField.intField", "25")

    testSqlApi("testTable.f1.objectField.stringField", "Timo")
    testSqlApi("f1.objectField.stringField", "Timo")

    testSqlApi("testTable.f1.objectField.booleanField", "false")
    testSqlApi("f1.objectField.booleanField", "false")

    testAllApis(
      'f2.get(0),
      "f2.get(0)",
      "testTable.f2._1",
      "a")
    testSqlApi("f2._1", "a")

    testSqlApi("testTable.f3.f1", "b")
    testSqlApi("f3.f1", "b")

    testSqlApi("testTable.f4.myString", "Hello")
    testSqlApi("f4.myString", "Hello")

    testSqlApi("testTable.f5", "13")
    testSqlApi("f5", "13")

    testAllApis(
      'f7.get("_1"),
      "get(f7, '_1')",
      "testTable.f7._1",
      "true")

    // composite field return type
    testSqlApi("testTable.f6", "MyCaseClass2(null)")
    testSqlApi("f6", "MyCaseClass2(null)")

    testAllApis(
      'f1.get("objectField"),
      "f1.get('objectField')",
      "testTable.f1.objectField",
      "MyCaseClass(25,Timo,false)")
    testSqlApi("f1.objectField", "MyCaseClass(25,Timo,false)")

    testAllApis(
      'f0,
      "f0",
      "testTable.f0",
      "MyCaseClass(42,Bob,true)")
    testSqlApi("f0", "MyCaseClass(42,Bob,true)")

    // flattening (test base only returns first column)
    testAllApis(
      'f1.get("objectField").flatten(),
      "f1.get('objectField').flatten()",
      "testTable.f1.objectField.*",
      "25")
    testSqlApi("f1.objectField.*", "25")

    testAllApis(
      'f0.flatten(),
      "flatten(f0)",
      "testTable.f0.*",
      "42")
    testSqlApi("f0.*", "42")

    testTableApi(12.flatten(), "12.flatten()", "12")

    testTableApi('f5.flatten(), "f5.flatten()", "13")

    // array of composites
    testAllApis(
      'f8.at(1).get("_1"),
      "f8.at(1).get('_1')",
      "f8[1]._1",
      "true"
    )

    testAllApis(
      'f8.at(1).get("_2"),
      "f8.at(1).get('_2')",
      "f8[1]._2",
      "23"
    )

    testAllApis(
      'f9.at(2).get("_1"),
      "f9.at(2).get('_1')",
      "f9[2]._1",
      "null"
    )

    testAllApis(
      'f10.at(1).get("stringField"),
      "f10.at(1).get('stringField')",
      "f10[1].stringField",
      "Bob"
    )

    testAllApis(
      'f11.at(1).get("myString"),
      "f11.at(1).get('myString')",
      "f11[1].myString",
      "Hello"
    )

    testAllApis(
      'f12.at(1).get("arrayField").at(1).get("stringField"),
      "f12.at(1).get('arrayField').at(1).get('stringField')",
      "f12[1].arrayField[1].stringField",
      "Alice"
    )
  }
}


