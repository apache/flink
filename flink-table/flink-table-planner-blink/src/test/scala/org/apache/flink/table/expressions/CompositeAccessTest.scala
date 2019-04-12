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

import org.apache.flink.table.expressions.utils.CompositeTypeTestBase
import org.junit.Test

class CompositeAccessTest extends CompositeTypeTestBase {

  @Test
  def testGetField(): Unit = {

    // single field by string key
    testSqlApi(
      "testTable.f0.intField",
      "42")
    testSqlApi("f0.intField", "42")

    testSqlApi("testTable.f0.stringField", "Bob")
    testSqlApi("f0.stringField", "Bob")

    testSqlApi("testTable.f0.booleanField", "true")
    testSqlApi("f0.booleanField", "true")

    // nested single field
    testSqlApi(
      "testTable.f1.objectField.intField",
      "25")
    testSqlApi("f1.objectField.intField", "25")

    testSqlApi("testTable.f1.objectField.stringField", "Timo")
    testSqlApi("f1.objectField.stringField", "Timo")

    testSqlApi("testTable.f1.objectField.booleanField", "false")
    testSqlApi("f1.objectField.booleanField", "false")

    testSqlApi(
      "testTable.f2._1",
      "a")
    testSqlApi("f2._1", "a")

    testSqlApi("testTable.f3.f1", "b")
    testSqlApi("f3.f1", "b")

    testSqlApi("testTable.f4.myString", "Hello")
    testSqlApi("f4.myString", "Hello")

    testSqlApi("testTable.f5", "13")
    testSqlApi("f5", "13")

    testSqlApi(
      "testTable.f7._1",
      "true")

    // composite field return type
    testSqlApi("testTable.f6", "MyCaseClass2(null)")
    testSqlApi("f6", "MyCaseClass2(null)")

    // MyCaseClass is converted to BaseRow
    // so the result of "toString" does'nt contain MyCaseClass prefix
    testSqlApi(
      "testTable.f1.objectField",
      "(25,Timo,false)")
    testSqlApi("f1.objectField", "(25,Timo,false)")

    testSqlApi(
      "testTable.f0",
      "(42,Bob,true)")
    testSqlApi("f0", "(42,Bob,true)")

    // flattening (test base only returns first column)
    testSqlApi(
      "testTable.f1.objectField.*",
      "25")
    testSqlApi("f1.objectField.*", "25")

    testSqlApi(
      "testTable.f0.*",
      "42")
    testSqlApi("f0.*", "42")

    // array of composites
    testSqlApi(
      "f8[1]._1",
      "true"
    )

    testSqlApi(
      "f8[1]._2",
      "23"
    )

    testSqlApi(
      "f9[2]._1",
      "null"
    )

    testSqlApi(
      "f10[1].stringField",
      "Bob"
    )

    testSqlApi(
      "f11[1].myString",
      "Hello"
    )

    testSqlApi(
      "f11[2]",
      "null"
    )

    testSqlApi(
      "f12[1].arrayField[1].stringField",
      "Alice"
    )

    testSqlApi(
      "f13[1].objectField.stringField",
      "Bob"
    )
  }
}


