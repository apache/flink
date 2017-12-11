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

package org.apache.flink.table.runtime.batch

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util
import java.util.Collections

import org.apache.avro.util.Utf8
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.generated.{Address, Colors, Fixed16, User}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TableProgramsClusterTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Tests for interoperability with Avro types.
  */
@RunWith(classOf[Parameterized])
class AvroTypesITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsClusterTestBase(mode, configMode) {

  @Test
  def testAvroToRow(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = testData(env).toTable(tEnv)

    val result = t.select('*)

    val results = result.toDataSet[Row].collect()
    val expected = "black,null,Whatever,[true],[hello],true,0.0,GREEN," +
      "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],42,{},null,null,null,null\n" +
      "blue,null,Charlie,[],[],false,1.337,RED," +
      "null,1337,{},{\"num\": 42, \"street\": \"Bakerstreet\", \"city\": \"Berlin\", " +
      "\"state\": \"Berlin\", \"zip\": \"12049\"},null,null,null\n" +
      "yellow,null,Terminator,[false],[world],false,0.0,GREEN," +
      "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],1,{},null,null,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAvroStringAccess(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = testData(env).toTable(tEnv)

    val result = t.select('name)
    val results = result.toDataSet[Utf8].collect()
    val expected = "Charlie\n" +
      "Terminator\n" +
      "Whatever"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAvroObjectAccess(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = testData(env).toTable(tEnv)

    val result = t
      .filter('type_nested.isNotNull)
      .select('type_nested.flatten()).as('city, 'num, 'state, 'street, 'zip)

    val results = result.toDataSet[Address].collect()
    val expected = AvroTypesITCase.USER_1.getTypeNested.toString
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAvroToAvro(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = testData(env).toTable(tEnv)

    val result = t.select('*).toDataSet[User].collect()
    val expected = AvroTypesITCase.USER_1 + "\n" +
      AvroTypesITCase.USER_2 + "\n" +
      AvroTypesITCase.USER_3
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  private def testData(env: ExecutionEnvironment): DataSet[User] = {

    val data = new mutable.MutableList[User]

    data.+=(AvroTypesITCase.USER_1)
    data.+=(AvroTypesITCase.USER_2)
    data.+=(AvroTypesITCase.USER_3)

    env.fromCollection(data)
  }
}

object AvroTypesITCase {

  val USER_1: User = User.newBuilder()
    .setName("Charlie")
    .setFavoriteColor("blue")
    .setFavoriteNumber(null)
    .setTypeBoolTest(false)
    .setTypeDoubleTest(1.337d)
    .setTypeNullTest(null)
    .setTypeLongTest(1337L)
    .setTypeArrayString(new util.ArrayList[CharSequence])
    .setTypeArrayBoolean(new util.ArrayList[JBoolean]())
    .setTypeNullableArray(null)
    .setTypeEnum(Colors.RED)
    .setTypeMap(new util.HashMap[CharSequence, JLong])
    .setTypeFixed(null)
    .setTypeUnion(null)
    .setTypeNested(
      Address.newBuilder
      .setNum(42)
      .setStreet("Bakerstreet")
      .setCity("Berlin")
      .setState("Berlin")
      .setZip("12049")
      .build)
    .build

  val USER_2: User = User.newBuilder()
    .setName("Whatever")
    .setFavoriteNumber(null)
    .setFavoriteColor("black")
    .setTypeLongTest(42L)
    .setTypeDoubleTest(0.0)
    .setTypeNullTest(null)
    .setTypeBoolTest(true)
    .setTypeArrayString(Collections.singletonList("hello"))
    .setTypeArrayBoolean(Collections.singletonList(true))
    .setTypeEnum(Colors.GREEN)
    .setTypeMap(new util.HashMap[CharSequence, JLong])
    .setTypeFixed(new Fixed16())
    .setTypeUnion(null)
    .setTypeNested(null)
    .build()

  val USER_3: User = User.newBuilder()
    .setName("Terminator")
    .setFavoriteNumber(null)
    .setFavoriteColor("yellow")
    .setTypeLongTest(1L)
    .setTypeDoubleTest(0.0)
    .setTypeNullTest(null)
    .setTypeBoolTest(false)
    .setTypeArrayString(Collections.singletonList("world"))
    .setTypeArrayBoolean(Collections.singletonList(false))
    .setTypeEnum(Colors.GREEN)
    .setTypeMap(new util.HashMap[CharSequence, JLong])
    .setTypeFixed(new Fixed16())
    .setTypeUnion(null)
    .setTypeNested(null)
    .build()
}
