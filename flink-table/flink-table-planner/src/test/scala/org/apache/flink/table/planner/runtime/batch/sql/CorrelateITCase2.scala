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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.runtime.utils.TestData._

import org.junit.jupiter.api.{BeforeEach, Test}

class CorrelateITCase2 extends BatchTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    registerCollection("inputT", TableFunctionITCase.testData, type3, "a, b, c")
    registerCollection("inputTWithNull", TableFunctionITCase.testDataWithNull, type3, "a, b, c")
    registerCollection("SmallTable3", smallData3, type3, "a, b, c")
  }

  @Test
  def testJavaGenericTableFunc(): Unit = {
    registerFunction("func0", new GenericTableFunc[Integer](Types.INT))
    registerFunction("func1", new GenericTableFunc[String](Types.STRING))
    testGenericTableFunc()
  }

  @Test
  def testScalaGenericTableFunc(): Unit = {
    registerFunction("func0", new GenericTableFunc[Integer](Types.INT))
    registerFunction("func1", new GenericTableFunc[String](Types.STRING))
    testGenericTableFunc()
  }

  def testGenericTableFunc(): Unit = {
    checkResult(
      "select a, s from inputT, LATERAL TABLE(func0(a)) as T(s)",
      Seq(
        row(1, 1),
        row(2, 2),
        row(3, 3),
        row(4, 4)
      ))

    checkResult(
      "select a, s from inputT, LATERAL TABLE(func1(a)) as T(s)",
      Seq(
        row(1, 1),
        row(2, 2),
        row(3, 3),
        row(4, 4)
      ))
  }

  @Test
  def testConstantTableFunc(): Unit = {
    registerFunction("str_split", new StringSplit())
    checkResult(
      "SELECT * FROM LATERAL TABLE(str_split()) as T0(d)",
      Seq(row("a"), row("b"), row("c"))
    )

    checkResult(
      "SELECT * FROM LATERAL TABLE(str_split('Jack,John', ',')) as T0(d)",
      Seq(row("Jack"), row("John"))
    )
  }

  @Test
  def testConstantTableFunc2(): Unit = {
    registerFunction("str_split", new StringSplit())

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split()) AS T0(d)",
      Seq(
        row("Jack#22", "a"),
        row("Jack#22", "b"),
        row("Jack#22", "c"),
        row("John#19", "a"),
        row("John#19", "b"),
        row("John#19", "c"),
        row("Anna#44", "a"),
        row("Anna#44", "b"),
        row("Anna#44", "c"),
        row("nosharp", "a"),
        row("nosharp", "b"),
        row("nosharp", "c")
      )
    )

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split('Jack,John', ',')) AS T0(d)",
      Seq(
        row("Jack#22", "Jack"),
        row("Jack#22", "John"),
        row("John#19", "Jack"),
        row("John#19", "John"),
        row("Anna#44", "Jack"),
        row("Anna#44", "John"),
        row("nosharp", "Jack"),
        row("nosharp", "John")
      )
    )

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split('Jack,John', ',')) AS T0(d) " +
        "WHERE d = 'Jack'",
      Seq(
        row("Jack#22", "Jack"),
        row("John#19", "Jack"),
        row("Anna#44", "Jack"),
        row("nosharp", "Jack")
      )
    )
  }

  @Test
  def testConstantTableFunc3(): Unit = {
    registerFunction("str_split", new StringSplit())

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split('Jack,John', ',', 1)) AS T0(d) " +
        "WHERE SUBSTRING(c, 1, 4) = d",
      Seq(row("John#19", "John"))
    )
  }

  @Test
  def testConstantTableFuncWithSubString(): Unit = {
    registerFunction("str_split", new StringSplit())
    checkResult(
      "SELECT * FROM " +
        "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 2, 4), ',')) as T1(s), " +
        "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x)",
      Seq(row("b", "a"), row("b", "b"), row("b", "c"), row("c", "a"), row("c", "b"), row("c", "c"))
    )
  }

// TODO support dyn
//  @Test
//  def testConstantTableFuncWithDyn(): Unit = {
//    registerFunction("str_split", new StringSplit())
//    registerFunction("funcDyn1", new UDTFWithDynamicType0)
//    registerFunction("funcDyn2", new UDTFWithDynamicType0)
//    checkResult(
//      "SELECT * FROM " +
//          "LATERAL TABLE(funcDyn1('test#Hello world#Hi', 'string,int,int')) AS" +
//          " T1(name0,len0,len1)," +
//          "LATERAL TABLE(funcDyn2('abc#defijk', 'string,int')) AS T2(name1,len2)" +
//          "WHERE len0 < 5 AND len2 < 4",
//      Seq(row("Hi", 2, 2, "abc", 3), row("test", 4, 4, "abc", 3))
//    )
//    checkResult(
//      "SELECT c, name0, len0, len1, name1, len2 FROM " +
//          "LATERAL TABLE(funcDyn1('test#Hello world#Hi', 'string,int,int')) AS" +
//          " T1(name0,len0,len1)," +
//          "LATERAL TABLE(funcDyn2('abc#defijk', 'string,int')) AS T2(name1,len2), " +
//          "inputT WHERE a > 2 AND len1 < 5 AND len2 < 4",
//      Seq(row("Anna#44", "Hi", 2, 2, "abc", 3),
//        row("Anna#44", "test", 4, 4, "abc", 3),
//        row("nosharp", "Hi", 2, 2, "abc", 3),
//        row("nosharp", "test", 4, 4, "abc", 3)
//      )
//    )
//  }

  /** Test binaryString => string => binaryString => string => binaryString. */
  @Test
  def testUdfAfterUdtf(): Unit = {

    registerFunction("str_split", new StringSplit())
    registerFunction("func", StringUdFunc)

    checkResult(
      "select func(s) from inputT, LATERAL TABLE(str_split(c, '#')) as T(s)",
      Seq(row("Anna"), row("Jack"), row("John"), row("nosharp"), row("19"), row("22"), row("44"))
    )
  }

  @Test
  def testLeftInputAllProjectWithEmptyOutput(): Unit = {

    registerFunction("str_split", new StringSplit())

    checkResult(
      "select s from inputTWithNull, LATERAL TABLE(str_split(c, '#')) as T(s)",
      Seq(row("Jack"), row("nosharp"), row("22")))
  }

  @Test
  def testLeftJoinLeftInputAllProjectWithEmptyOutput(): Unit = {

    registerFunction("str_split", new StringSplit())

    checkResult(
      "select s from inputTWithNull left join LATERAL TABLE(str_split(c, '#')) as T(s) ON TRUE",
      Seq(row("Jack"), row("nosharp"), row("22"), row(null), row(null))
    )
  }

  @Test
  def testLeftInputPartialProjectWithEmptyOutput(): Unit = {

    registerFunction("str_split", new StringSplit())

    checkResult(
      "select a, s from inputTWithNull, LATERAL TABLE(str_split(c, '#')) as T(s)",
      Seq(row(1, "Jack"), row(4, "nosharp"), row(1, "22")))
  }

  @Test
  def testLeftJoinLeftInputPartialProjectWithEmptyOutput(): Unit = {

    registerFunction("str_split", new StringSplit())

    checkResult(
      "select b, s from inputTWithNull left join LATERAL TABLE(str_split(c, '#')) as T(s) ON TRUE",
      Seq(row(1, "Jack"), row(3, "nosharp"), row(1, "22"), row(2, null), row(2, null))
    )
  }

  @Test
  def testLeftInputPartialProjectWithEmptyOutput2(): Unit = {

    registerFunction("toPojo", new MyToPojoTableFunc())
    registerFunction("func", StringUdFunc)

    checkResult(
      "select a, s2 from inputTWithNull, LATERAL TABLE(toPojo(a)) as T(s1,s2)" +
        " where a + s1 > 2",
      Seq(row(2, 2), row(3, 3), row(4, 4)))
  }

  @Test
  def testLeftJoinLeftInputPartialProjectWithEmptyOutput2(): Unit = {
    registerFunction("toPojo", new MyToPojoTableFunc())
    registerFunction("func", StringUdFunc)

    checkResult(
      "select b, s1 from inputTWithNull left join LATERAL TABLE(toPojo(a)) as T(s1,s2) ON TRUE" +
        " where a + s1 > 2",
      Seq(row(2, 2), row(2, 3), row(3, 4)))
  }

  @Test
  def testTableFunctionWithBinaryString(): Unit = {
    registerFunction("func", new BinaryStringTableFunc)
    checkResult(
      "select c, s1, s2 from inputT, LATERAL TABLE(func(c, 'haha')) as T(s1, s2)",
      Seq(
        row("Jack#22", "Jack#22", "haha"),
        row("John#19", "John#19", "haha"),
        row("nosharp", "nosharp", "haha"),
        row("Anna#44", "Anna#44", "haha")
      )
    )
  }
}
