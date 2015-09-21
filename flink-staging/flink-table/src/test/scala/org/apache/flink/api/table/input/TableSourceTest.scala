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

package org.apache.flink.api.table.input

import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.java.{ExecutionEnvironment => JavaEnv}
import org.apache.flink.api.java.table.JavaBatchTranslator
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Table
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.input.TableSourceTest.MyResult
import org.apache.flink.api.table.parser.ExpressionParser
import org.junit.Assert.assertEquals
import org.junit.Test

class TableSourceTest {

  @Test
  def testResolvedFieldPushDown() = {
    val tableSource1 = new DummyTableSourceWithPushdown
    getTableJava(tableSource1)
      .filter("a===10")
    assertEquals(Set(("a", true)), tableSource1.resolvedFields)

    val tableSource2 = new DummyTableSourceWithPushdown
    getTableJava(tableSource2)
      .filter("a===b")
    assertEquals(Set(("a", true), ("b", true)), tableSource2.resolvedFields)

    val tableSource3 = new DummyTableSourceWithPushdown
    getTableJava(tableSource3)
      .select("a, b")
    assertEquals(Set(("a", false), ("b", false)), tableSource3.resolvedFields)

    val tableSource4 = new DummyTableSourceWithPushdown
    getTableJava(tableSource4)
      .filter("a===b")
      .select("a")
    assertEquals(Set(("a", true), ("b", true), ("a", false)), tableSource4.resolvedFields)

    val tableSource5 = new DummyTableSourceWithPushdown
    getTableJava(tableSource5)
      .filter("a===b")
      .select("b")
    assertEquals(Set(("a", true), ("b", true), ("b", false)), tableSource5.resolvedFields)

    val tableSource6 = new DummyTableSourceWithPushdown
    val tableSource7 = new DummyTableSourceWithPushdown
    val t6 = getTableJava(tableSource6)
      .filter("a===b")
      .select("a as a6")
    val t7 = getTableJava(tableSource7)
      .filter("a===b")
      .select("a as a7")
    t6.join(t7).where("a6===a7");
    assertEquals(Set(("a", true), ("b", true), ("a", false)), tableSource6.resolvedFields)
    assertEquals(Set(("a", true), ("b", true), ("a", false)), tableSource7.resolvedFields)

    val tableSource8 = new DummyTableSourceWithPushdown
    getTableJava(tableSource8)
      .select("c.substring(0,2) as X")
    assertEquals(Set(("c", false)), tableSource8.resolvedFields)

    val tableSource9 = new DummyTableSourceWithPushdown
    val tableSource10 = new DummyTableSourceWithPushdown
    val t9 = getTableJava(tableSource9)
      .select("a as a9, b as b9, c as c9");
    getTableJava(tableSource10)
      .join(t9)
      .where("a9===a")
      .groupBy("b")
    assertEquals(Set(("a", true), ("a", false), ("b", false), ("c", false)),
      tableSource9.resolvedFields)
    assertEquals(Set(("a", true), ("b", false)), tableSource10.resolvedFields)

    val tableSource11 = new DummyTableSourceWithPushdown
    val tableSource12 = new DummyTableSourceWithPushdown
    getTableJava(tableSource11)
      .filter("a===5 || a===6")
      .select("a as a4, b as b4, c as c4")
      .filter("b4===7")
      .join(getTableJava(tableSource12))
      .where("a===a4 && c==='Test' && c4==='Test2'")
    assertEquals(Set(("a", true), ("a", false), ("b", true), ("b", false), ("c", false),
      ("c", true)), tableSource11.resolvedFields)
    assertEquals(Set(("a", true), ("c", true)), tableSource12.resolvedFields)
  }

  @Test
  def testPredicatePushDown() = {
    val tableSource1 = new DummyTableSourceWithPushdown
    getTableJava(tableSource1)
      .filter("a===10")
    comparePredicateList(List("a===10"), tableSource1)

    val tableSource2 = new DummyTableSourceWithPushdown
    getTableJava(tableSource2)
      .filter("a===10 && b===a")
    comparePredicateList(List("a===10"), tableSource2)

    val tableSource3 = new DummyTableSourceWithPushdown
    getTableJava(tableSource3)
      .filter("(a===10 || b===a) && b===15")
    comparePredicateList(List("b===15"), tableSource3)

    val tableSource4 = new DummyTableSourceWithPushdown
    val tableSource5 = new DummyTableSourceWithPushdown
    getTableJava(tableSource4)
      .filter("a===5 || a===6")
      .select("a as a4, b as b4, c as c4")
      .filter("b4===7")
      .join(getTableJava(tableSource5))
      .where("a===a4 && c==='Test' && c4==='Test2'")
    comparePredicateList(List("a===5 || a===6", "b===7", "c==='Test2'"), tableSource4)
    comparePredicateList(List("c==='Test'"), tableSource5)

    val tableSource6 = new DummyTableSourceWithPushdown
    val tableSource7 = new DummyTableSourceWithPushdown
    getTableJava(tableSource6)
      .filter("a===5 || a===6")
      .select("b, c")
      .as("X, Y")
      .filter("X===34")
      .join(getTableJava(tableSource7))
      .where("X===b && Y==='Test2' && c==='Test'")
    comparePredicateList(List("a===5 || a===6", "b===34", "c==='Test2'"), tableSource6)
    comparePredicateList(List("c==='Test'"), tableSource7)

    val tableSource8 = new DummyTableSourceWithPushdown
    getTableJava(tableSource8)
      .select("a, b")
      .groupBy("a")
      .select("a, b.avg as b")
      .filter("a===5 && b>6")
      .select("b, a.max as c")
      .filter("c<4")
    comparePredicateList(List("a===5"), tableSource8)
  }

  @Test
  def testStaticTableSource() = {
    val ds = getTableJava(new DummyTableSourceWithoutPushdown)
      .filter("field!=='D'")
      .toDataSet[MyResult]
      .collect()
    assertEquals(Seq(MyResult("A"), MyResult("B"), MyResult("C")), ds)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testScalaImplicitConversions(): Unit = {
    getTableJava(new DummyTableSourceWithPushdown())
        .toDataSet[MyResult]
  }

  // ----------------------------------------------------------------------------------------------

  def getTableJava(tableSource: TableSource): Table = {
    val translator = new JavaBatchTranslator(JavaEnv.getExecutionEnvironment)
    translator.createTable(tableSource)
  }

  def getTableScala(tableSource: TableSource): Table = {
    val translator = new JavaBatchTranslator(JavaEnv.getExecutionEnvironment)
    translator.createTable(tableSource)
  }

  def comparePredicateList(expected: List[String], ts: DummyTableSourceWithPushdown) = {
    val expectedExpr: List[Expression] = expected.map(ExpressionParser.parseExpression(_))
    val actual = ts.predicates.map( _.transformPost {
      case rfr@ResolvedFieldReference(fieldName, typeInfo) =>
        UnresolvedFieldReference(fieldName)
    })
    assertEquals(expectedExpr, actual)
  }
}

object TableSourceTest {
  case class MyResult(field: String)
}
