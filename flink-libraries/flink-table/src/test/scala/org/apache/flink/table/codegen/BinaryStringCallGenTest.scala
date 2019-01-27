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

package org.apache.flink.table.codegen

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.codegen.CodeGenUtils.{compile, generateCallExpression, newName}
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_STRING
import org.apache.flink.table.functions.sql.ScalarSqlFunctions

import org.junit.Assert._
import org.junit.{Ignore, Test}

class BinaryStringCallGenTest {

  def invoke(operator: SqlOperator, operands: Seq[GeneratedExpression],
             resultType: TypeInformation[_] = null): Any = {
    val config = new TableConfig()
    val ctx = new CodeGeneratorContext(config, true)
    compileAndInvoke(ctx,
      generateCallExpression(
        ctx, operator, operands,
        TypeConverters.createInternalTypeFromTypeInfo(resultType), nullCheck = true))
  }

  def compileAndInvoke(ctx: CodeGeneratorContext, expr: GeneratedExpression): Any = {
    val name = newName("BinaryStringCallGenTest")
    val abstractClass = classOf[Func].getCanonicalName
    val code =
      s"""
          |public class $name extends $abstractClass {
          |
          | ${ctx.reuseMemberCode()}
          |
          | @Override
          | public Object apply() {
          |   ${ctx.reuseFieldCode()}
          |   ${expr.code}
          |   if (${expr.nullTerm}) {
          |     return null;
          |   } else {
          |     return ${expr.resultTerm};
          |   }
          | }
          |}
          """.stripMargin
    val func = compile(Thread.currentThread().getContextClassLoader, name, code)
        .newInstance().asInstanceOf[Func]
    func()
  }

  def toBinary(term: String): String = s"$BINARY_STRING.fromString($term)"

  def str(term: String): GeneratedExpression =
    newOperand(toBinary("\"" + term + "\""), Types.STRING)

  def int(term: String): GeneratedExpression =
    newOperand(term, Types.INT)

  def newOperand(resultTerm: String, resultType: TypeInformation[_]): GeneratedExpression =
    GeneratedExpression(resultTerm, "false", "",
      TypeConverters.createInternalTypeFromTypeInfo(resultType))

  @Test
  def testEquals(): Unit = {
    assertFalse(invoke(EQUALS, Seq(str("haha"), str("hehe"))).asInstanceOf[Boolean])
    assertTrue(invoke(EQUALS, Seq(str("haha"), str("haha"))).asInstanceOf[Boolean])

    assertTrue(invoke(NOT_EQUALS, Seq(str("haha"), str("hehe"))).asInstanceOf[Boolean])
    assertFalse(invoke(NOT_EQUALS, Seq(str("haha"), str("haha"))).asInstanceOf[Boolean])
  }

  @Test
  def testLike(): Unit = {
    assertFalse(invoke(SqlStdOperatorTable.LIKE, Seq(str("haha"), str("hehe")))
        .asInstanceOf[Boolean])
    assertTrue(invoke(SqlStdOperatorTable.LIKE, Seq(str("haha"), str("haha")))
        .asInstanceOf[Boolean])

    assertTrue(invoke(SqlStdOperatorTable.NOT_LIKE, Seq(str("haha"), str("hehe")))
        .asInstanceOf[Boolean])
    assertFalse(invoke(SqlStdOperatorTable.NOT_LIKE, Seq(str("haha"), str("haha")))
        .asInstanceOf[Boolean])
  }

  @Test
  def testCharLength(): Unit = {
    assertEquals(4, invoke(CHAR_LENGTH, Seq(str("haha"))))
    assertEquals(4, invoke(CHARACTER_LENGTH, Seq(str("haha"))))
  }

  @Test
  def testSqlTime(): Unit = {
    assertEquals(1453438905L,
      invoke(ScalarSqlFunctions.UNIX_TIMESTAMP,
        Seq(str("2016-01-22 05:01:45")), Types.SQL_TIMESTAMP))

    assertEquals(-120,
      invoke(ScalarSqlFunctions.DATEDIFF,
        Seq(str("2016-01-22"), str("2016-05-21")), Types.INT))
  }

  @Test
  def testSimilarTo(): Unit = {
    assertFalse(invoke(SIMILAR_TO, Seq(str("haha"), str("hehe")))
        .asInstanceOf[Boolean])
    assertTrue(invoke(SIMILAR_TO, Seq(str("haha"), str("haha")))
        .asInstanceOf[Boolean])

    assertTrue(invoke(NOT_SIMILAR_TO, Seq(str("haha"), str("hehe")))
        .asInstanceOf[Boolean])
    assertFalse(invoke(NOT_SIMILAR_TO, Seq(str("haha"), str("haha")))
        .asInstanceOf[Boolean])
  }

  @Test
  def testIsXxx(): Unit = {
    assertTrue(invoke(ScalarSqlFunctions.IS_DECIMAL, Seq(str("1234134")))
        .asInstanceOf[Boolean])
    assertTrue(invoke(ScalarSqlFunctions.IS_DIGIT, Seq(str("1234134")))
        .asInstanceOf[Boolean])
    assertTrue(invoke(ScalarSqlFunctions.IS_ALPHA, Seq(str("adb")))
        .asInstanceOf[Boolean])
  }

  @Test
  def testPosition(): Unit = {
    assertEquals(5, invoke(POSITION, Seq(str("d"), str("aaaadfg"))))
  }

  @Test
  def testHash(): Unit = {
    assertEquals(1236857883, invoke(ScalarSqlFunctions.HASH_CODE, Seq(str("aaaadfg"))))
  }
}

abstract class Func {
  def apply(): Any
}
