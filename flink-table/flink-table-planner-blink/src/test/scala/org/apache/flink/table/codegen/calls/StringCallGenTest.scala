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

package org.apache.flink.table.codegen.calls

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.table.api.{DataTypes, TableConfig}
import org.apache.flink.table.codegen.CodeGenUtils.{BINARY_STRING, newName}
import org.apache.flink.table.codegen.ExprCodeGenerator.generateCallExpression
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.generated.CompileUtils.compile
import org.apache.flink.table.types.logical.LogicalType
import org.junit.Assert._
import org.junit.Test

class StringCallGenTest {

  def invoke(operator: SqlOperator, operands: Seq[GeneratedExpression], tpe: LogicalType): Any = {
    val config = new TableConfig()
    val ctx = new CodeGeneratorContext(config)
    val expr = generateCallExpression(ctx, operator, operands, tpe)
    compileAndInvoke(ctx, expr)
  }

  def compileAndInvoke(ctx: CodeGeneratorContext, expr: GeneratedExpression): Any = {
    val name = newName("StringCallGenTest")
    val abstractClass = classOf[Func].getCanonicalName
    val code =
      s"""
         |public class $name extends $abstractClass {
         |
         |  ${ctx.reuseMemberCode()}
         |
         |  @Override
         |  public Object apply() {
         |    ${ctx.reuseLocalVariableCode()}
         |    ${expr.code}
         |    if (${expr.nullTerm}) {
         |      return null;
         |    } else {
         |      return ${expr.resultTerm};
         |    }
         |  }
         |}
          """.stripMargin
    val func = compile(Thread.currentThread().getContextClassLoader, name, code)
      .newInstance().asInstanceOf[Func]
    func()
  }

  def toBinary(term: String): String = s"$BINARY_STRING.fromString($term)"

  def str(term: String): GeneratedExpression =
    newOperand(toBinary("\"" + term + "\""), DataTypes.STRING.getLogicalType)

  def int(term: String): GeneratedExpression =
    newOperand(term, DataTypes.INT.getLogicalType)

  def newOperand(resultTerm: String, resultType: LogicalType): GeneratedExpression =
    GeneratedExpression(resultTerm, "false", "", resultType)

  @Test
  def testEquals(): Unit = {
    assertFalse(invoke(EQUALS, Seq(str("haha"), str("hehe")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertTrue(invoke(EQUALS, Seq(str("haha"), str("haha")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])

    assertTrue(invoke(NOT_EQUALS, Seq(str("haha"), str("hehe")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertFalse(invoke(NOT_EQUALS, Seq(str("haha"), str("haha")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
  }

  @Test
  def testLike(): Unit = {
    assertFalse(invoke(SqlStdOperatorTable.LIKE, Seq(str("haha"), str("hehe")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertTrue(invoke(SqlStdOperatorTable.LIKE, Seq(str("haha"), str("haha")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])

    assertTrue(invoke(SqlStdOperatorTable.NOT_LIKE, Seq(str("haha"), str("hehe")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertFalse(invoke(SqlStdOperatorTable.NOT_LIKE, Seq(str("haha"), str("haha")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
  }

  @Test
  def testCharLength(): Unit = {
    assertEquals(4, invoke(CHAR_LENGTH, Seq(str("haha")), DataTypes.INT.getLogicalType))
    assertEquals(4, invoke(CHARACTER_LENGTH, Seq(str("haha")), DataTypes.INT.getLogicalType))
  }

  @Test
  def testSqlTime(): Unit = {
    assertEquals(1453438905L,
      invoke(FlinkSqlOperatorTable.UNIX_TIMESTAMP,
        Seq(str("2016-01-22 05:01:45")), DataTypes.TIMESTAMP.getLogicalType))

    assertEquals(-120,
      invoke(FlinkSqlOperatorTable.DATEDIFF,
        Seq(str("2016-01-22"), str("2016-05-21")), DataTypes.DATE.getLogicalType))
  }

  @Test
  def testSimilarTo(): Unit = {
    assertFalse(invoke(SIMILAR_TO, Seq(str("haha"), str("hehe")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertTrue(invoke(SIMILAR_TO, Seq(str("haha"), str("haha")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])

    assertTrue(invoke(NOT_SIMILAR_TO, Seq(str("haha"), str("hehe")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertFalse(invoke(NOT_SIMILAR_TO, Seq(str("haha"), str("haha")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
  }

  @Test
  def testIsXxx(): Unit = {
    assertTrue(invoke(FlinkSqlOperatorTable.IS_DECIMAL, Seq(str("1234134")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertTrue(invoke(FlinkSqlOperatorTable.IS_DIGIT, Seq(str("1234134")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
    assertTrue(invoke(FlinkSqlOperatorTable.IS_ALPHA, Seq(str("adb")),
      DataTypes.BOOLEAN.getLogicalType).asInstanceOf[Boolean])
  }

  @Test
  def testPosition(): Unit = {
    assertEquals(5,
      invoke(POSITION, Seq(str("d"), str("aaaadfg")),
      DataTypes.INT.getLogicalType))
  }

  @Test
  def testHash(): Unit = {
    assertEquals(1236857883,
      invoke(FlinkSqlOperatorTable.HASH_CODE, Seq(str("aaaadfg")),
      DataTypes.INT.getLogicalType))
  }
}

abstract class Func {
  def apply(): Any
}
