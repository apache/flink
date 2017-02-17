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

package org.apache.flink.table.plan.util

import java.math.BigDecimal

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.{RexBuilder, RexProgram, RexProgramBuilder}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.plan.util.RexProgramExpressionExtractor._
import org.apache.flink.table.plan.schema.CompositeRelDataType
import org.apache.flink.table.utils.CommonTestData
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._

class RexProgramExpressionExtractorTest {

  private val typeFactory = new FlinkTypeFactory(RelDataTypeSystem.DEFAULT)
  private val allFieldTypes = List(VARCHAR, DECIMAL, INTEGER, DOUBLE).map(typeFactory.createSqlType)
  private val allFieldTypeInfos: Array[TypeInformation[_]] =
    Array(BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.BIG_DEC_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO)
  private val allFieldNames = List("name", "id", "amount", "price")

  @Test
  def testExtractExpression(): Unit = {
    val builder: RexBuilder = new RexBuilder(typeFactory)
    val program = buildRexProgram(
      allFieldNames, allFieldTypes, typeFactory, builder)
    val firstExp = ExpressionParser.parseExpression("id > 6")
    val secondExp = ExpressionParser.parseExpression("amount * price < 100")
    val expected: Array[Expression] = Array(firstExp, secondExp)
    val actual = extractPredicateExpressions(
      program,
      builder,
      CommonTestData.getMockTableEnvironment.getFunctionCatalog)

    assertEquals(expected.length, actual.length)
    // todo
  }

  @Test
  def testRewriteRexProgramWithCondition(): Unit = {
    val originalRexProgram = buildRexProgram(
      allFieldNames, allFieldTypes, typeFactory, new RexBuilder(typeFactory))
    val array = Array(
      "$0",
      "$1",
      "$2",
      "$3",
      "*($t2, $t3)",
      "100",
      "<($t4, $t5)",
      "6",
      ">($t1, $t7)",
      "AND($t6, $t8)")
    assertTrue(extractExprStrList(originalRexProgram) sameElements array)

    val tEnv = CommonTestData.getMockTableEnvironment
    val builder = FlinkRelBuilder.create(tEnv.getFrameworkConfig)
    val tableScan = new MockTableScan(builder.getRexBuilder)
    val newExpression = ExpressionParser.parseExpression("amount * price < 100")
    val newRexProgram = rewriteRexProgram(
      originalRexProgram,
      tableScan,
      Array(newExpression)
    )(builder)

    val newArray = Array(
      "$0",
      "$1",
      "$2",
      "$3",
      "*($t2, $t3)",
      "100",
      "<($t4, $t5)")
    assertTrue(extractExprStrList(newRexProgram) sameElements newArray)
  }

//  @Test
//  def testVerifyExpressions(): Unit = {
//    val strPart = "f1 < 4"
//    val part = parseExpression(strPart)
//
//    val shortFalseOrigin = parseExpression(s"f0 > 10 || $strPart")
//    assertFalse(verifyExpressions(shortFalseOrigin, part))
//
//    val longFalseOrigin = parseExpression(s"(f0 > 10 || (($strPart) > POWER(f0, f1))) && 2")
//    assertFalse(verifyExpressions(longFalseOrigin, part))
//
//    val shortOkayOrigin = parseExpression(s"f0 > 10 && ($strPart)")
//    assertTrue(verifyExpressions(shortOkayOrigin, part))
//
//    val longOkayOrigin = parseExpression(s"f0 > 10 && (($strPart) > POWER(f0, f1))")
//    assertTrue(verifyExpressions(longOkayOrigin, part))
//
//    val longOkayOrigin2 = parseExpression(s"(f0 > 10 || (2 > POWER(f0, f1))) && $strPart")
//    assertTrue(verifyExpressions(longOkayOrigin2, part))
//  }

  private def buildRexProgram(
      fieldNames: List[String],
      fieldTypes: Seq[RelDataType],
      typeFactory: JavaTypeFactory,
      rexBuilder: RexBuilder): RexProgram = {

    val inputRowType = typeFactory.createStructType(fieldTypes.asJava, fieldNames.asJava)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    val t0 = rexBuilder.makeInputRef(fieldTypes(2), 2)
    val t1 = rexBuilder.makeInputRef(fieldTypes(1), 1)
    val t2 = rexBuilder.makeInputRef(fieldTypes(3), 3)
    // t3 = t0 * t2
    val t3 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, t0, t2))
    val t4 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    val t5 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(6L))
    // project: amount, amount * price
    builder.addProject(t0, "amount")
    builder.addProject(t3, "total")
    // t6 = t3 < t4
    val t6 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t3, t4))
    // t7 = t1 > t5
    val t7 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t5))
    val t8 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.AND, List(t6, t7).asJava))
    // condition: t6 and t7
    // (t0 * t2 < t4 && t1 > t5)
    builder.addCondition(t8)
    builder.getProgram
  }

  /**
    * extract all expression string list from input RexProgram expression lists
    *
    * @param rexProgram input RexProgram instance to analyze
    * @return all expression string list of input RexProgram expression lists
    */
  private def extractExprStrList(rexProgram: RexProgram) =
    rexProgram.getExprList.asScala.map(_.toString).toArray

  class MockTableScan(
      rexBuilder: RexBuilder)
    extends TableScan(
      RelOptCluster.create(new VolcanoPlanner(), rexBuilder),
      RelTraitSet.createEmpty,
      new MockRelOptTable)

  class MockRelOptTable
    extends RelOptAbstractTable(
      null,
      "mockRelTable",
      new CompositeRelDataType(
        new RowTypeInfo(allFieldTypeInfos, allFieldNames.toArray), typeFactory))
}
