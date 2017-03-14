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

package org.apache.flink.table.plan.rules.util

import java.math.BigDecimal

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rex.{RexBuilder, RexProgram, RexProgramBuilder}
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, DOUBLE, INTEGER, VARCHAR}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.table.plan.rules.util.RexProgramProjectExtractor._
import org.apache.flink.table.plan.rules.util.InputTypeBuilder._
import org.junit.Assert.{assertArrayEquals, assertThat, assertTrue}
import org.hamcrest.CoreMatchers.is
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

/**
  * This class is responsible for testing RexProgramProjectExtractor.
  */
class RexProgramProjectExtractorTest {
  private var typeFactory: JavaTypeFactory = _
  private var rexBuilder: RexBuilder = _
  private var allFieldTypes: Seq[RelDataType] = _
  private val allFieldNames = List("name", "id", "amount", "price")

  @Before
  def setUp(): Unit = {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
    rexBuilder = new RexBuilder(typeFactory)
    allFieldTypes = List(VARCHAR, BIGINT, INTEGER, DOUBLE).map(typeFactory.createSqlType(_))
  }

  @Test
  def testExtractRefInputFields(): Unit = {
    val usedFields = extractRefInputFields(buildRexProgram())
    assertArrayEquals(usedFields, Array(2, 3, 1))
  }

  @Test
  def testExtractRefNestedInputFields(): Unit = {
    val rexProgram = buildRexProgramWithNesting()

    val usedFields = extractRefInputFields(rexProgram)
    val usedNestedFields = extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(Array("amount"), Array("*"))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractRefNestedInputFieldsWithNoNesting(): Unit = {
    val rexProgram = buildRexProgram()

    val usedFields = extractRefInputFields(rexProgram)
    val usedNestedFields = extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(Array("*"), Array("*"), Array("*"))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractDeepRefNestedInputFields(): Unit = {
    val rexProgram = buildRexProgramWithDeepNesting()

    val usedFields = extractRefInputFields(rexProgram)
    val usedNestedFields = extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(
      Array("amount"),
      Array("passport.status"),
      Array("with.deep.entry", "with.deeper.entry.inside.entry"))

    assertThat(usedFields, is(Array(1, 0, 2)))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testRewriteRexProgram(): Unit = {
    val originRexProgram = buildRexProgram()
    assertTrue(extractExprStrList(originRexProgram).sameElements(Array(
      "$0",
      "$1",
      "$2",
      "$3",
      "*($t2, $t3)",
      "100",
      "<($t4, $t5)",
      "6",
      ">($t1, $t7)",
      "AND($t6, $t8)")))
    // use amount, id, price fields to create a new RexProgram
    val usedFields = Array(2, 3, 1)
    val types = usedFields.map(allFieldTypes(_)).toList.asJava
    val names = usedFields.map(allFieldNames(_)).toList.asJava
    val inputRowType = typeFactory.createStructType(types, names)
    val newRexProgram = rewriteRexProgram(originRexProgram, inputRowType, usedFields, rexBuilder)
    assertTrue(extractExprStrList(newRexProgram).sameElements(Array(
      "$0",
      "$1",
      "$2",
      "*($t0, $t1)",
      "100",
      "<($t3, $t4)",
      "6",
      ">($t2, $t6)",
      "AND($t5, $t7)")))
  }

  private def buildRexProgramWithDeepNesting(): RexProgram = {

    // person input
    val passportRow = inputOf(typeFactory)
      .field("id").withType(VARCHAR)
      .field("status").withType(VARCHAR)
      .build

    val personRow = inputOf(typeFactory)
      .field("name").withType(VARCHAR)
      .field("age").withType(INTEGER)
      .field("passport").withNestedType(passportRow)
      .build

    // payment input
    val paymentRow = inputOf(typeFactory)
      .field("id").withType(BIGINT)
      .field("amount").withType(INTEGER)
      .build

    // deep field input
    val deepRowType = inputOf(typeFactory)
      .field("entry").withType(VARCHAR)
      .build

    val insideRowType = deepRowType

    val entryRowType = inputOf(typeFactory)
      .field("inside").withNestedType(insideRowType)
      .build

    val deeperRowType = inputOf(typeFactory)
      .field("entry").withNestedType(entryRowType)
      .build

    val withRowType = inputOf(typeFactory)
      .field("deep").withNestedType(deepRowType)
      .field("deeper").withNestedType(deeperRowType)
      .build

    val fieldRowType = inputOf(typeFactory)
      .field("with").withNestedType(withRowType)
      .build

    // main input
    val inputRowType = inputOf(typeFactory)
      .field("persons").withNestedType(personRow)
      .field("payments").withNestedType(paymentRow)
      .field("field").withNestedType(fieldRowType)
      .build

    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    val t0 = rexBuilder.makeInputRef(personRow, 0)
    val t1 = rexBuilder.makeInputRef(paymentRow, 1)
    val t2 = rexBuilder.makeInputRef(fieldRowType, 2)
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10L))

    // person
    val person$pass = rexBuilder.makeFieldAccess(t0, "passport", false)
    val person$pass$stat = rexBuilder.makeFieldAccess(person$pass, "status", false)

    // payment
    val pay$amount = rexBuilder.makeFieldAccess(t1, "amount", false)
    val multiplyAmount = builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, pay$amount, t3))

    // field
    val field$with = rexBuilder.makeFieldAccess(t2, "with", false)
    val field$with$deep = rexBuilder.makeFieldAccess(field$with, "deep", false)
    val field$with$deeper = rexBuilder.makeFieldAccess(field$with, "deeper", false)
    val field$with$deep$entry = rexBuilder.makeFieldAccess(field$with$deep, "entry", false)
    val field$with$deeper$entry = rexBuilder.makeFieldAccess(field$with$deeper, "entry", false)
    val field$with$deeper$entry$inside = rexBuilder
      .makeFieldAccess(field$with$deeper$entry, "inside", false)
    val field$with$deeper$inside$entry = rexBuilder
      .makeFieldAccess(field$with$deeper$entry$inside, "entry", false)

    builder.addProject(multiplyAmount, "amount")
    builder.addProject(person$pass$stat, "status")
    builder.addProject(field$with$deep$entry, "entry")
    builder.addProject(field$with$deeper$inside$entry, "entry")
    builder.getProgram
  }

  private def buildRexProgramWithNesting(): RexProgram = {

    val personRow = inputOf(typeFactory)
      .field("name").withType(INTEGER)
      .field("age").withType(VARCHAR)
      .build

    val paymentRow = inputOf(typeFactory)
      .field("id").withType(BIGINT)
      .field("amount").withType(INTEGER)
      .build

    val types = List(personRow, paymentRow).asJava
    val names = List("persons", "payments").asJava
    val inputRowType = typeFactory.createStructType(types, names)

    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    val t0 = rexBuilder.makeInputRef(types.get(0), 0)
    val t1 = rexBuilder.makeInputRef(types.get(1), 1)
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    val payment$amount = rexBuilder.makeFieldAccess(t1, "amount", false)

    builder.addProject(payment$amount, "amount")
    builder.addProject(t0, "persons")
    builder.addProject(t2, "number")
    builder.getProgram
  }

  private def buildRexProgram(): RexProgram = {
    val types = allFieldTypes.asJava
    val names = allFieldNames.asJava
    val inputRowType = typeFactory.createStructType(types, names)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)
    val t0 = rexBuilder.makeInputRef(types.get(2), 2)
    val t1 = rexBuilder.makeInputRef(types.get(1), 1)
    val t2 = rexBuilder.makeInputRef(types.get(3), 3)
    val t3 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, t0, t2))
    val t4 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    val t5 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(6L))
    // project: amount, amount * price
    builder.addProject(t0, "amount")
    builder.addProject(t3, "total")
    // condition: amount * price < 100 and id > 6
    val t6 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t3, t4))
    val t7 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t5))
    val t8 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.AND, List(t6, t7).asJava))
    builder.addCondition(t8)
    builder.getProgram
  }

  /**
    * extract all expression string list from input RexProgram expression lists
    *
    * @param rexProgram input RexProgram instance to analyze
    * @return all expression string list of input RexProgram expression lists
    */
  private def extractExprStrList(rexProgram: RexProgram) = {
    rexProgram.getExprList.asScala.map(_.toString)
  }

}
