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

import org.apache.flink.table.plan.util.InputTypeBuilder.inputOf

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, INTEGER, VARCHAR}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.hamcrest.CoreMatchers.is
import org.junit.Assert.{assertArrayEquals, assertThat}
import org.junit.Test

import java.math.BigDecimal
import java.util.{List => JList}

import scala.collection.JavaConverters._

/**
  * Test for [[RexNodeExtractor]].
  */
class RexNodeExtractorTest extends RexNodeTestBase {

  @Test
  def testExtractRefInputFields(): Unit = {
    val usedFields = RexNodeExtractor.extractRefInputFields(buildExprs())
    assertArrayEquals(usedFields, Array(2, 3, 1))
  }

  @Test
  def testExtractRefNestedInputFields(): Unit = {
    val rexProgram = buildExprsWithNesting()

    val usedFields = RexNodeExtractor.extractRefInputFields(rexProgram)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(Array("amount"), Array("*"))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractRefNestedInputFieldsWithNoNesting(): Unit = {
    val exprs = buildExprs()

    val usedFields = RexNodeExtractor.extractRefInputFields(exprs)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(exprs, usedFields)

    val expected = Array(Array("*"), Array("*"), Array("*"))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractDeepRefNestedInputFields(): Unit = {
    val rexProgram = buildExprsWithDeepNesting()

    val usedFields = RexNodeExtractor.extractRefInputFields(rexProgram)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(
      Array("amount"),
      Array("*"),
      Array("with.deeper.entry", "with.deep.entry"))

    assertThat(usedFields, is(Array(1, 0, 2)))
    assertThat(usedNestedFields, is(expected))
  }

  private def buildExprsWithDeepNesting(): JList[RexNode] = {

    // person input
    val passportRow = inputOf(typeFactory)
      .field("id", VARCHAR)
      .field("status", VARCHAR)
      .build

    val personRow = inputOf(typeFactory)
      .field("name", VARCHAR)
      .field("age", INTEGER)
      .nestedField("passport", passportRow)
      .build

    // payment input
    val paymentRow = inputOf(typeFactory)
      .field("id", BIGINT)
      .field("amount", INTEGER)
      .build

    // deep field input
    val deepRowType = inputOf(typeFactory)
      .field("entry", VARCHAR)
      .build

    val entryRowType = inputOf(typeFactory)
      .nestedField("inside", deepRowType)
      .build

    val deeperRowType = inputOf(typeFactory)
      .nestedField("entry", entryRowType)
      .build

    val withRowType = inputOf(typeFactory)
      .nestedField("deep", deepRowType)
      .nestedField("deeper", deeperRowType)
      .build

    val fieldRowType = inputOf(typeFactory)
      .nestedField("with", withRowType)
      .build

    // inputRowType
    //
    // [ persons:  [ name: VARCHAR, age:  INT, passport: [id: VARCHAR, status: VARCHAR ] ],
    //   payments: [ id: BIGINT, amount: INT ],
    //   field:    [ with: [ deep: [ entry: VARCHAR ],
    //                       deeper: [ entry: [ inside: [entry: VARCHAR ] ] ]
    //             ] ]
    // ]

    val t0 = rexBuilder.makeInputRef(personRow, 0)
    val t1 = rexBuilder.makeInputRef(paymentRow, 1)
    val t2 = rexBuilder.makeInputRef(fieldRowType, 2)
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10L))

    // person
    val person$pass = rexBuilder.makeFieldAccess(t0, "passport", false)
    val person$pass$stat = rexBuilder.makeFieldAccess(person$pass, "status", false)

    // payment
    val pay$amount = rexBuilder.makeFieldAccess(t1, "amount", false)
    val multiplyAmount = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, pay$amount, t3)

    // field
    val field$with = rexBuilder.makeFieldAccess(t2, "with", false)
    val field$with$deep = rexBuilder.makeFieldAccess(field$with, "deep", false)
    val field$with$deeper = rexBuilder.makeFieldAccess(field$with, "deeper", false)
    val field$with$deep$entry = rexBuilder.makeFieldAccess(field$with$deep, "entry", false)
    val field$with$deeper$entry = rexBuilder.makeFieldAccess(field$with$deeper, "entry", false)
    val field$with$deeper$entry$inside = rexBuilder
      .makeFieldAccess(field$with$deeper$entry, "inside", false)
    val field$with$deeper$entry$inside$entry = rexBuilder
      .makeFieldAccess(field$with$deeper$entry$inside, "entry", false)

    // Program
    // (
    //   payments.amount * 10),
    //   persons.passport.status,
    //   field.with.deep.entry
    //   field.with.deeper.entry.inside.entry
    //   field.with.deeper.entry
    //   persons
    // )
    List(multiplyAmount, person$pass$stat, field$with$deep$entry,
      field$with$deeper$entry$inside$entry, field$with$deeper$entry, t0).asJava

  }

  private def buildExprsWithNesting(): JList[RexNode] = {
    val personRow = inputOf(typeFactory)
      .field("name", INTEGER)
      .field("age", VARCHAR)
      .build

    val paymentRow = inputOf(typeFactory)
      .field("id", BIGINT)
      .field("amount", INTEGER)
      .build

    val types = List(personRow, paymentRow).asJava

    val t0 = rexBuilder.makeInputRef(types.get(0), 0)
    val t1 = rexBuilder.makeInputRef(types.get(1), 1)
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    val payment$amount = rexBuilder.makeFieldAccess(t1, "amount", false)

    List(payment$amount, t0, t2).asJava
  }

}
