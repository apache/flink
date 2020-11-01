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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.calcite.FlinkRexBuilder
import org.apache.flink.table.planner.plan.utils.InputTypeBuilder.inputOf

import org.apache.calcite.rex.RexProgram
import org.apache.calcite.sql.`type`.SqlTypeName.{INTEGER, VARCHAR}

import org.junit.Assert.assertTrue
import org.junit.Test

import java.util.Arrays

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Test for [[RexNodeRewriter]].
  */
class RexNodeRewriterTest extends RexNodeTestBase {

  @Test
  def testRewriteRexProgram(): Unit = {
    val rexProgram = buildSimpleRexProgram()
    val exprs = rexProgram.getExprList
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$0",
      "$1",
      "$2",
      "$3",
      "$4",
      "*($t2, $t3)",
      "100",
      "<($t5, $t6)",
      "6",
      ">($t1, $t8)",
      "AND($t7, $t9)")))

    // use amount, id, price fields to create a new RexProgram
    val usedFields = Array(2, 3, 1)
    val projectExprs = rexProgram.getProjectList.map(expr => rexProgram.expandLocalRef(expr))
    val newProjectExprs = RexNodeRewriter.rewriteWithNewFieldInput(projectExprs, usedFields)
    val conditionExpr = rexProgram.expandLocalRef(rexProgram.getCondition)
    val newConditionExpr = RexNodeRewriter.rewriteWithNewFieldInput(
      List(conditionExpr).asJava,
      usedFields).head
    val types = usedFields.map(allFieldTypes.get).toList.asJava
    val names = usedFields.map(allFieldNames.get).toList.asJava
    val inputRowType = typeFactory.createStructType(types, names)
    val newProgram = RexProgram.create(
      inputRowType, newProjectExprs, newConditionExpr, rexProgram.getOutputRowType, rexBuilder)
    val newExprs = newProgram.getExprList
    assertTrue(newExprs.asScala.map(_.toString) == wrapRefArray(Array(
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

  @Test
  def testRewriteRexProgramWithNestedField(): Unit = {
    val rexProgram = buildSimpleRexProgram()
    val exprs = rexProgram.getExprList
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$0",
      "$1",
      "$2",
      "$3",
      "$4",
      "*($t2, $t3)",
      "100",
      "<($t5, $t6)",
      "6",
      ">($t1, $t8)",
      "AND($t7, $t9)")))

    val nestedField = RexNodeNestedField.build(exprs, rexProgram.getInputRowType)
    val paths = RexNodeNestedField.labelAndConvert(nestedField)
    val orderedPaths = Array(
      Array(4),
      Array(0),
      Array(1),
      Array(2),
      Array(3)
    )
    // actual data has the same order as expected
    orderedPaths.zip(paths).foreach {
      case (expected, actual) => assert(expected.sameElements(actual))
    }
    val newExprs = RexNodeNestedField.rewrite(exprs, nestedField, new FlinkRexBuilder(typeFactory))
    assertTrue(newExprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$1",
      "$2",
      "$3",
      "$4",
      "$0",
      "*($t2, $t3)",
      "100",
      "<($t5, $t6)",
      "6",
      ">($t1, $t8)",
      "AND($t7, $t9)")))
  }

  @Test
  def testRewriteRexProgramWithNestedProject(): Unit ={
    val (exprs, _) = buildExprsWithNesting()
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$1.amount",
      "$0",
      "100"
    )))

    // origin schema: $0 = RAW<name INT, age varchar>, $1 = RAW<id BIGINT, amount int>.amount
    // new schema: $1 = ROW<name INT, age varchar>, $0 = ROW<id BIGINT, amount int>.amount
    val fieldMap = Map(int2Integer(0) -> Map(Arrays.asList("*") -> int2Integer(1)).asJava,
      int2Integer(1) -> Map(Arrays.asList("amount") -> int2Integer(0)).asJava).asJava
    val rowTypes = Arrays.asList(
      inputOf(typeFactory).field("amount", INTEGER).build,
      inputOf(typeFactory)
        .field("name", INTEGER)
        .field("age", VARCHAR)
        .build)
    val builder = new FlinkRexBuilder(typeFactory)
    val actual = RexNodeRewriter.rewriteNestedProjectionWithNewFieldInput(
      exprs, fieldMap, rowTypes, builder)

    assertTrue(actual.asScala.map(_.toString) == wrapRefArray(Array(
      "$0",
      "$1",
      "100")))
  }

  @Test
  def testRewriteRExProgramWithNestedProjectUsingNestedField(): Unit = {
    val (exprs, rowType) = buildExprsWithNesting()
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$1.amount",
      "$0",
      "100"
    )))

    val nestedField = RexNodeNestedField.build(exprs, rowType)
    val paths = RexNodeNestedField.labelAndConvert(nestedField)
    val orderedPaths = Array(
      Array(0),
      Array(1, 1)
    )
    // actual data has the same order as expected
    orderedPaths.zip(paths).foreach {
      case (expected, actual) => assert(expected.sameElements(actual))
    }
    val newExprs = RexNodeNestedField.rewrite(exprs, nestedField, new FlinkRexBuilder(typeFactory))

    assertTrue(newExprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$1",
      "$0",
      "100")))
  }

  @Test
  def testRewriteRexProgramWithDeepNestedProject(): Unit ={
    val (exprs, _) = buildExprsWithDeepNesting()
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "*($1.amount, 10)",
      "$0.passport.status",
      "$2.with.deep.entry",
      "$2.with.deeper.entry.inside.entry",
      "$2.with.deeper.entry",
      "$0"
    )))

    // origin schema:
    // $0 = persons ROW<name VARCHAR, age INT, passport ROW<id VARCHAR, status VARCHAR>>
    // $1 = payment ROW<id BIGINT, amount INT>
    // $2 = field ROW<with ROW<deeper ROW<entry ROW<inside ROW<entry VARCHAR>>>,
    //                         deep ROW<entry VARCHAR>>>

    // new schema:
    // $0 = payment.amount INT
    // $1 = persons ROW<name VARCHAR, age INT, passport ROW<id VARCHAR, status VARCHAR>>
    // $2 = field.with.deep.entry VARCHAR
    // $3 = field.with.deeper.entry ROW<inside ROW<entry VARCHAR>>

    // mapping
    // $1.amount -> $0
    // $0.passport.status -> $1.passport.status
    // $2.with.deep.entry -> $2
    // $2.with.deeper.entry.inside.entry -> $3.inside.entry
    // $2.with.deeper.entry -> $3
    // $0 -> $1


    val fieldMap = Map(
      int2Integer(0) -> Map(Arrays.asList("*") -> int2Integer(1)).asJava,
      int2Integer(1) -> Map(Arrays.asList("amount") -> int2Integer(0)).asJava,
      int2Integer(2) ->
        Map(Arrays.asList("with","deep", "entry") -> int2Integer(2),
          Arrays.asList("with", "deeper", "entry") -> int2Integer(3)).asJava
    ).asJava
    val rowTypes = Arrays.asList(
      inputOf(typeFactory).field("amount", INTEGER).build,
      inputOf(typeFactory)
        .field("name", VARCHAR)
        .field("age", INTEGER)
        .nestedField(
          "passport",
          inputOf(typeFactory)
            .field("id", VARCHAR)
            .field("status", VARCHAR)
            .build
        ).build,
      inputOf(typeFactory).field("entry", VARCHAR).build,
      inputOf(typeFactory).nestedField("inside",
        inputOf(typeFactory).field("entry", VARCHAR).build)
        .build
    )
    val builder = new FlinkRexBuilder(typeFactory)
    val actual = RexNodeRewriter.rewriteNestedProjectionWithNewFieldInput(
      exprs, fieldMap, rowTypes, builder)
    assertTrue(actual.asScala.map(_.toString) == wrapRefArray(Array(
      "*($0, 10)",
      "$1.passport.status",
      "$2",
      "$3.inside.entry",
      "$3",
      "$1")))
  }

  @Test
  def testRewriteRexProgramWithDeepNestedProjectUsingNestedFields(): Unit = {
    val (exprs, rowType) = buildExprsWithDeepNesting()
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "*($1.amount, 10)",
      "$0.passport.status",
      "$2.with.deep.entry",
      "$2.with.deeper.entry.inside.entry",
      "$2.with.deeper.entry",
      "$0"
    )))
    val nestedField = RexNodeNestedField.build(exprs, rowType)
    val paths = RexNodeNestedField.labelAndConvert(nestedField)
    val orderedPaths = Array(
      Array(0),
      Array(1, 1),
      Array(2, 0, 0, 0),
      Array(2, 0, 1, 0)
    )
    orderedPaths.zip(paths).foreach {
      case (expected, actual) => assert(expected.sameElements(actual))
    }
    val newExprs = RexNodeNestedField.rewrite(exprs, nestedField, new FlinkRexBuilder(typeFactory))

    assertTrue(newExprs.asScala.map(_.toString) == wrapRefArray(Array(
      "*($1, 10)",
      "$0.passport.status",
      "$2",
      "$3.inside.entry",
      "$3",
      "$0")))
  }
}
