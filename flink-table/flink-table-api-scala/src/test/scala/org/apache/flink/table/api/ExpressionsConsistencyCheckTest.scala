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

package org.apache.flink.table.api

import org.apache.flink.table.expressions.ApiExpressionUtils._
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.{EQUALS, PLUS, TRIM}

import org.hamcrest.CoreMatchers
import org.hamcrest.collection.IsEmptyIterable
import org.junit.Assert._
import org.junit.Test

import java.lang.reflect.Modifier

import scala.collection.JavaConverters._

/**
 * We test that all methods are either available or have equivalents in both Scala and Java
 * expression DSL's
 *
 * If there are methods that do not map exactly in both APIs but have equivalent
 * methods add those to `explicitScalaToJavaStaticMethodsMapping`(for static methods
 * [[ImplicitExpressionConversions]]/[[Expressions]]) or `explicitScalaToJavaMapping`
 * (for infix methods [[ApiExpression]]/[[ImplicitExpressionOperations]]).
 * If equally named methods are not found the test will check if a mapping exists.
 * This is a bidirectional mapping.
 *
 * If there are methods that should not have an equivalent in the other API add those to a
 * corresponding list of exclude (`excludedStaticScalaMethods`, `excludedScalaMethods`,
 * `excludedStaticJavaMethods`, `excludedJavaMethods`).
 */
class ExpressionsConsistencyCheckTest {

  // we cannot get class of package object
  class Conversions extends ImplicitExpressionConversions {}

  // static methods from ImplicitExpressionConversions
  val explicitScalaToJavaStaticMethodsMapping = Map(
    "FieldExpression" -> "$",
    "UnresolvedFieldExpression" -> "$",
    "ImperativeAggregateFunctionCall" -> "call",
    "ScalarFunctionCall" -> "call",
    "TableFunctionCall" -> "call",
    "concat_ws" -> "concatWs"
  )

  // methods from WithOperations
  val explicitScalaToJavaMapping = Map(
    "$bang$eq$eq" -> "isNotEqual", // !==
    "$eq$eq$eq" -> "isEqual", // ===
    "$less$eq" -> "isLessOrEqual", // <=
    "$greater$eq" -> "isGreaterOrEqual", // >=
    "$less" -> "isLess", // <
    "$greater" -> "isGreater", // >
    "$amp$amp" -> "and", // &&
    "$bar$bar" -> "or", // ||
    "$times" -> "times", // *
    "$div" -> "dividedBy", // /
    "$plus" -> "plus", // +
    "$minus" -> "minus", // -
    "$percent" -> "mod", // %

    // in scala trim has default values
    "trim$default$1" -> "trimLeading",
    "trim$default$2" -> "trimTrailing",
    "trim$default$3" -> "trim"
  )

  val excludedStaticScalaMethods = Set(

    //-----------------------------------------------------------------------------------
    //  Scala implicit conversions to ImplicitExpressionOperations
    //-----------------------------------------------------------------------------------
    "WithOperations",
    "AnyWithOperations",
    "LiteralScalaDecimalExpression",
    "LiteralJavaDecimalExpression",
    "LiteralShortExpression",
    "LiteralFloatExpression",
    "LiteralSqlDateExpression",
    "LiteralBooleanExpression",
    "LiteralStringExpression",
    "LiteralByteExpression",
    "LiteralSqlTimestampExpression",
    "LiteralLongExpression",
    "LiteralDoubleExpression",
    "LiteralIntExpression",
    "LiteralSqlTimeExpression",

    //-----------------------------------------------------------------------------------
    //  Scala implicit conversions to Expressions
    //-----------------------------------------------------------------------------------
    "scalaRange2RangeExpression",
    "scalaDec2Literal",
    "double2Literal",
    "sqlTime2Literal",
    "symbol2FieldExpression",
    "sqlTimestamp2Literal",
    "localDateTime2Literal",
    "localTime2Literal",
    "javaDec2Literal",
    "byte2Literal",
    "int2Literal",
    "long2Literal",
    "short2Literal",
    "string2Literal",
    "sqlDate2Literal",
    "boolean2Literal",
    "localDate2Literal",
    "float2Literal",
    "array2ArrayConstructor",
    "seq2ArrayConstructor",
    "javaList2ArrayConstructor",
    "map2MapConstructor",
    "javaMap2MapConstructor",
    "row2RowConstructor",
    "tableSymbolToExpression",

    //-----------------------------------------------------------------------------------
    //  Internal methods
    //-----------------------------------------------------------------------------------
    "org$apache$flink$table$api$ImplicitExpressionConversions$_setter_$CURRENT_RANGE_$eq",
    "org$apache$flink$table$api$ImplicitExpressionConversions$_setter_$CURRENT_ROW_$eq",
    "org$apache$flink$table$api$ImplicitExpressionConversions$_setter_$UNBOUNDED_ROW_$eq",
    "org$apache$flink$table$api$ImplicitExpressionConversions$_setter_$UNBOUNDED_RANGE_$eq",
    "org$apache$flink$table$api$ExpressionsConsistencyCheckTest$Conversions$$$outer"
  )

  val excludedScalaMethods = Set(
    // in java we can use only static ifThenElse
    "$qmark", // ?

    // in java we can use only static not
    "unary_$bang", // unary_!

    // in java we can use only static range
    "to",

    // in java we can use only static rowInterval
    "rows",

    // users in java should use static negative()
    "unary_$minus", // unary_-

    // not supported in java
    "unary_$plus", // unary_+

    //-----------------------------------------------------------------------------------
    //  Internal methods
    //-----------------------------------------------------------------------------------
    "expr",
    "org$apache$flink$table$api$ImplicitExpressionConversions$WithOperations$$$outer",
    "toApiSpecificExpression"
  )

  val excludedStaticJavaMethods = Set(
    // in scala users should use "A" to "B"
    "range",

    // users should use 1.rows, 123.millis, 3.years
    "rowInterval",

    // users should use unary_-
    "negative"
  )

  val excludedJavaMethods = Set(
    //-----------------------------------------------------------------------------------
    //  Methods from Expression.java
    //-----------------------------------------------------------------------------------
    "accept",
    "asSummaryString",
    "getChildren"
  )

  @Test
  def testScalaStaticMethodsAvailableInJava(): Unit = {
    val scalaMethodNames = classOf[Conversions]
      .getMethods
      .map(_.getName)
      .toSet
    val javaMethodNames = classOf[Expressions].getMethods.map(_.getName).toSet ++
      classOf[Expressions].getFields.filter(f => Modifier.isStatic(f.getModifiers)).map(_.getName)

    checkMethodsMatch(
      scalaMethodNames,
      javaMethodNames,
      explicitScalaToJavaStaticMethodsMapping,
      excludedStaticScalaMethods)
  }

  @Test
  def testScalaExpressionMethodsAvailableInJava(): Unit = {
    val scalaMethodNames = classOf[ImplicitExpressionConversions#WithOperations]
      .getMethods
      .map(_.getName)
      .toSet
    val javaMethodNames = classOf[ApiExpression].getMethods.map(_.getName).toSet

    checkMethodsMatch(
      scalaMethodNames,
      javaMethodNames,
      explicitScalaToJavaMapping,
      excludedScalaMethods)
  }

  @Test
  def testJavaStaticMethodsAvailableInScala(): Unit = {
    val scalaMethodNames = classOf[Conversions].getMethods.map(_.getName).toSet
    val javaMethodNames = classOf[Expressions].getMethods.map(_.getName).toSet

    checkMethodsMatch(
      javaMethodNames,
      scalaMethodNames,
      explicitScalaToJavaStaticMethodsMapping.map(_.swap),
      excludedStaticJavaMethods)
  }

  @Test
  def testJavaExpressionMethodsAvailableInScala(): Unit = {
    val scalaMethodNames = classOf[ImplicitExpressionConversions#WithOperations]
      .getMethods
      .map(_.getName)
      .toSet
    val javaMethodNames = classOf[ApiExpression].getMethods.map(_.getName).toSet

    checkMethodsMatch(
      javaMethodNames,
      scalaMethodNames,
      explicitScalaToJavaMapping,
      excludedJavaMethods)
  }

  @Test
  def testInteroperability(): Unit = {
    // In most cases it should be just fine to mix the two APIs.
    // It should be discouraged though as it might have unforeseen side effects
    val expr = lit("ABC") === $"f0".plus(Expressions.$("f1")).plus($("f2")).trim()

    assertThat(
      expr,
      CoreMatchers.equalTo[Expression](
        unresolvedCall(
          EQUALS,
          valueLiteral("ABC"),
          unresolvedCall(
            TRIM,
            valueLiteral(true),
            valueLiteral(true),
            valueLiteral(" "),
            unresolvedCall(
              PLUS,
              unresolvedCall(
              PLUS,
                unresolvedRef("f0"),
                unresolvedRef("f1")
              ),
              unresolvedRef("f2")
            )
          )
        )
      )
    )
  }

  private def checkMethodsMatch(
      checkedMethods: Set[String],
      methodsBeingCheckedAgainst: Set[String],
      methodsMapping: Map[String, String],
      excludedMethods: Set[String])
    : Unit = {
    val missingMethods = (checkedMethods -- methodsBeingCheckedAgainst)
      .filterNot(
        scalaName => {
          val mappedName = methodsMapping.getOrElse(scalaName, scalaName)
          methodsBeingCheckedAgainst.contains(mappedName)
        }
      ).diff(excludedMethods)

    assertThat(missingMethods.asJava, IsEmptyIterable.emptyIterableOf(classOf[String]))
  }
}
