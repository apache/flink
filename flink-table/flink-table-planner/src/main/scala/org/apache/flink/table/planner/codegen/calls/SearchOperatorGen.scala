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
package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenException, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.newNames
import org.apache.flink.table.planner.codegen.GenerateUtils.generateLiteral
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens._
import org.apache.flink.table.planner.functions.casting.CastRuleProvider
import org.apache.flink.table.planner.plan.utils.RexLiteralUtil.toFlinkInternalValue
import org.apache.flink.table.types.logical.{BooleanType, LogicalType}
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging.findCommonType

import org.apache.calcite.rex.{RexLiteral, RexUnknownAs}
import org.apache.calcite.util.{RangeSets, Sarg}

import java.util.Arrays.asList

import scala.collection.JavaConverters._

/**
 * Class containing utilities to implement the SEARCH operator.
 *
 * This does not implement [[CallGenerator]] as the interface does not fit, because the [[Sarg]]
 * argument cannot be converted directly to [[GeneratedExpression]].
 */
object SearchOperatorGen {

  /**
   * Generates SEARCH expression using either an HashSet or a concatenation of OR, depending on
   * whether the elements of the haystack are all literals or not.
   *
   * Note that both IN/NOT IN are converted to SEARCH when the set has only constant values,
   * otherwise the IN/NOT IN are converted to a set of disjunctions. See
   * [[org.apache.calcite.rex.RexBuilder#makeIn(org.apache.calcite.rex.RexNode, java.util.List)]].
   */
  def generateSearch(
      ctx: CodeGeneratorContext,
      target: GeneratedExpression,
      sargLiteral: RexLiteral): GeneratedExpression = {
    val sarg: Sarg[Nothing] = sargLiteral.getValueAs(classOf[Sarg[Nothing]])
    val targetType = target.resultType
    val sargType = FlinkTypeFactory.toLogicalType(sargLiteral.getType)

    val commonType: LogicalType = findCommonType(asList(targetType, sargType))
      .orElseThrow(
        () => new CodeGenException(s"Unable to find common type of $target and $sargLiteral."))

    val needle = generateCast(
      ctx,
      target,
      commonType,
      nullOnFailure = false
    )

    // In case the search is among points we use the hashset implementation
    if (sarg.isPoints || sarg.isComplementedPoints) {
      val rangeSet = if (sarg.isPoints) sarg.rangeSet else sarg.rangeSet.complement()
      val haystack = rangeSet
        .asRanges()
        .asScala
        // We need to go through the generateLiteral to normalize the value from calcite
        .map(r => toFlinkInternalValue(r.lowerEndpoint, sargType))
        // The elements are constant, we perform the cast immediately
        .map(CastRuleProvider.cast(toCastContext(ctx), sargType, commonType, _))
        .map(generateLiteral(ctx, _, commonType))
      val setTerm = ctx.addReusableHashSet(haystack.toSeq, commonType)
      val negation = if (sarg.isComplementedPoints) "!" else ""

      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
      // Since https://issues.apache.org/jira/browse/CALCITE-4446
      // there is three-valued logic for SEARCH operator
      // sarg.nullAs should be used instead of sarg.containsNull
      val isNullCode = sarg.nullAs match {
        case RexUnknownAs.TRUE =>
          s"""
             |$resultTerm = true;
             |$nullTerm = false;
             |""".stripMargin
        case RexUnknownAs.FALSE =>
          s"""
             |$resultTerm = false;
             |$nullTerm = false;
             |""".stripMargin
        case RexUnknownAs.UNKNOWN =>
          s"""
             |$resultTerm = false;
             |$nullTerm = true;
             |""".stripMargin
      }

      val operatorCode =
        s"""
           |${needle.code}
           |// --- Begin SEARCH ${target.resultTerm}
           |boolean $resultTerm;
           |boolean $nullTerm;
           |$isNullCode
           |if (!${needle.nullTerm}) {
           |  $resultTerm = $negation$setTerm.contains(${needle.resultTerm});
           |  $nullTerm = false;
           |}
           |// --- End SEARCH ${target.resultTerm}
           |""".stripMargin.trim

      GeneratedExpression(resultTerm, nullTerm, operatorCode, new BooleanType())
    } else {
      // We copy the target to don't re-evaluate on each range check
      val dummyTarget = target.copy(code = "")

      val rangeToExpression = new RangeToExpression(ctx, sargType, dummyTarget)

      // We use a chain of ORs and range comparisons
      var rangeChecks: Seq[GeneratedExpression] = sarg.rangeSet.asRanges.asScala.toSeq
        .map(RangeSets.map(_, rangeToExpression))

      // Based on https://issues.apache.org/jira/browse/CALCITE-4446 description it is calculated as
      // for sarg.nullAs == RexUnknownAs.TRUE: X IS NULL OR X IN (...)
      // for sarg.nullAs == RexUnknownAs.FALSE: X IS NOT NULL AND (X IN (...))
      // for sarg.nullAs == RexUnknownAs.UNKNOWN: X IN (...)
      if (sarg.nullAs == RexUnknownAs.TRUE) {
        rangeChecks =
          Seq(generateIsNull(target, new BooleanType(target.resultType.isNullable))) ++ rangeChecks
      }

      val generatedRangeChecks = rangeChecks
        .reduce(
          (left, right) =>
            generateOr(
              left,
              right,
              new BooleanType(left.resultType.isNullable || right.resultType.isNullable)))

      val generatedRangeWithIsNotNullIfRequiredChecks =
        if (sarg.nullAs == RexUnknownAs.FALSE)
          generateAnd(
            generatedRangeChecks,
            generateIsNotNull(target, new BooleanType(target.resultType.isNullable)),
            new BooleanType(
              generatedRangeChecks.resultType.isNullable || target.resultType.isNullable)
          )
        else generatedRangeChecks;
      // Add the target expression code
      val finalCode =
        s"""
           |${target.code}
           |// --- Begin SEARCH ${target.resultTerm}
           |${generatedRangeWithIsNotNullIfRequiredChecks.code}
           |// --- End SEARCH ${target.resultTerm}
           |""".stripMargin.trim
      generatedRangeWithIsNotNullIfRequiredChecks.copy(code = finalCode)
    }
  }

  private class RangeToExpression[C <: Comparable[C]](
      ctx: CodeGeneratorContext,
      boundType: LogicalType,
      target: GeneratedExpression)
    extends RangeSets.Handler[C, GeneratedExpression] {

    final val resultTypeForBoolExpr = new BooleanType(
      boundType.isNullable
        || target.resultType.isNullable)

    override def all(): GeneratedExpression = {
      generateLiteral(ctx, true, new BooleanType(false))
    }

    /** lower <= target */
    override def atLeast(lower: C): GeneratedExpression = {
      generateComparison(ctx, "<=", lit(lower), target, resultTypeForBoolExpr)
    }

    /** target <= upper */
    override def atMost(upper: C): GeneratedExpression = {
      generateComparison(ctx, "<=", target, lit(upper), resultTypeForBoolExpr)
    }

    /** lower < target */
    override def greaterThan(lower: C): GeneratedExpression = {
      generateComparison(ctx, "<", lit(lower), target, resultTypeForBoolExpr)
    }

    /** target < upper */
    override def lessThan(upper: C): GeneratedExpression = {
      generateComparison(ctx, "<", target, lit(upper), resultTypeForBoolExpr)
    }

    /** value == target */
    override def singleton(value: C): GeneratedExpression = {
      generateComparison(ctx, "==", lit(value), target, resultTypeForBoolExpr)
    }

    /** lower <= target && target <= upper */
    override def closed(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        generateComparison(ctx, "<=", lit(lower), target, resultTypeForBoolExpr),
        generateComparison(ctx, "<=", target, lit(upper), resultTypeForBoolExpr),
        resultTypeForBoolExpr
      )
    }

    /** lower <= target && target < upper */
    override def closedOpen(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        generateComparison(ctx, "<=", lit(lower), target, resultTypeForBoolExpr),
        generateComparison(ctx, "<", target, lit(upper), resultTypeForBoolExpr),
        resultTypeForBoolExpr
      )
    }

    /** lower < target && target <= upper */
    override def openClosed(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        generateComparison(ctx, "<", lit(lower), target, resultTypeForBoolExpr),
        generateComparison(ctx, "<=", target, lit(upper), resultTypeForBoolExpr),
        resultTypeForBoolExpr
      )
    }

    /** lower < target && target < upper */
    override def open(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        generateComparison(ctx, "<", lit(lower), target, resultTypeForBoolExpr),
        generateComparison(ctx, "<", target, lit(upper), resultTypeForBoolExpr),
        resultTypeForBoolExpr
      )
    }

    private def lit(value: C): GeneratedExpression = {
      generateLiteral(ctx, toFlinkInternalValue(value, boundType), boundType)
    }
  }

}
