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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.catalog.{FunctionCatalog, UnresolvedIdentifier}
import org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall
import org.apache.flink.table.expressions._
import org.apache.flink.table.util.JavaScalaConversionUtil
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.{SqlFunction, SqlKind, SqlPostfixOperator}
import org.apache.calcite.util.{DateString, TimeString, TimestampString}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Date, Time, Timestamp}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object RexProgramExtractor {

  lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Extracts the indices of input fields which accessed by the RexProgram.
    *
    * @param rexProgram The RexProgram to analyze
    * @return The indices of accessed input fields
    */
  def extractRefInputFields(rexProgram: RexProgram): Array[Int] = {
    val visitor = new InputRefVisitor

    // extract referenced input fields from projections
    rexProgram.getProjectList.foreach(
      exp => rexProgram.expandLocalRef(exp).accept(visitor))

    // extract referenced input fields from condition
    val condition = rexProgram.getCondition
    if (condition != null) {
      rexProgram.expandLocalRef(condition).accept(visitor)
    }

    visitor.getFields
  }

  /**
    * Extract condition from RexProgram and convert it into independent CNF expressions.
    *
    * @param rexProgram The RexProgram to analyze
    * @return converted expressions as well as RexNodes which cannot be translated
    */
  def extractConjunctiveConditions(
      rexProgram: RexProgram,
      rexBuilder: RexBuilder,
      catalog: FunctionCatalog): (Array[Expression], Array[RexNode]) = {

    rexProgram.getCondition match {
      case condition: RexLocalRef =>
        val expanded = rexProgram.expandLocalRef(condition)
        // converts the expanded expression to conjunctive normal form,
        // like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
        // CALCITE-4173: expand the Sarg, then converts to expressions.
        val rewrite = if (expanded.getKind == SqlKind.SEARCH) {
          RexUtil.expandSearch(rexBuilder, null, expanded)
        } else {
          expanded
        }
        val cnf = RexUtil.toCnf(rexBuilder, rewrite)
        // converts the cnf condition to a list of AND conditions
        val conjunctions = RelOptUtil.conjunctions(cnf)

        val convertedExpressions = new mutable.ArrayBuffer[Expression]
        val unconvertedRexNodes = new mutable.ArrayBuffer[RexNode]
        val inputNames = rexProgram.getInputRowType.getFieldNames.asScala.toArray
        val converter = new RexNodeToExpressionConverter(rexBuilder, inputNames, catalog)

        conjunctions.asScala.foreach(rex => {
          rex.accept(converter) match {
            case Some(expression) => convertedExpressions += expression
            case None => unconvertedRexNodes += rex
          }
        })
        (convertedExpressions.toArray, unconvertedRexNodes.toArray)

      case _ => (Array.empty, Array.empty)
    }
  }

  /**
    * Extracts the name of nested input fields accessed by the RexProgram and returns the
    * prefix of the accesses.
    *
    * @param rexProgram The RexProgram to analyze
    * @return The full names of accessed input fields. e.g. field.subfield
    */
  def extractRefNestedInputFields(
      rexProgram: RexProgram, usedFields: Array[Int]): Array[Array[String]] = {

    val visitor = new RefFieldAccessorVisitor(usedFields)
    rexProgram.getProjectList.foreach(exp => rexProgram.expandLocalRef(exp).accept(visitor))

    val condition = rexProgram.getCondition
    if (condition != null) {
      rexProgram.expandLocalRef(condition).accept(visitor)
    }
    visitor.getProjectedFields
  }
}

/**
  * An RexVisitor to extract all referenced input fields
  */
class InputRefVisitor extends RexVisitorImpl[Unit](true) {

  private val fields = mutable.LinkedHashSet[Int]()

  def getFields: Array[Int] = fields.toArray

  override def visitInputRef(inputRef: RexInputRef): Unit =
    fields += inputRef.getIndex

  override def visitCall(call: RexCall): Unit =
    call.operands.foreach(operand => operand.accept(this))
}

/**
  * An RexVisitor to convert RexNode to Expression.
  *
  * @param inputNames      The input names of the relation node
  * @param functionCatalog The function catalog
  */
class RexNodeToExpressionConverter(
    rexBuilder: RexBuilder,
    inputNames: Array[String],
    functionCatalog: FunctionCatalog)
    extends RexVisitor[Option[Expression]] {

  override def visitInputRef(inputRef: RexInputRef): Option[Expression] = {
    Preconditions.checkArgument(inputRef.getIndex < inputNames.length)
    Some(PlannerResolvedFieldReference(
      inputNames(inputRef.getIndex),
      FlinkTypeFactory.toTypeInfo(inputRef.getType)
    ))
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): Option[Expression] =
    visitInputRef(rexTableInputRef)

  override def visitLocalRef(localRef: RexLocalRef): Option[Expression] = {
    throw new TableException("Bug: RexLocalRef should have been expanded")
  }

  override def visitLiteral(literal: RexLiteral): Option[Expression] = {
    val literalType = FlinkTypeFactory.toTypeInfo(literal.getType)

    val literalValue = literalType match {

      case _@SqlTimeTypeInfo.DATE =>
        val rexValue = literal.getValueAs(classOf[DateString])
        Date.valueOf(rexValue.toString)

      case _@SqlTimeTypeInfo.TIME =>
        val rexValue = literal.getValueAs(classOf[TimeString])
        Time.valueOf(rexValue.toString(0))

      case _@SqlTimeTypeInfo.TIMESTAMP =>
        val rexValue = literal.getValueAs(classOf[TimestampString])
        Timestamp.valueOf(rexValue.toString(3))

      case _@BasicTypeInfo.BYTE_TYPE_INFO =>
        // convert from BigDecimal to Byte
        literal.getValueAs(classOf[java.lang.Byte])

      case _@BasicTypeInfo.SHORT_TYPE_INFO =>
        // convert from BigDecimal to Short
        literal.getValueAs(classOf[java.lang.Short])

      case _@BasicTypeInfo.INT_TYPE_INFO =>
        // convert from BigDecimal to Integer
        literal.getValueAs(classOf[java.lang.Integer])

      case _@BasicTypeInfo.LONG_TYPE_INFO =>
        // convert from BigDecimal to Long
        literal.getValueAs(classOf[java.lang.Long])

      case _@BasicTypeInfo.FLOAT_TYPE_INFO =>
        // convert from BigDecimal to Float
        literal.getValueAs(classOf[java.lang.Float])

      case _@BasicTypeInfo.DOUBLE_TYPE_INFO =>
        // convert from BigDecimal to Double
        literal.getValueAs(classOf[java.lang.Double])

      case _@BasicTypeInfo.STRING_TYPE_INFO =>
        // convert from NlsString to String
        literal.getValueAs(classOf[java.lang.String])

      case _@BasicTypeInfo.BOOLEAN_TYPE_INFO =>
        // convert to Boolean
        literal.getValueAs(classOf[java.lang.Boolean])

      case _@BasicTypeInfo.BIG_DEC_TYPE_INFO =>
        // convert to BigDecimal
        literal.getValueAs(classOf[java.math.BigDecimal])

      case _ =>
        // Literal type is not supported.
        RexProgramExtractor.LOG.debug(
          "Literal {} of SQL type {} is not supported and cannot be converted. " +
            "Please reach out to the community if you think this type should be supported.",
          Array(literal, literal.getType): _*)
        return None
    }

    Some(Literal(literalValue, literalType))
  }

  /** Expands the SEARCH into normal disjunctions recursively. */
  private def expandSearch(rexBuilder: RexBuilder, rex: RexNode): RexNode = {
    val shuttle = new RexShuttle() {
      override def visitCall(call: RexCall): RexNode = {
        if (call.getKind == SqlKind.SEARCH) {
          RexUtil.expandSearch(rexBuilder, null, call)
        } else {
          super.visitCall(call)
        }
      }
    }
    rex.accept(shuttle)
  }

  override def visitCall(oriRexCall: RexCall): Option[Expression] = {
    val call = expandSearch(
      rexBuilder,
      oriRexCall).asInstanceOf[RexCall]
    val operands = call.getOperands.map(
      operand => operand.accept(this).orNull
    )

    // return null if we cannot translate all the operands of the call
    if (operands.contains(null)) {
      None
    } else {
        // TODO we cast to planner expression as a temporary solution to keep the old interfaces
        call.getOperator match {
          case SqlStdOperatorTable.OR =>
            Option(operands.reduceLeft { (l, r) =>
              Or(l.asInstanceOf[PlannerExpression], r.asInstanceOf[PlannerExpression])
            })
          case SqlStdOperatorTable.AND =>
            Option(operands.reduceLeft { (l, r) =>
              And(l.asInstanceOf[PlannerExpression], r.asInstanceOf[PlannerExpression])
            })
          case function: SqlFunction =>
            lookupFunction(replace(function.getName), operands)
          case postfix: SqlPostfixOperator =>
            lookupFunction(replace(postfix.getName), operands)
          case operator@_ =>
            lookupFunction(replace(s"${operator.getKind}"), operands)
      }
    }
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Option[Expression] = None

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): Option[Expression] = None

  override def visitRangeRef(rangeRef: RexRangeRef): Option[Expression] = None

  override def visitSubQuery(subQuery: RexSubQuery): Option[Expression] = None

  override def visitDynamicParam(dynamicParam: RexDynamicParam): Option[Expression] = None

  override def visitOver(over: RexOver): Option[Expression] = None

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): Option[Expression] = None

  private def lookupFunction(name: String, operands: Seq[Expression]): Option[Expression] = {
    // TODO we assume only planner expression as a temporary solution to keep the old interfaces
    val expressionBridge = new ExpressionBridge[PlannerExpression](
      PlannerExpressionConverter.INSTANCE)
    JavaScalaConversionUtil.toScala(functionCatalog.lookupFunction(UnresolvedIdentifier.of(name)))
      .flatMap(result =>
        Try(expressionBridge.bridge(
          unresolvedCall(result.getFunctionDefinition, operands: _*))).toOption
      )
  }

  private def replace(str: String): String = {
    str.replaceAll("\\s|_", "")
  }
}

/**
  * A RexVisitor to extract used nested input fields
  */
class RefFieldAccessorVisitor(usedFields: Array[Int]) extends RexVisitorImpl[Unit](true) {

  private val projectedFields: Array[Array[String]] = Array.fill(usedFields.length)(Array.empty)

  private val order: Map[Int, Int] = usedFields.zipWithIndex.toMap

  /** Returns the prefix of the nested field accesses */
  def getProjectedFields: Array[Array[String]] = {

    projectedFields.map { nestedFields =>
      // sort nested field accesses
      val sorted = nestedFields.sorted
      // get prefix field accesses
      val prefixAccesses = sorted.foldLeft(Nil: List[String]) {
        (prefixAccesses, nestedAccess) => prefixAccesses match {
              // first access => add access
            case Nil => List[String](nestedAccess)
              // top-level access already found => return top-level access
            case head :: Nil if head.equals("*") => prefixAccesses
              // access is top-level access => return top-level access
            case _ :: _ if nestedAccess.equals("*") => List("*")
            // previous access is not prefix of this access => add access
            case head :: _ if !nestedAccess.startsWith(head) =>
              nestedAccess :: prefixAccesses
              // previous access is a prefix of this access => do not add access
            case _ => prefixAccesses
          }
      }
      prefixAccesses.toArray
    }
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Unit = {
    def internalVisit(fieldAccess: RexFieldAccess): (Int, String) = {
      fieldAccess.getReferenceExpr match {
        case ref: RexInputRef =>
          (ref.getIndex, fieldAccess.getField.getName)
        case fac: RexFieldAccess =>
          val (i, n) = internalVisit(fac)
          (i, s"$n.${fieldAccess.getField.getName}")
      }
    }
    val (index, fullName) = internalVisit(fieldAccess)
    val outputIndex = order.getOrElse(index, -1)
    val fields: Array[String] = projectedFields(outputIndex)
    projectedFields(outputIndex) = fields :+ fullName
  }

  override def visitInputRef(inputRef: RexInputRef): Unit = {
    val outputIndex = order.getOrElse(inputRef.getIndex, -1)
    val fields: Array[String] = projectedFields(outputIndex)
    projectedFields(outputIndex) = fields :+ "*"
  }

  override def visitCall(call: RexCall): Unit =
    call.operands.foreach(operand => operand.accept(this))
}
