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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.util.Preconditions

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.{SqlStdOperatorTable, SqlTrimFunction}
import org.apache.calcite.sql.{SqlFunction, SqlPostfixOperator}
import org.apache.calcite.util.{DateString, TimeString, TimestampString}

import java.sql.{Date, Time, Timestamp}
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object RexNodeExtractor {

  /**
    * Extracts the indices of input fields which accessed by the expressions.
    *
    * @param exprs The RexNode list to analyze
    * @return The indices of accessed input fields
    */
  def extractRefInputFields(exprs: JList[RexNode]): Array[Int] = {
    val visitor = new InputRefVisitor
    // extract referenced input fields from expressions
    exprs.foreach(_.accept(visitor))
    visitor.getFields
  }

  /**
    * Convert rexNode into independent CNF expressions.
    *
    * @param expr            The RexNode to analyze
    * @param inputFieldNames The input names of the RexNode
    * @param rexBuilder      The factory to build CNF expressions
    * @param catalog         The function catalog
    * @return
    */
  def extractConjunctiveConditions(
      expr: RexNode,
      maxCnfNodeCount: Int,
      inputFieldNames: JList[String],
      rexBuilder: RexBuilder,
      catalog: FunctionCatalog): (Array[Expression], Array[RexNode]) = {

    // converts the expanded expression to conjunctive normal form,
    // like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
    val cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, expr)
    // converts the cnf condition to a list of AND conditions
    val conjunctions = RelOptUtil.conjunctions(cnf)

    val convertedExpressions = new mutable.ArrayBuffer[Expression]
    val unconvertedRexNodes = new mutable.ArrayBuffer[RexNode]
    val inputNames = inputFieldNames.asScala.toArray
    val converter = new RexNodeToExpressionConverter(inputNames, catalog)

    conjunctions.asScala.foreach(rex => {
      rex.accept(converter) match {
        case Some(expression) => convertedExpressions += expression
        case None => unconvertedRexNodes += rex
      }
    })
    (convertedExpressions.toArray, unconvertedRexNodes.toArray)

  }

  /**
    * Extracts the name of nested input fields accessed by the expressions and returns the
    * prefix of the accesses.
    *
    * @param exprs The expressions to analyze
    * @return The full names of accessed input fields. e.g. field.subfield
    */
  def extractRefNestedInputFields(
      exprs: JList[RexNode], usedFields: Array[Int]): Array[Array[String]] = {

    val visitor = new RefFieldAccessorVisitor(usedFields)
    exprs.foreach(_.accept(visitor))
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
    inputNames: Array[String],
    functionCatalog: FunctionCatalog)
    extends RexVisitor[Option[Expression]] {

  override def visitInputRef(inputRef: RexInputRef): Option[Expression] = {
    Preconditions.checkArgument(inputRef.getIndex < inputNames.length)
    Some(ResolvedFieldReference(
      inputNames(inputRef.getIndex),
      FlinkTypeFactory.toInternalType(inputRef.getType)
    ))
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): Option[Expression] =
    visitInputRef(rexTableInputRef)

  override def visitLocalRef(localRef: RexLocalRef): Option[Expression] = {
    throw new TableException("Bug: RexLocalRef should have been expanded")
  }

  override def visitLiteral(literal: RexLiteral): Option[Expression] = {
    literal.getValue match {
      case f: SqlTrimFunction.Flag =>
        val trimMode = f match {
          case SqlTrimFunction.Flag.BOTH => TrimMode.BOTH
          case SqlTrimFunction.Flag.LEADING => TrimMode.LEADING
          case SqlTrimFunction.Flag.TRAILING => TrimMode.TRAILING
        }
        return Some(trimMode)
      case _ => // do nothing
    }

    val literalType = FlinkTypeFactory.toInternalType(literal.getType)
    val literalValue = literalType match {

      // deal with time related values, taking care of time zone
      case _@DataTypes.DATE =>
        val v = literal.getValueAs(classOf[DateString])
        new Date(DateTimeUtils.dateStringToUnixDate(v.toString) * DateTimeUtils.MILLIS_PER_DAY)
      case _@DataTypes.TIME =>
        val v = literal.getValueAs(classOf[TimeString])
        new Time(DateTimeUtils.timeStringToUnixDate(v.toString(0)).longValue())
      case _@DataTypes.TIMESTAMP =>
        val v = literal.getValueAs(classOf[TimestampString])
        new Timestamp(DateTimeUtils.timestampStringToUnixDate(v.toString(3)))

      // manually convert all primitive numeric values since calcite will wrap these
      // with BigDecimal, see [[RexBuilder.makeExactLiteral]]
      case _@DataTypes.BYTE => literal.getValueAs(classOf[java.lang.Byte])
      case _@DataTypes.SHORT => literal.getValueAs(classOf[java.lang.Short])
      case _@DataTypes.INT => literal.getValueAs(classOf[java.lang.Integer])
      case _@DataTypes.LONG => literal.getValueAs(classOf[java.lang.Long])
      case _@DataTypes.FLOAT => literal.getValueAs(classOf[java.lang.Float])
      case _@DataTypes.DOUBLE => literal.getValueAs(classOf[java.lang.Double])

      case _ => literal.getValue
    }
    Some(Literal(literalValue, literalType))
  }

  override def visitCall(call: RexCall): Option[Expression] = {
    val operands = call.getOperands.map(
      operand => operand.accept(this).orNull
    )

    // return null if we cannot translate all the operands of the call
    if (operands.contains(null)) {
      None
    } else {
      call.getOperator match {
        case SqlStdOperatorTable.OR =>
          Option(operands.reduceLeft(Or))
        case SqlStdOperatorTable.AND =>
          Option(operands.reduceLeft(And))
        case SqlStdOperatorTable.CAST =>
          Option(Cast(operands.head, FlinkTypeFactory.toInternalType(call.getType)))
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
    Try(functionCatalog.lookupFunction(name, operands)) match {
      case Success(expr) => Some(expr)
      case Failure(_) => None
    }
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
