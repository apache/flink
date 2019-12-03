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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, FunctionLookup, UnresolvedIdentifier}
import org.apache.flink.table.dataformat.DataFormatConverters.{LocalDateConverter, LocalDateTimeConverter, LocalTimeConverter}
import org.apache.flink.table.dataformat.SqlTimestamp
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.utils.ApiExpressionUtils._
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.{AND, CAST, OR}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils.unixTimestampToLocalDateTime
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.util.Preconditions
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.{SqlStdOperatorTable, SqlTrimFunction}
import org.apache.calcite.sql.{SqlFunction, SqlPostfixOperator}
import org.apache.calcite.util.{TimestampString, Util}
import java.util
import java.util.{TimeZone, List => JList}

import org.apache.flink.table.runtime.functions.SqlDateTimeUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object RexNodeExtractor extends Logging {

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
    * Extracts the name of nested input fields accessed by the expressions and returns the
    * prefix of the accesses.
    *
    * @param exprs The expressions to analyze
    * @param usedFields indices of used input fields
    * @return The full names of accessed input fields. e.g. field.subfield
    */
  def extractRefNestedInputFields(
      exprs: JList[RexNode],
      usedFields: Array[Int]): Array[Array[String]] = {
    val visitor = new RefFieldAccessorVisitor(usedFields)
    exprs.foreach(_.accept(visitor))
    visitor.getProjectedFields
  }

  /**
    * Convert rexNode into independent CNF expressions.
    *
    * @param expr            The RexNode to analyze
    * @param inputFieldNames The input names of the RexNode
    * @param rexBuilder      The factory to build CNF expressions
    * @param functionCatalog The function catalog
    * @return converted expressions and unconverted rex nodes
    */
  def extractConjunctiveConditions(
      expr: RexNode,
      maxCnfNodeCount: Int,
      inputFieldNames: JList[String],
      rexBuilder: RexBuilder,
      functionCatalog: FunctionCatalog,
      catalogManager: CatalogManager,
      timeZone: TimeZone): (Array[Expression], Array[RexNode]) = {
    // converts the expanded expression to conjunctive normal form,
    // like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
    val cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, expr)
    // converts the cnf condition to a list of AND conditions
    val conjunctions = RelOptUtil.conjunctions(cnf)

    val convertedExpressions = new mutable.ArrayBuffer[Expression]
    val unconvertedRexNodes = new mutable.ArrayBuffer[RexNode]
    val inputNames = inputFieldNames.asScala.toArray
    val converter = new RexNodeToExpressionConverter(
      inputNames, functionCatalog, catalogManager, timeZone)

    conjunctions.asScala.foreach(rex => {
      rex.accept(converter) match {
        case Some(expression) => convertedExpressions += expression
        case None => unconvertedRexNodes += rex
      }
    })
    (convertedExpressions.toArray, unconvertedRexNodes.toArray)
  }

  /**
    * Extract partition predicate from filter condition.
    *
    * @param expr            The RexNode to analyze
    * @param inputFieldNames The input names of the RexNode
    * @param rexBuilder      The factory to build CNF expressions
    * @param partitionFieldNames Partition field names.
    * @return Partition predicates and non-partition predicates.
    */
  def extractPartitionPredicates(
      expr: RexNode,
      maxCnfNodeCount: Int,
      inputFieldNames: Array[String],
      rexBuilder: RexBuilder,
      partitionFieldNames: Array[String]): (RexNode, RexNode) = {
    // converts the expanded expression to conjunctive normal form,
    // like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
    val cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, expr)
    // converts the cnf condition to a list of AND conditions
    val conjunctions = RelOptUtil.conjunctions(cnf)

    val (partitionPredicates, nonPartitionPredicates) =
      conjunctions.partition(isSupportedPartitionPredicate(_, partitionFieldNames, inputFieldNames))
    val partitionPredicate = RexUtil.composeConjunction(rexBuilder, partitionPredicates)
    val nonPartitionPredicate = RexUtil.composeConjunction(rexBuilder, nonPartitionPredicates)
    (partitionPredicate, nonPartitionPredicate)
  }

  /**
    * returns true if the given predicate only contains [[RexInputRef]], [[RexLiteral]] and
    * [[RexCall]], and all [[RexInputRef]]s reference partition fields. otherwise false.
    */
  private def isSupportedPartitionPredicate(
      predicate: RexNode,
      partitionFieldNames: Array[String],
      inputFieldNames: Array[String]): Boolean = {
    val visitor = new RexVisitorImpl[Boolean](true) {
      override def visitInputRef(inputRef: RexInputRef): Boolean = {
        val fieldName = inputFieldNames.apply(inputRef.getIndex)
        val typeRoot = FlinkTypeFactory.toLogicalType(inputRef.getType).getTypeRoot
        if (!partitionFieldNames.contains(fieldName) ||
          !PartitionPruner.supportedPartitionFieldTypes.contains(typeRoot)) {
          throw new Util.FoundOne(false)
        } else {
          super.visitInputRef(inputRef)
        }
      }

      override def visitLocalRef(localRef: RexLocalRef): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitOver(over: RexOver): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitCorrelVariable(correlVariable: RexCorrelVariable): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitDynamicParam(dynamicParam: RexDynamicParam): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitRangeRef(rangeRef: RexRangeRef): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitFieldAccess(fieldAccess: RexFieldAccess): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitSubQuery(subQuery: RexSubQuery): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitTableInputRef(ref: RexTableInputRef): Boolean = {
        throw new Util.FoundOne(false)
      }

      override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): Boolean = {
        throw new Util.FoundOne(false)
      }
    }

    try {
      predicate.accept(visitor)
      true
    } catch {
      case _: Util.FoundOne => false
    }
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
        (prefixAccesses, nestedAccess) =>
          prefixAccesses match {
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

/**
  * An RexVisitor to convert RexNode to Expression.
  *
  * @param inputNames      The input names of the relation node
  * @param functionCatalog The function catalog
  */
class RexNodeToExpressionConverter(
    inputNames: Array[String],
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager,
    timeZone: TimeZone)
  extends RexVisitor[Option[ResolvedExpression]] {

  override def visitInputRef(inputRef: RexInputRef): Option[ResolvedExpression] = {
    Preconditions.checkArgument(inputRef.getIndex < inputNames.length)
    Some(new FieldReferenceExpression(
      inputNames(inputRef.getIndex),
      fromLogicalTypeToDataType(FlinkTypeFactory.toLogicalType(inputRef.getType)),
      0,
      inputRef.getIndex
    ))
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): Option[ResolvedExpression] =
    visitInputRef(rexTableInputRef)

  override def visitLocalRef(localRef: RexLocalRef): Option[ResolvedExpression] = {
    throw new TableException("Bug: RexLocalRef should have been expanded")
  }

  override def visitLiteral(literal: RexLiteral): Option[ResolvedExpression] = {
    // TODO support SqlTrimFunction.Flag
    literal.getValue match {
      case _: SqlTrimFunction.Flag => return None
      case _ => // do nothing
    }

    val literalType = FlinkTypeFactory.toLogicalType(literal.getType)

    val literalValue = literalType.getTypeRoot match {

      case DATE =>
        val v = literal.getValueAs(classOf[Integer])
        LocalDateConverter.INSTANCE.toExternal(v)

      case TIME_WITHOUT_TIME_ZONE =>
        val v = literal.getValueAs(classOf[Integer])
        LocalTimeConverter.INSTANCE.toExternal(v)

      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val v = literal.getValueAs(classOf[TimestampString])
        val millisecond = v.getMillisSinceEpoch
        val nanoOfMillisecond = SqlDateTimeUtils.getNanoOfMillisSinceEpoch(v.toString)
        SqlTimestamp.fromEpochMillis(millisecond, nanoOfMillisecond).toLocalDateTime

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val v = literal.getValueAs(classOf[java.lang.Long])
        unixTimestampToLocalDateTime(v).atZone(timeZone.toZoneId).toInstant

      case TINYINT =>
        // convert from BigDecimal to Byte
        literal.getValueAs(classOf[java.lang.Byte])

      case SMALLINT =>
        // convert from BigDecimal to Short
        literal.getValueAs(classOf[java.lang.Short])

      case INTEGER =>
        // convert from BigDecimal to Integer
        literal.getValueAs(classOf[java.lang.Integer])

      case BIGINT =>
        // convert from BigDecimal to Long
        literal.getValueAs(classOf[java.lang.Long])

      case FLOAT =>
        // convert from BigDecimal to Float
        literal.getValueAs(classOf[java.lang.Float])

      case DOUBLE =>
        // convert from BigDecimal to Double
        literal.getValueAs(classOf[java.lang.Double])

      case VARCHAR | CHAR =>
        // convert from NlsString to String
        literal.getValueAs(classOf[java.lang.String])

      case BOOLEAN =>
        // convert to Boolean
        literal.getValueAs(classOf[java.lang.Boolean])

      case DECIMAL =>
        // convert to BigDecimal
        literal.getValueAs(classOf[java.math.BigDecimal])

      case _ =>
        literal.getValue
    }

    Some(valueLiteral(literalValue,
      fromLogicalTypeToDataType(literalType)))
  }

  override def visitCall(rexCall: RexCall): Option[ResolvedExpression] = {
    val operands = rexCall.getOperands.map(
      operand => operand.accept(this).orNull
    )

    val outputType = fromLogicalTypeToDataType(FlinkTypeFactory.toLogicalType(rexCall.getType))

    // return null if we cannot translate all the operands of the call
    if (operands.contains(null)) {
      None
    } else {
      rexCall.getOperator match {
        case SqlStdOperatorTable.OR =>
          Option(operands.reduceLeft((l, r) => new CallExpression(OR, Seq(l, r), outputType)))
        case SqlStdOperatorTable.AND =>
          Option(operands.reduceLeft((l, r) => new CallExpression(AND, Seq(l, r), outputType)))
        case SqlStdOperatorTable.CAST =>
          Option(new CallExpression(CAST, Seq(operands.head, typeLiteral(outputType)), outputType))
        case _: SqlFunction | _: SqlPostfixOperator =>
          val names = new util.ArrayList[String](rexCall.getOperator.getNameAsId.names)
          names.set(names.size() - 1, replace(names.get(names.size() - 1)))
          val id = UnresolvedIdentifier.of(names.asScala.toArray: _*)
          lookupFunction(id, operands, outputType)
        case operator@_ =>
          lookupFunction(
            UnresolvedIdentifier.of(replace(s"${operator.getKind}")),
            operands,
            outputType)
      }
    }
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Option[ResolvedExpression] = None

  override def visitCorrelVariable(
      correlVariable: RexCorrelVariable): Option[ResolvedExpression] = None

  override def visitRangeRef(rangeRef: RexRangeRef): Option[ResolvedExpression] = None

  override def visitSubQuery(subQuery: RexSubQuery): Option[ResolvedExpression] = None

  override def visitDynamicParam(dynamicParam: RexDynamicParam): Option[ResolvedExpression] = None

  override def visitOver(over: RexOver): Option[ResolvedExpression] = None

  override def visitPatternFieldRef(
      fieldRef: RexPatternFieldRef): Option[ResolvedExpression] = None

  private def lookupFunction(
      identifier: UnresolvedIdentifier,
      operands: Seq[ResolvedExpression],
      outputType: DataType): Option[ResolvedExpression] = {
    Try(functionCatalog.lookupFunction(identifier)) match {
      case Success(f: java.util.Optional[FunctionLookup.Result]) =>
        if (f.isPresent) {
          Some(new CallExpression(f.get().getFunctionDefinition, operands, outputType))
        } else {
          None
        }
      case Failure(_) => None
    }
  }

  private def replace(str: String): String = {
    str.replaceAll("\\s|_", "")
  }

}
