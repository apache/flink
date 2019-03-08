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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.table.CalcitePair
import org.apache.flink.table.`type`.InternalType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.dataview.DataViewSpec
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.ExpressionFormat
import org.apache.flink.table.plan.nodes.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.plan.nodes.physical.batch.BatchPhysicalRel
import org.apache.flink.table.typeutils.BinaryRowSerializer

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Calc, Window}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelFieldCollation, RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode, RexProgram, RexWindowBound}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.mapping.IntPair

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Utility methods for RelNode.
  */
object RelNodeUtil {

  /**
    * Returns indices of group functions.
    */
  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
      call.getAggregation.getKind match {
        case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
        case _ => false
      }
    }.map { case (_, idx) => idx }
  }

  /**
    * Returns whether any of the aggregates are accurate DISTINCT.
    *
    * @return Whether any of the aggregates are accurate DISTINCT
    */
  def containsAccurateDistinctCall(aggCalls: Seq[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && !call.isApproximate)
  }

  /**
    * Converts field names corresponding to given indices to String.
    */
  def fieldToString(fieldIndices: Array[Int], inputType: RelDataType): String = {
    val fieldNames = inputType.getFieldNames
    fieldIndices.map(fieldNames(_)).mkString(", ")
  }

  /**
    * Converts sort fields to String.
    */
  def orderingToString(
      orderFields: util.List[RelFieldCollation],
      inputType: RelDataType): String = {
    val fieldNames = inputType.getFieldNames
    orderFields.map {
      x => s"${fieldNames(x.getFieldIndex)} ${x.direction.shortString}"
    }.mkString(", ")
  }

  /**
    * Converts group aggregate attributes to String.
    */
  def groupAggregationToString(
      inputRowType: RelDataType,
      outputRowType: RelDataType,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      isMerge: Boolean,
      isGlobal: Boolean,
      distinctInfos: Seq[DistinctInfo] = Seq()): String = {
    val prefix = if (isMerge) {
      "Final_"
    } else if (!isGlobal) {
      "Partial_"
    } else {
      ""
    }

    val inputFieldNames = inputRowType.getFieldNames
    val outputFieldNames = outputRowType.getFieldNames
    val fullGrouping = grouping ++ auxGrouping

    val distinctFieldNames = distinctInfos.indices.map(index => s"distinct$$$index")
    val distinctStrings = if (isMerge) {
      // not output distinct fields in global merge
      Seq()
    } else {
      distinctInfos.map { distinct =>
        val argListNames = distinct.argIndexes.map(inputFieldNames).mkString(",")
        // TODO Refactor local&global aggregate name
        val filterNames = distinct.filterArgs.filter(_ > 0).map(inputFieldNames).mkString(", ")
        if (filterNames.nonEmpty) {
          s"DISTINCT($argListNames) FILTER ($filterNames)"
        } else {
          s"DISTINCT($argListNames)"
        }
      }
    }

    val aggToDistinctMapping = mutable.HashMap.empty[Int, String]
    distinctInfos.zipWithIndex.foreach {
      case (distinct, index) =>
        distinct.aggIndexes.foreach {
          aggIndex =>
            aggToDistinctMapping += (aggIndex -> distinctFieldNames(index))
        }
    }

    // agg
    var offset = fullGrouping.length
    val aggStrings = aggCallToAggFunction.zipWithIndex.map {
      case ((aggCall, udf), index) =>
        val distinct = if (aggCall.isDistinct) {
          if (aggCall.getArgList.size() == 0) {
            "DISTINCT"
          } else {
            "DISTINCT "
          }
        } else {
          if (isMerge && aggToDistinctMapping.contains(index)) {
            "DISTINCT "
          } else {
            ""
          }
        }
        var newArgList = aggCall.getArgList.map(_.toInt).toList
        if (isMerge) {
          newArgList = udf match {
            case _: AggregateFunction[_, _] =>
              val argList = List(offset)
              offset = offset + 1
              argList
            case _ =>
              // TODO supports DeclarativeAggregateFunction
              throw new TableException("Unsupported now")
          }
        }
        val argListNames = if (aggToDistinctMapping.contains(index)) {
          aggToDistinctMapping(index)
        } else if (newArgList.nonEmpty) {
          newArgList.map(inputFieldNames(_)).mkString(", ")
        } else {
          "*"
        }

        if (aggCall.filterArg >= 0 && aggCall.filterArg < inputFieldNames.size) {
          s"${aggCall.getAggregation}($distinct$argListNames) FILTER " +
            s"${inputFieldNames(aggCall.filterArg)}"
        } else {
          s"${aggCall.getAggregation}($distinct$argListNames)"
        }
    }

    //output for agg
    val aggFunctions = aggCallToAggFunction.map(_._2)
    offset = fullGrouping.length
    val outFieldNames = aggFunctions.map { udf =>
      val outFieldName = if (isGlobal) {
        val name = outputFieldNames(offset)
        offset = offset + 1
        name
      } else {
        udf match {
          case _: AggregateFunction[_, _] =>
            val name = outputFieldNames(offset)
            offset = offset + 1
            name
          case _ =>
            // TODO supports DeclarativeAggregateFunction
            throw new TableException("Unsupported now")
        }
      }
      outFieldName
    }

    (fullGrouping.map(inputFieldNames(_)) ++ aggStrings ++ distinctStrings).zip(
      fullGrouping.indices.map(outputFieldNames(_)) ++ outFieldNames ++ distinctFieldNames).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$prefix$f AS $o"
      }
    }.mkString(", ")
  }

  /**
    * Converts over aggregate attributes to String.
    */
  def overAggregationToString(
      inputRowType: RelDataType,
      outputRowType: RelDataType,
      constants: Seq[RexLiteral],
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      outputInputName: Boolean = true,
      rowTypeOffset: Int = 0): String = {

    val inputFieldNames = inputRowType.getFieldNames
    val outputFieldNames = outputRowType.getFieldNames

    val aggStrings = namedAggregates.map(_.getKey).map(
      a => s"${a.getAggregation}(${
        val prefix = if (a.isDistinct) {
          "DISTINCT "
        } else {
          ""
        }
        prefix + (if (a.getArgList.size() > 0) {
          a.getArgList.map { arg =>
            // index to constant
            if (arg >= inputRowType.getFieldCount) {
              constants(arg - inputRowType.getFieldCount)
            }
            // index to input field
            else {
              inputFieldNames(arg)
            }
          }.mkString(", ")
        } else {
          "*"
        })
      })")

    val output = if (outputInputName) inputFieldNames ++ aggStrings else aggStrings
    output.zip(outputFieldNames.drop(rowTypeOffset)).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }.mkString(", ")
  }

  /**
    * Computes [[Calc]] cost.
    */
  def computeCalcCost(
      calc: Calc,
      planner: RelOptPlanner,
      mq: RelMetadataQuery): RelOptCost = {
    val calcProgram = calc.getProgram
    // compute number of expressions that do not access a field or literal, i.e. computations,
    // conditions, etc. We only want to account for computations, not for simple projections.
    // CASTs in RexProgram are reduced as far as possible by ReduceExpressionsRule
    // in normalization stage. So we should ignore CASTs here in optimization stage.
    val compCnt = calcProgram.getProjectList.map(calcProgram.expandLocalRef).toList.count {
      case _: RexInputRef => false
      case _: RexLiteral => false
      case c: RexCall if c.getOperator.getName.equals("CAST") => false
      case _ => true
    }
    val newRowCnt = mq.getRowCount(calc)
    // TODO use inputRowCnt to compute cpu cost
    planner.getCostFactory.makeCost(newRowCnt, newRowCnt * compCnt, 0)
  }

  def computeSortMemory(mq: RelMetadataQuery, inputOfSort: RelNode): Double = {
    //TODO It's hard to make sure that the normalized key's length is accurate in optimized stage.
    // use SortCodeGenerator.MAX_NORMALIZED_KEY_LEN instead of 16
    val normalizedKeyBytes = 16
    val rowCount = mq.getRowCount(inputOfSort)
    val averageRowSize = BatchPhysicalRel.binaryRowAverageSize(inputOfSort)
    val recordAreaInBytes = rowCount * (averageRowSize + BinaryRowSerializer.LENGTH_SIZE_IN_BYTES)
    // TODO use BinaryIndexedSortable.OFFSET_LEN instead of 8
    val indexAreaInBytes = rowCount * (normalizedKeyBytes + 8)
    recordAreaInBytes + indexAreaInBytes
  }

  /**
    * Converts [[RexNode]] to String.
    */
  def expressionToString(
      expr: RexNode,
      inputType: RelDataType,
      expressionFunc: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    if (expr != null) {
      val inputFieldNames = inputType.getFieldNames.toList
      expressionFunc(expr, inputFieldNames, None)
    } else {
      ""
    }
  }

  /**
    * Converts [[RexProgram]] to String.
    */
  def programToString(
      program: RexProgram,
      expressionFunc: (RexNode, List[String], Option[List[RexNode]], ExpressionFormat) => String,
      expressionFormat: ExpressionFormat = ExpressionFormat.Prefix): (String, String) = {
    val condition = program.getCondition
    val projectList = program.getProjectList.toList
    val inputFieldNames = program.getInputRowType.getFieldNames.toList
    val localExprs = program.getExprList.toList
    val outputFieldNames = program.getOutputRowType.getFieldNames.toList

    val conditionStr = if (condition != null) {
      expressionFunc(condition, inputFieldNames, Some(localExprs), expressionFormat)
    } else {
      ""
    }
    val projectStr = projectList
      .map(expressionFunc(_, inputFieldNames, Some(localExprs), expressionFormat))
      .zip(outputFieldNames).map {
      case (e, o) =>
        if (e != o) {
          e + " AS " + o
        } else {
          e
        }
    }.mkString(", ")

    (conditionStr, projectStr)
  }

  /**
    * Converts project list to String.
    */
  def projectsToString(
      projects: util.List[util.List[RexNode]],
      inputRowType: RelDataType,
      outputRowType: RelDataType): String = {
    val inFieldNames = inputRowType.getFieldNames
    val outFieldNames = outputRowType.getFieldNames
    projects.map { project =>
      project.zipWithIndex.map {
        case (r: RexInputRef, i) =>
          val inputFieldName = inFieldNames.get(r.getIndex)
          val outputFieldName = outFieldNames.get(i)
          if (inputFieldName != outputFieldName) {
            s"$inputFieldName AS $outputFieldName"
          } else {
            outputFieldName
          }
        case (l: RexLiteral, i) => s"${l.getValue3} AS ${outFieldNames.get(i)}"
        case (_, i) => outFieldNames.get(i)
      }.mkString("{", ", ", "}")
    }.mkString(", ")
  }

  /**
    * Converts [[FlinkJoinRelType]] to String.
    */
  def joinTypeToString(joinType: FlinkJoinRelType): String = {
    joinType match {
      case FlinkJoinRelType.INNER => "InnerJoin"
      case FlinkJoinRelType.LEFT => "LeftOuterJoin"
      case FlinkJoinRelType.RIGHT => "RightOuterJoin"
      case FlinkJoinRelType.FULL => "FullOuterJoin"
      case FlinkJoinRelType.SEMI => "LeftSemiJoin"
      case FlinkJoinRelType.ANTI => "LeftAntiJoin"
    }
  }

  /**
    * Converts join attributes to String.
    */
  def joinToString(
      inputType: RelDataType,
      joinCondition: RexNode,
      joinType: FlinkJoinRelType,
      expressionFunc: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    s"${joinTypeToString(joinType)}" +
      s"(where: (${expressionToString(joinCondition, inputType, expressionFunc)}), " +
      s"join: (${inputType.getFieldNames.mkString(", ")}))"
  }

  def joinExplainTerms(
      pw: RelWriter,
      joinCondition: RexNode,
      joinType: FlinkJoinRelType,
      inputRowType: RelDataType,
      outputRowType: RelDataType,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): RelWriter = {
    pw.item("where", RelNodeUtil.expressionToString(joinCondition, inputRowType, expression))
      .item("join", outputRowType.getFieldNames.mkString(", "))
      .item("joinType", joinTypeToString(joinType))
  }

  /**
    * Check and get join left and right keys.
    */
  def checkAndGetJoinKeys(
      keyPairs: List[IntPair],
      left: RelNode,
      right: RelNode,
      allowEmptyKey: Boolean = false): (Array[Int], Array[Int]) = {
    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      if (allowEmptyKey) {
        (leftKeys.toArray, rightKeys.toArray)
      } else {
        throw new TableException(
          s"Joins should have at least one equality condition.\n" +
            s"\tleft: ${left.toString}\n\tright: ${right.toString}\n" +
            s"please re-check the join statement and make sure there's " +
            "equality condition for join.")
      }
    } else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach { pair =>
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.add(pair.source)
          rightKeys.add(pair.target)
        } else {
          throw new TableException(
            s"Join: Equality join predicate on incompatible types. " +
              s"\tLeft: ${left.toString}\n\tright: ${right.toString}\n" +
              "please re-check the join statement.")
        }
      }
      (leftKeys.toArray, rightKeys.toArray)
    }
  }

  /**
    * Returns limit start value (never null).
    */
  def getLimitStart(offset: RexNode): Long = if (offset != null) RexLiteral.intValue(offset) else 0L

  /**
    * Returns limit end value (never null).
    */
  def getLimitEnd(offset: RexNode, fetch: RexNode): Long = {
    if (fetch != null) {
      getLimitStart(offset) + RexLiteral.intValue(fetch)
    } else {
      Long.MaxValue
    }
  }

  /**
    * Converts sort fetch to String.
    */
  def fetchToString(fetch: RexNode): String = {
    if (fetch != null) {
      s"${RexLiteral.intValue(fetch)}"
    } else {
      "unlimited"
    }
  }

  /**
    * Converts [[Direction]] to [[Order]].
    */
  def directionToOrder(direction: Direction): Order = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }
  }

  /**
    * Gets sort key indices, sort orders and null directions from given field collations.
    */
  def getSortKeysAndOrders(
      fieldCollations: Seq[RelFieldCollation]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val fieldMappingDirections = fieldCollations.map {
      c => (c.getFieldIndex, directionToOrder(c.getDirection))
    }
    val keys = fieldMappingDirections.map(_._1)
    val orders = fieldMappingDirections.map(_._2 == Order.ASCENDING)
    val nullsIsLast = fieldCollations.map(_.nullDirection).map {
      case RelFieldCollation.NullDirection.LAST => true
      case RelFieldCollation.NullDirection.FIRST => false
      case RelFieldCollation.NullDirection.UNSPECIFIED =>
        throw new TableException(s"Do not support UNSPECIFIED for null order.")
    }.toArray

    deduplicateSortKeys(keys.toArray, orders.toArray, nullsIsLast)
  }

  /**
    * Removes duplicate sort keys.
    */
  def deduplicateSortKeys(
      keys: Array[Int],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val keySet = new mutable.HashSet[Int]
    val keyBuffer = new mutable.ArrayBuffer[Int]
    val orderBuffer = new mutable.ArrayBuffer[Boolean]
    val nullsIsLastBuffer = new mutable.ArrayBuffer[Boolean]
    for (i <- keys.indices) {
      if (keySet.add(keys(i))) {
        keyBuffer += keys(i)
        orderBuffer += orders(i)
        nullsIsLastBuffer += nullsIsLast(i)
      }
    }
    (keyBuffer.toArray, orderBuffer.toArray, nullsIsLastBuffer.toArray)
  }

  /**
    * Converts window range in [[Group]] to String.
    */
  def windowRangeToString(
      logicWindow: Window,
      groupWindow: Group): String = {

    def calcOriginInputRows(window: Window): Int = {
      window.getRowType.getFieldCount - window.groups.flatMap(_.aggCalls).size
    }

    def boundString(bound: RexWindowBound, window: Window): String = {
      if (bound.getOffset != null) {
        val ref = bound.getOffset.asInstanceOf[RexInputRef]
        val boundIndex = ref.getIndex - calcOriginInputRows(window)
        val offset = window.constants.get(boundIndex).getValue2
        val offsetKind = if (bound.isPreceding) "PRECEDING" else "FOLLOWING"
        s"$offset $offsetKind"
      } else {
        bound.toString
      }
    }

    val buf = new StringBuilder
    buf.append(if (groupWindow.isRows) " ROWS " else " RANG ")
    val lowerBound = groupWindow.lowerBound
    val upperBound = groupWindow.upperBound
    if (lowerBound != null) {
      if (upperBound != null) {
        buf.append("BETWEEN ")
        buf.append(boundString(lowerBound, logicWindow))
        buf.append(" AND ")
        buf.append(boundString(upperBound, logicWindow))
      } else {
        buf.append(boundString(lowerBound, logicWindow))
      }
    } else if (upperBound != null) {
      buf.append(boundString(upperBound, logicWindow))
    }
    buf.toString
  }


}

/**
  * The information about aggregate function call
  *
  * @param agg  calcite agg call
  * @param function AggregateFunction or DeclarativeAggregateFunction
  * @param aggIndex the index of the aggregate call in the aggregation list
  * @param argIndexes the aggregate arguments indexes in the input
  * @param externalAccTypes  accumulator types
  * @param viewSpecs  data view specs
  * @param externalResultType the result type of aggregate
  * @param consumeRetraction whether the aggregate consumes retractions
  */
case class AggregateInfo(
    agg: AggregateCall,
    function: UserDefinedFunction,
    aggIndex: Int,
    argIndexes: Array[Int],
    externalAccTypes: Array[InternalType],
    viewSpecs: Array[DataViewSpec],
    externalResultType: InternalType,
    consumeRetraction: Boolean)

/**
  * The information about shared distinct of the aggregates. It indicates which aggregates are
  * distinct aggregates.
  *
  * @param argIndexes the distinct aggregate arguments indexes in the input
  * @param keyType the distinct key type
  * @param accType the accumulator type of the shared distinct
  * @param excludeAcc whether the distinct acc should excluded from the aggregate accumulator.
  *                    e.g. when this works in incremental mode, returns true, otherwise false.
  * @param dataViewSpec data view spec about this distinct agg used to generate state access,
  * None when dataview is not worked in state mode
  * @param consumeRetraction whether the distinct agg consumes retractions
  * @param filterArgs the ordinal of filter argument for each aggregate, -1 means without filter
  * @param aggIndexes the distinct aggregate index in the aggregation list
  */
case class DistinctInfo(
    argIndexes: Array[Int],
    keyType: InternalType,
    accType: InternalType,
    excludeAcc: Boolean,
    dataViewSpec: Option[DataViewSpec],
    consumeRetraction: Boolean,
    filterArgs: ArrayBuffer[Int],
    aggIndexes: ArrayBuffer[Int])