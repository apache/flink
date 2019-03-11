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

import org.apache.flink.table.CalcitePair
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.plan.FlinkJoinRelType

import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexNode, RexWindowBound}

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Explain rel utility methods.
  */
object RelExplainUtil {

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
