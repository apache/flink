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
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.planner.CalcitePair
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.plan.nodes.ExpressionFormat
import org.apache.flink.table.planner.plan.nodes.ExpressionFormat.ExpressionFormat

import com.google.common.collect.{ImmutableList, ImmutableMap}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.{RelCollation, RelWriter}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlMatchRecognize.AfterOption

import java.util
import java.util.{SortedSet => JSortedSet}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Explain rel utility methods.
  */
object RelExplainUtil {

  /**
   * Returns the prefer [[ExpressionFormat]] of the [[RelWriter]]. Use Prefix for traditional
   * writers, but use Infix for [[RelDescriptionWriterImpl]] which is more readable.
   * The [[RelDescriptionWriterImpl]] is mainly used to generate
   * [[org.apache.flink.table.planner.plan.nodes.FlinkRelNode#getRelDetailedDescription()]].
   */
  def preferExpressionFormat(pw: RelWriter): ExpressionFormat = pw match {
    // infix format is more readable for displaying
    case _: RelDescriptionWriterImpl => ExpressionFormat.Infix
    // traditional writer prefers prefix expression format, e.g. +(x, y)
    case _ => ExpressionFormat.Prefix
  }

  /**
    * Converts field names corresponding to given indices to String.
    */
  def fieldToString(fieldIndices: Array[Int], inputType: RelDataType): String = {
    val fieldNames = inputType.getFieldNames
    fieldIndices.map(fieldNames(_)).mkString(", ")
  }

  /**
    * Returns the Java string representation of this literal.
    */
  def literalToString(literal: RexLiteral): String = {
    literal.computeDigest(RexDigestIncludeType.NO_TYPE)
  }

  /**
    * Converts [[RelCollation]] to String.
    *
    * format sort fields as field name with direction `shortString`.
    */
  def collationToString(
      collation: RelCollation,
      inputRowType: RelDataType): String = {
    val inputFieldNames = inputRowType.getFieldNames
    collation.getFieldCollations.map { c =>
      s"${inputFieldNames(c.getFieldIndex)} ${c.direction.shortString}"
    }.mkString(", ")
  }

  /**
    * Converts [[RelCollation]] to String.
    *
    * format sort fields as field index with direction `shortString`.
    */
  def collationToString(collation: RelCollation): String = {
    collation.getFieldCollations.map { c =>
      s"$$${c.getFieldIndex} ${c.direction.shortString}"
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
            case daf: DeclarativeAggregateFunction =>
              val aggBufferTypes = daf.getAggBufferTypes.map(_.getLogicalType)
              val argList = aggBufferTypes.indices.map(offset + _).toList
              offset = offset + aggBufferTypes.length
              argList
            case _ =>
              throw new TableException(s"Unsupported function: $udf")
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

    // output for agg
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
          case daf: DeclarativeAggregateFunction =>
            val aggBufferTypes = daf.getAggBufferTypes.map(_.getLogicalType)
            val name = aggBufferTypes.indices
              .map(i => outputFieldNames(offset + i))
              .mkString(", ")
            offset = offset + aggBufferTypes.length
            if (aggBufferTypes.length > 1) s"($name)" else name
          case _ =>
            throw new TableException(s"Unsupported function: $udf")
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

  def streamWindowAggregationToString(
      inputRowType: RelDataType,
      outputRowType: RelDataType,
      aggInfoList: AggregateInfoList,
      grouping: Array[Int],
      windowProperties: Seq[PlannerNamedWindowProperty],
      isLocal: Boolean = false,
      isGlobal: Boolean = false): String = {
    stringifyStreamAggregationToString(
      inputRowType,
      outputRowType,
      aggInfoList,
      grouping,
      shuffleKey = None,
      windowProperties,
      isLocal,
      isGlobal)
  }

  def streamGroupAggregationToString(
      inputRowType: RelDataType,
      outputRowType: RelDataType,
      aggInfoList: AggregateInfoList,
      grouping: Array[Int],
      shuffleKey: Option[Array[Int]] = None,
      isLocal: Boolean = false,
      isGlobal: Boolean = false): String = {
    stringifyStreamAggregationToString(
      inputRowType,
      outputRowType,
      aggInfoList,
      grouping,
      shuffleKey,
      windowProperties = Seq(),
      isLocal,
      isGlobal)
  }

  private def stringifyStreamAggregationToString(
      inputRowType: RelDataType,
      outputRowType: RelDataType,
      aggInfoList: AggregateInfoList,
      grouping: Array[Int],
      shuffleKey: Option[Array[Int]],
      windowProperties: Seq[PlannerNamedWindowProperty],
      isLocal: Boolean,
      isGlobal: Boolean): String = {

    val aggInfos = aggInfoList.aggInfos
    val actualAggInfos = aggInfoList.getActualAggregateInfos
    val distinctInfos = aggInfoList.distinctInfos
    val distinctFieldNames = distinctInfos.indices.map(index => s"distinct$$$index")
    // aggIndex -> distinctFieldName
    val distinctAggs = distinctInfos.zip(distinctFieldNames)
      .flatMap(f => f._1.aggIndexes.map(i => (i, f._2)))
      .toMap
    val aggFilters = {
      val distinctAggFilters = distinctInfos
        .flatMap(d => d.aggIndexes.zip(d.filterArgs))
        .toMap
      val otherAggFilters = aggInfos
        .map(info => (info.aggIndex, info.agg.filterArg))
        .toMap
      otherAggFilters ++ distinctAggFilters
    }

    val inFieldNames = inputRowType.getFieldNames.toList.toArray
    val outFieldNames = outputRowType.getFieldNames.toList.toArray
    val groupingNames = grouping.map(inFieldNames(_))
    val aggOffset = shuffleKey match {
      case None => grouping.length
      case Some(k) => k.length
    }
    val isIncremental: Boolean = shuffleKey.isDefined

    // TODO output local/global agg call names like Partial_XXX, Final_XXX
    val aggStrings = if (isLocal) {
      stringifyLocalAggregates(aggInfos, distinctInfos, distinctAggs, aggFilters, inFieldNames)
    } else if (isGlobal || isIncremental) {
      val accFieldNames = inputRowType.getFieldNames.toList.toArray
      val aggOutputFieldNames = localAggOutputFieldNames(aggOffset, aggInfos, accFieldNames)
      stringifyGlobalAggregates(aggInfos, distinctAggs, aggOutputFieldNames)
    } else {
      stringifyAggregates(actualAggInfos, distinctAggs, aggFilters, inFieldNames)
    }

    val isTableAggregate =
      AggregateUtil.isTableAggregate(aggInfoList.getActualAggregateCalls.toList)
    val outputFieldNames = if (isLocal) {
      grouping.map(inFieldNames(_)) ++ localAggOutputFieldNames(aggOffset, aggInfos, outFieldNames)
    } else if (isIncremental) {
      val accFieldNames = inputRowType.getFieldNames.toList.toArray
      grouping.map(inFieldNames(_)) ++ localAggOutputFieldNames(aggOffset, aggInfos, accFieldNames)
    } else if (isTableAggregate) {
      val groupingOutNames = outFieldNames.slice(0, grouping.length)
      val aggOutNames = List(s"(${outFieldNames.drop(grouping.length)
        .dropRight(windowProperties.length).mkString(", ")})")
      val propertyOutNames = outFieldNames.slice(
        outFieldNames.length - windowProperties.length,
        outFieldNames.length)
      groupingOutNames ++ aggOutNames ++ propertyOutNames
    } else {
      outFieldNames
    }

    val propStrings = windowProperties.map(_.getProperty.toString)
    (groupingNames ++ aggStrings ++ propStrings).zip(outputFieldNames).map {
      case (f, o) if f == o => f
      case (f, o) => s"$f AS $o"
    }.mkString(", ")
  }

  private def stringifyGlobalAggregates(
      aggInfos: Array[AggregateInfo],
      distinctAggs: Map[Int, String],
      accFieldNames: Seq[String]): Array[String] = {
    aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      if (index >= accFieldNames.length) {
        println()
      }
      val argNames = accFieldNames(index)
      if (distinctAggs.contains(index)) {
        buf.append(s"${distinctAggs(index)} ")
      }
      buf.append(argNames).append(")")
      buf.toString
    }
  }

  private def stringifyLocalAggregates(
      aggInfos: Array[AggregateInfo],
      distincts: Array[DistinctInfo],
      distinctAggs: Map[Int, String],
      aggFilters: Map[Int, Int],
      inFieldNames: Array[String]): Array[String] = {
    val aggStrs = aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      val argNames = aggInfo.agg.getArgList.map(inFieldNames(_))
      if (distinctAggs.contains(index)) {
        buf.append(if (argNames.nonEmpty) s"${distinctAggs(index)} " else distinctAggs(index))
      }
      val argNameStr = if (argNames.nonEmpty) {
        argNames.mkString(", ")
      } else {
        "*"
      }
      buf.append(argNameStr).append(")")
      if (aggFilters(index) >= 0) {
        val filterName = inFieldNames(aggFilters(index))
        buf.append(" FILTER ").append(filterName)
      }
      buf.toString
    }
    val distinctStrs = distincts.map { distinctInfo =>
      val argNames = distinctInfo.argIndexes.map(inFieldNames(_)).mkString(", ")
      s"DISTINCT($argNames)"
    }
    aggStrs ++ distinctStrs
  }

  private def localAggOutputFieldNames(
      aggOffset: Int,
      aggInfos: Array[AggregateInfo],
      accNames: Array[String]): Array[String] = {
    var offset = aggOffset
    val aggOutputNames = aggInfos.map { info =>
      info.function match {
        case _: AggregateFunction[_, _] =>
          val name = accNames(offset)
          offset = offset + 1
          name
        case daf: DeclarativeAggregateFunction =>
          val aggBufferTypes = daf.getAggBufferTypes.map(_.getLogicalType)
          val name = aggBufferTypes.indices
            .map(i => accNames(offset + i))
            .mkString(", ")
          offset = offset + aggBufferTypes.length
          if (aggBufferTypes.length > 1) s"($name)" else name
        case _ =>
          throw new TableException(s"Unsupported function: ${info.function}")
      }
    }
    val distinctFieldNames = (offset until accNames.length).map(accNames)
    aggOutputNames ++ distinctFieldNames
  }

  private def stringifyAggregates(
      aggInfos: Array[AggregateInfo],
      distinctAggs: Map[Int, String],
      aggFilters: Map[Int, Int],
      inFields: Array[String]): Array[String] = {
    // MAX_RETRACT(DISTINCT a) FILTER b
    aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      val argNames = aggInfo.agg.getArgList.map(inFields(_))
      if (distinctAggs.contains(index)) {
        buf.append(if (argNames.nonEmpty) "DISTINCT " else "DISTINCT")
      }
      val argNameStr = if (argNames.nonEmpty) {
        argNames.mkString(", ")
      } else {
        "*"
      }
      buf.append(argNameStr).append(")")
      if (aggFilters(index) >= 0) {
        val filterName = inFields(aggFilters(index))
        buf.append(" FILTER ").append(filterName)
      }
      buf.toString
    }
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

  def conditionToString(
      calcProgram: RexProgram,
      f: (RexNode, List[String], Option[List[RexNode]], ExpressionFormat) => String,
      expressionFormat: ExpressionFormat = ExpressionFormat.Prefix): String = {
    val cond = calcProgram.getCondition
    val inputFieldNames = calcProgram.getInputRowType.getFieldNames.toList
    val localExprs = calcProgram.getExprList.toList

    if (cond != null) {
      f(cond, inputFieldNames, Some(localExprs), expressionFormat)
    } else {
      ""
    }
  }

  def selectionToString(
      calcProgram: RexProgram,
      expression: (RexNode, List[String], Option[List[RexNode]], ExpressionFormat) => String,
      expressionFormat: ExpressionFormat = ExpressionFormat.Prefix): String = {

    val proj = calcProgram.getProjectList.toList
    val inFields = calcProgram.getInputRowType.getFieldNames.toList
    val localExprs = calcProgram.getExprList.toList
    val outFields = calcProgram.getOutputRowType.getFieldNames.toList

    proj.map(expression(_, inFields, Some(localExprs), expressionFormat))
        .zip(outFields).map { case (e, o) =>
      if (e != o) {
        e + " AS " + o
      } else {
        e
      }
    }.mkString(", ")
  }

  def correlateToString(
      inputType: RelDataType,
      rexCall: RexCall,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    val name = rexCall.getOperator.toString
    val inFields = inputType.getFieldNames.toList
    val operands = rexCall.getOperands.map(expression(_, inFields, None)).mkString(",")
    s"table($name($operands))"
  }

  def windowAggregationToString(
      inputType: RelDataType,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      rowType: RelDataType,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      enableAssignPane: Boolean,
      isMerge: Boolean,
      isGlobal: Boolean): String = {
    val prefix = if (isMerge) {
      "Final_"
    } else if (!isGlobal) {
      "Partial_"
    } else {
      ""
    }

    val inFields = inputType.getFieldNames
    val outFields = rowType.getFieldNames

    /**
      *  - local window agg input type: grouping keys + aux-grouping keys + agg arg list
      *  - global window agg input type: grouping keys + timestamp + aux-grouping keys + agg buffer
      *  agg buffer as agg merge args list
      */
    var offset = if (isMerge) {
      grouping.length + 1 + auxGrouping.length
    } else {
      grouping.length + auxGrouping.length
    }
    val aggStrings = aggCallToAggFunction.map { case (aggCall, udf) =>
      var newArgList = aggCall.getArgList.map(_.toInt).toList
      if (isMerge) {
        newArgList = udf match {
          case _: AggregateFunction[_, _] =>
            val argList = List(offset)
            offset = offset + 1
            argList
          case daf: DeclarativeAggregateFunction =>
            val argList = daf.aggBufferAttributes().indices.map(offset + _).toList
            offset = offset + daf.aggBufferAttributes.length
            argList
        }
      }
      val argListNames = if (newArgList.nonEmpty) {
        newArgList.map(inFields(_)).mkString(", ")
      } else {
        "*"
      }
      if (aggCall.filterArg >= 0 && aggCall.filterArg < inFields.size) {
        s"${aggCall.getAggregation}($argListNames) FILTER ${inFields(aggCall.filterArg)}"
      } else {
        s"${aggCall.getAggregation}($argListNames)"
      }
    }

    /**
      * - local window agg output type: grouping keys + timestamp + aux-grouping keys + agg buffer
      * - global window agg output type:
      * grouping keys + aux-grouping keys + agg result + window props
      */
    offset = if (!isGlobal) {
      grouping.length + 1 + auxGrouping.length
    } else {
      grouping.length + auxGrouping.length
    }
    val outFieldNames = aggCallToAggFunction.map { case (_, udf) =>
      val outFieldName = if (isGlobal) {
        val name = outFields(offset)
        offset = offset + 1
        name
      } else {
        udf match {
          case _: AggregateFunction[_, _] =>
            val name = outFields(offset)
            offset = offset + 1
            name
          case daf: DeclarativeAggregateFunction =>
            val name = daf.aggBufferAttributes().zipWithIndex.map(offset + _._2).map(
              outFields(_)).mkString(", ")
            offset = offset + daf.aggBufferAttributes().length
            if (daf.aggBufferAttributes.length > 1) s"($name)" else name
        }
      }
      outFieldName
    }

    val inNames = grouping.map(inFields(_)) ++ auxGrouping.map(inFields(_)) ++ aggStrings
    val outNames = grouping.indices.map(outFields(_)) ++
        (grouping.length + 1 until grouping.length + 1 + auxGrouping.length).map(outFields(_)) ++
        outFieldNames
    inNames.zip(outNames).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$prefix$f AS $o"
      }
    }.mkString(", ")
  }

  /**
   * @deprecated please use [[streamWindowAggregationToString()]] instead.
   */
  @Deprecated
  def legacyStreamWindowAggregationToString(
      inputType: RelDataType,
      grouping: Array[Int],
      rowType: RelDataType,
      aggs: Seq[AggregateCall],
      namedProperties: Seq[PlannerNamedWindowProperty],
      withOutputFieldNames: Boolean = true): String = {
    val inFields = inputType.getFieldNames
    val isTableAggregate = AggregateUtil.isTableAggregate(aggs)
    val outFields: Seq[String] = if (isTableAggregate) {
      val outNames = rowType.getFieldNames
      outNames.slice(0, grouping.length) ++
        List(s"(${outNames.drop(grouping.length)
          .dropRight(namedProperties.length).mkString(", ")})") ++
        outNames.slice(outNames.length - namedProperties.length, outNames.length)
    } else {
      rowType.getFieldNames
    }
    val groupStrings = grouping.map(inFields(_))

    val aggStrings = aggs.map(call => {
      val distinct = if (call.isDistinct) {
        if (call.getArgList.size() == 0) {
          "DISTINCT"
        } else {
          "DISTINCT "
        }
      } else {
        ""
      }
      val argList = if (call.getArgList.size() > 0) {
        call.getArgList.map(inFields(_)).mkString(", ")
      } else {
        "*"
      }

      val filter = if (call.filterArg >= 0 && call.filterArg < inFields.size) {
        s" FILTER ${inFields(call.filterArg)}"
      } else {
        ""
      }

      s"${call.getAggregation}($distinct$argList)$filter"
    })

    val propStrings = namedProperties.map(_.getProperty.toString)
    (groupStrings ++ aggStrings ++ propStrings).zip(outFields).map {
      case (f, o) => if (f == o) {
        f
      } else {
        if (withOutputFieldNames) s"$f AS $o" else f
      }
    }.mkString(", ")
  }

  // ------------------------------------------------------------------------------------
  // MATCH RECOGNIZE
  // ------------------------------------------------------------------------------------

  /**
    * Converts measures of MatchRecognize to String.
    */
  def measuresDefineToString(
    measures: ImmutableMap[String, RexNode],
    fieldNames: List[String],
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): String =
    measures.map {
      case (k, v) => s"${expression(v, fieldNames, None)} AS $k"
    }.mkString(", ")

  /**
    * Converts all rows or not of MatchRecognize to ROWS PER MATCH String
    */
  def rowsPerMatchToString(isAll: Boolean): String =
    if (isAll) "ALL ROWS PER MATCH" else "ONE ROW PER MATCH"

  /**
    * Converts AFTER clause of MatchRecognize to String
    */
  def afterMatchToString(
      after: RexNode,
      fieldNames: Seq[String]): String =
    after.getKind match {
      case SqlKind.SKIP_TO_FIRST => s"SKIP TO FIRST ${
        after.asInstanceOf[RexCall].operands.get(0).toString
      }"
      case SqlKind.SKIP_TO_LAST => s"SKIP TO LAST ${
        after.asInstanceOf[RexCall].operands.get(0).toString
      }"
      case SqlKind.LITERAL => after.asInstanceOf[RexLiteral]
        .getValueAs(classOf[AfterOption]) match {
        case AfterOption.SKIP_PAST_LAST_ROW => "SKIP PAST LAST ROW"
        case AfterOption.SKIP_TO_NEXT_ROW => "SKIP TO NEXT ROW"
      }
      case _ => throw new IllegalStateException(s"Corrupted query tree. Unexpected $after for " +
        s"after match strategy.")
    }

  /**
    * Converts subset clause of MatchRecognize to String
    */
  def subsetToString(subset: ImmutableMap[String, JSortedSet[String]]): String =
    subset.map {
      case (k, v) => s"$k = (${v.mkString(", ")})"
    }.mkString(", ")

  /**
   * Converts [[RelHint]]s to String.
   */
  def hintsToString(hints: util.List[RelHint]): String = {
    val sb = new StringBuilder
    sb.append("[")
    hints.foreach { hint =>
      sb.append("[").append(hint.hintName)
      if (!hint.inheritPath.isEmpty) {
        sb.append(" inheritPath:").append(hint.inheritPath)
      }
      if (hint.listOptions.size() > 0 || hint.kvOptions.size() > 0) {
        sb.append(" options:")
        if (hint.listOptions.size > 0) {
          sb.append(hint.listOptions.toString)
        } else {
          sb.append(hint.kvOptions.toString)
        }
      }
      sb.append("]")
    }
    sb.append("]")
    sb.toString
  }
}
