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

import org.apache.flink.table.api.functions.{AggregateFunction, DeclarativeAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall

import scala.collection.JavaConverters._
import scala.collection.mutable

object AggregateNameUtil {

  def groupingToString(inputType: RelDataType, grouping: Array[Int]): String = {
    val inFields = inputType.getFieldNames.asScala
    grouping.map( inFields(_) ).mkString(", ")
  }

  def aggregationToString(
      inputType: RelDataType,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      rowType: RelDataType,
      aggCalls: Seq[AggregateCall],
      aggFunctions: Seq[UserDefinedFunction],
      isMerge: Boolean,
      isGlobal: Boolean,
      distincts: Seq[DistinctInfo] = Seq()): String = {
    val prefix = if (isMerge) {
      "Final_"
    } else if (!isGlobal) {
      "Partial_"
    } else {
      ""
    }

    val inFields = inputType.getFieldNames.asScala
    val outFields = rowType.getFieldNames.asScala
    val fullGrouping = grouping ++ auxGrouping

    val distinctFieldNames = distincts.indices.map(index => s"distinct$$$index")
    val distinctStrings = if (isMerge) {
      // not output distinct fields in global merge
      Seq()
    } else {
      distincts.map { distinct =>
        val argListNames = distinct.argIndexes.map(inFields).mkString(",")
        // TODO: [BLINK-16994458] Refactor local&global aggregate name
        val filterNames = distinct.filterArgs.filter(_ > 0).map(inFields).mkString(", ")
        if (filterNames.nonEmpty) {
          s"DISTINCT($argListNames) FILTER ($filterNames)"
        } else {
          s"DISTINCT($argListNames)"
        }
      }
    }

    val aggToDistinctMapping = mutable.HashMap.empty[Int, String]
    distincts.zipWithIndex.foreach { case (distinct, index) =>
      distinct.aggIndexes.foreach { aggIndex =>
        aggToDistinctMapping += (aggIndex -> distinctFieldNames(index))
      }
    }

    //agg
    var offset = fullGrouping.length
    val aggStrings = aggCalls.zip(aggFunctions).zipWithIndex.map { case ((aggCall, udf), index) =>
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
      var newArgList = aggCall.getArgList.asScala.map(_.toInt).toList
      if (isMerge) {
        newArgList = udf match {
          case _: AggregateFunction[_, _] =>
            val argList = List(offset)
            offset = offset + 1
            argList
          case daf: DeclarativeAggregateFunction =>
            val argList = daf.aggBufferSchema.indices.map(offset + _).toList
            offset = offset + daf.aggBufferSchema.length
            argList
        }
      }
      val argListNames = if (aggToDistinctMapping.contains(index)) {
        aggToDistinctMapping(index)
      } else if (newArgList.nonEmpty) {
        newArgList.map(inFields(_)).mkString(", ")
      } else {
        "*"
      }

      if (aggCall.filterArg >= 0 && aggCall.filterArg < inFields.size) {
        s"${aggCall.getAggregation}($distinct$argListNames) FILTER ${inFields(aggCall.filterArg)}"
      } else {
        s"${aggCall.getAggregation}($distinct$argListNames)"
      }
    }

    //output for agg
    offset = fullGrouping.length
    val outFieldNames = aggFunctions.map { udf =>
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
            val name = daf.aggBufferSchema.zipWithIndex.map(offset + _._2).map(
              outFields(_)).mkString(", ")
            offset = offset + daf.aggBufferSchema.length
            if (daf.aggBufferSchema.size > 1) s"($name)" else name
        }
      }
      outFieldName
    }

    (fullGrouping.map(inFields(_)) ++ aggStrings ++ distinctStrings).zip(
      fullGrouping.indices.map(outFields(_)) ++ outFieldNames ++ distinctFieldNames).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$prefix$f AS $o"
      }
    }.mkString(", ")
  }

  def aggregationToString(
      inputType: RelDataType,
      grouping: Array[Int],
      rowType: RelDataType,
      aggCalls: Seq[AggregateCall],
      namedProperties: Seq[NamedWindowProperty]): String = {
    buildAggregationToString(
      inputType, grouping, Array.empty[Int], rowType, aggCalls, namedProperties)
  }

  private def buildAggregationToString(
      inputType: RelDataType,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      rowType: RelDataType,
      aggs: Seq[AggregateCall],
      namedProperties: Seq[NamedWindowProperty]): String = {

    val inFields = inputType.getFieldNames.asScala
    val outFields = rowType.getFieldNames.asScala

    val groupStrings = grouping.map( inFields(_) )

    val aggStrings = aggs.map(a => {
      val distinct = if (a.isDistinct) {
        if (a.getArgList.size() == 0) {
          "DISTINCT"
        } else {
          "DISTINCT "
        }
      } else {
        ""
      }
      val argList = if (a.getArgList.size() > 0) {
        a.getArgList.asScala.map(inFields(_)).mkString(", ")
      } else {
        "*"
      }
      s"${a.getAggregation}($distinct$argList)"
    })

    val propStrings = namedProperties.map(_.property.toString)

    (groupStrings ++ aggStrings ++ propStrings).zip(outFields).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }.mkString(", ")
  }

  def streamAggregationToString(
      inputType: RelDataType,
      outputType: RelDataType,
      aggInfoList: AggregateInfoList,
      grouping: Array[Int],
      shuffleKey: Option[Array[Int]] = None,
      isLocal: Boolean = false,
      isGlobal: Boolean = false): String = {

    val aggInfos = aggInfoList.aggInfos
    val distincts = aggInfoList.distinctInfos
    val distinctFieldNames = distincts.indices.map(index => s"distinct$$$index")
    // aggIndex -> distinctFieldName
    val distinctAggs = distincts.zip(distinctFieldNames)
        .flatMap(f => f._1.aggIndexes.map(i => (i, f._2)))
        .toMap
    val aggFilters = {
      val distinctAggFilters = distincts
          .flatMap(d => d.aggIndexes.zip(d.filterArgs))
          .toMap
      val otherAggFilters = aggInfos
          .map(info => (info.aggIndex, info.agg.filterArg))
          .toMap
      otherAggFilters ++ distinctAggFilters
    }

    val inFields = inputType.getFieldNames.asScala.toArray
    val outFields = outputType.getFieldNames.asScala.toArray
    val groupingStrings = grouping.map(inFields(_))
    val aggOffset = shuffleKey match {
      case None => grouping.length
      case Some(k) => k.length
    }
    val isIncremental: Boolean = shuffleKey.isDefined

    val aggStrings = if (isLocal) {
      stringifyLocalAggregates(aggInfos, distincts, distinctAggs, aggFilters, inFields)
    } else if (isGlobal || isIncremental) {
      val accFieldNames = inputType.getFieldNames.asScala
      val aggOutputFieldNames = localAggOutputFieldNames(aggOffset, aggInfos, accFieldNames)
      stringifyGlobalAggregates(aggInfos, distinctAggs, aggOutputFieldNames)
    } else {
      stringifyAggregates(aggInfos, distinctAggs, aggFilters, inFields)
    }

    val outputFieldNames = if (isLocal) {
      grouping.map(inFields(_)) ++ localAggOutputFieldNames(aggOffset, aggInfos, outFields)
    } else if (isIncremental) {
      val accFieldNames = inputType.getFieldNames.asScala
      grouping.map(inFields(_)) ++ localAggOutputFieldNames(aggOffset, aggInfos, accFieldNames)
    } else {
      outFields
    }

    (groupingStrings ++ aggStrings).zip(outputFieldNames).map {
      case (f, o) if f == o => f
      case (f, o) => s"$f AS $o"
    }.mkString(", ")
  }


  private def localAggOutputFieldNames(
      aggOffset: Int,
      aggInfos: Array[AggregateInfo],
      outFields: Seq[String]): Array[String] = {
    var offset = aggOffset
    val aggOutputNames = aggInfos.map { info =>
      info.function match {
        case _: AggregateFunction[_, _] =>
          val name = outFields(offset)
          offset = offset + 1
          name
        case daf: DeclarativeAggregateFunction =>
          val name = daf.aggBufferSchema.indices.map(i => outFields(i + offset)).mkString(", ")
          offset = offset + daf.aggBufferSchema.length
          if (daf.aggBufferSchema.size > 1) s"($name)" else name
      }
    }
    val distinctFieldNames = (offset until outFields.length).map(outFields)
    aggOutputNames ++ distinctFieldNames
  }

  private def stringifyLocalAggregates(
      aggInfos: Array[AggregateInfo],
      distincts: Array[DistinctInfo],
      distinctAggs: Map[Int, String],
      aggFilters: Map[Int, Int],
      inFields: Seq[String]): Array[String] = {
    val aggStrs = aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      val argNames = aggInfo.agg.getArgList.asScala.map(inFields(_))
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
        val filterName = inFields(aggFilters(index))
        buf.append(" FILTER ").append(filterName)
      }
      buf.toString
    }
    val distinctStrs = distincts.map { distinctInfo =>
      val argNames = distinctInfo.argIndexes.map(inFields(_)).mkString(", ")
      s"DISTINCT($argNames)"
    }
    aggStrs ++ distinctStrs
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

  private def stringifyAggregates(
      aggInfos: Array[AggregateInfo],
      distinctAggs: Map[Int, String],
      aggFilters: Map[Int, Int],
      inFields: Seq[String]): Array[String] = {
    // MAX_RETRACT(DISTINCT a) FILTER b
    aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      val argNames = aggInfo.agg.getArgList.asScala.map(inFields(_))
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

    val inFields = inputType.getFieldNames.asScala
    val outFields = rowType.getFieldNames.asScala

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
      var newArgList = aggCall.getArgList.asScala.map(_.toInt).toList
      if (isMerge) {
        newArgList = udf match {
          case _: AggregateFunction[_, _] =>
            val argList = List(offset)
            offset = offset + 1
            argList
          case daf: DeclarativeAggregateFunction =>
            val argList = daf.aggBufferSchema.indices.map(offset + _).toList
            offset = offset + daf.aggBufferSchema.length
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
            val name = daf.aggBufferSchema.zipWithIndex.map(offset + _._2).map(
              outFields(_)).mkString(", ")
            offset = offset + daf.aggBufferSchema.length
            if (daf.aggBufferSchema.size > 1) s"($name)" else name
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
}
