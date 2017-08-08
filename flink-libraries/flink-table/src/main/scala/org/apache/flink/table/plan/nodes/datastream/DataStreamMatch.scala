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

package org.apache.flink.table.plan.nodes.datastream

import java.util
import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel._
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.cep.{CEP, PatternStream}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.RowtimeProcessFunction
import org.apache.flink.table.runtime.`match`._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Flink RelNode which matches along with LogicalMatch.
  */
class DataStreamMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    pattern: RexNode,
    strictStart: Boolean,
    strictEnd: Boolean,
    patternDefinitions: util.Map[String, RexNode],
    measures: util.Map[String, RexNode],
    after: RexNode,
    subsets: util.Map[String, util.SortedSet[String]],
    allRows: Boolean,
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    interval: RexNode,
    schema: RowSchema,
    inputSchema: RowSchema)
  extends SingleRel(cluster, traitSet, input)
  with DataStreamRel {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamMatch(
      cluster,
      traitSet,
      inputs.get(0),
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      measures,
      after,
      subsets,
      allRows,
      partitionKeys,
      orderKeys,
      interval,
      schema,
      inputSchema)
  }

  override def toString: String = {
    s"Match(${
      if (!partitionKeys.isEmpty) {
        s"PARTITION BY: ${partitionKeys.toArray.map(_.toString).mkString(", ")}, "
      } else {
        ""
      }
    }${
      if (!orderKeys.getFieldCollations.isEmpty) {
        s"ORDER BY: ${orderKeys.getFieldCollations.asScala.map {
          x => inputSchema.relDataType.getFieldList.get(x.getFieldIndex).getName
        }.mkString(", ")}, "
      } else {
        ""
      }
    }${
      if (!measures.isEmpty) {
        s"MEASURES: ${measures.asScala.map {
          case (k, v) => s"${v.toString} AS $k"
        }.mkString(", ")}, "
      } else {
        ""
      }
    }${
      if (allRows) {
        s"ALL ROWS PER MATCH, "
      } else {
        s"ONE ROW PER MATCH, "
      }
    }${
      s"${after.toString}, "
    }${
      s"PATTERN: (${pattern.toString})"
    }${
      if (interval != null) {
        s"WITHIN INTERVAL: $interval, "
      } else {
        s", "
      }
    }${
      if (!subsets.isEmpty) {
        s"SUBSET: ${subsets.asScala.map {
          case (k, v) => s"$k = (${v.toArray.mkString(", ")})"
        }.mkString(", ")}, "
      } else {
        ""
      }
    }${
      s"DEFINE: ${patternDefinitions.asScala.map {
        case (k, v) => s"$k AS ${v.toString}"
      }.mkString(", ")}"
    })"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput())
      .itemIf("partitionBy",
        partitionKeys.toArray.map(_.toString).mkString(", "),
        !partitionKeys.isEmpty)
      .itemIf("orderBy",
        orderKeys.getFieldCollations.asScala.map {
          x => inputSchema.relDataType.getFieldList.get(x.getFieldIndex).getName
        }.mkString(", "),
        !orderKeys.getFieldCollations.isEmpty)
      .itemIf("measures",
        measures.asScala.map { case (k, v) => s"${v.toString} AS $k"}.mkString(", "),
        !measures.isEmpty)
      .item("allrows", allRows)
      .item("after", after.toString)
      .item("pattern", pattern.toString)
      .itemIf("within interval",
        if (interval != null) {
          interval.toString
        } else {
          null
        },
        interval != null)
      .itemIf("subset",
        subsets.asScala.map { case (k, v) => s"$k = (${v.toArray.mkString(", ")})"}.mkString(", "),
        !subsets.isEmpty)
      .item("define",
        patternDefinitions.asScala.map { case (k, v) => s"$k AS ${v.toString}"}.mkString(", "))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.config
    val inputTypeInfo = inputSchema.typeInfo

    val crowInput: DataStream[CRow] = getInput
      .asInstanceOf[DataStreamRel]
      .translateToPlan(tableEnv, queryConfig)

    val rowtimeFields = inputSchema.relDataType
      .getFieldList.asScala
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    val timestampedInput = if (rowtimeFields.nonEmpty) {
      // copy the rowtime field into the StreamRecord timestamp field
      val timeIdx = rowtimeFields.head.getIndex

      crowInput
        .process(new RowtimeProcessFunction(timeIdx, CRowTypeInfo(inputTypeInfo)))
        .setParallelism(crowInput.getParallelism)
        .name(s"rowtime field: (${rowtimeFields.head})")
      } else {
        crowInput
      }

    val inputDS: DataStream[Row] = timestampedInput
      .map(new ConvertToRow)
      .setParallelism(timestampedInput.getParallelism)
      .name("ConvertToRow")
      .returns(inputTypeInfo)

    def translatePattern(
      rexNode: RexNode,
      currentPattern: Pattern[Row, Row],
      patternNames: ListBuffer[String]): Pattern[Row, Row] = rexNode match {
      case literal: RexLiteral =>
        val patternName = literal.getValue3.toString
        patternNames += patternName
        val newPattern = next(currentPattern, patternName)

        val patternDefinition = patternDefinitions.get(patternName)
        if (patternDefinition != null) {
          val condition = MatchUtil.generateIterativeCondition(
            config,
            inputSchema,
            patternName,
            patternNames,
            patternDefinition,
            inputTypeInfo)

          newPattern.where(condition)
        } else {
          newPattern
        }

      case call: RexCall =>

        call.getOperator match {
          case PATTERN_CONCAT =>
            val left = call.operands.get(0)
            val right = call.operands.get(1)
            translatePattern(right,
              translatePattern(left, currentPattern, patternNames),
              patternNames)

          case PATTERN_QUANTIFIER =>
            val name = call.operands.get(0).asInstanceOf[RexLiteral]
            val newPattern = translatePattern(name, currentPattern, patternNames)

            val startNum = call.operands.get(1).asInstanceOf[RexLiteral]
              .getValue3.asInstanceOf[JBigDecimal].intValue()
            val endNum = call.operands.get(2).asInstanceOf[RexLiteral]
              .getValue3.asInstanceOf[JBigDecimal].intValue()

            if (startNum == 0 && endNum == -1) {        // zero or more
              newPattern.oneOrMore().optional().consecutive()
            } else if (startNum == 1 && endNum == -1) { // one or more
              newPattern.oneOrMore().consecutive()
            } else if (startNum == 0 && endNum == 1) {  // optional
              newPattern.optional()
            } else if (endNum != -1) {                  // times
              newPattern.times(startNum, endNum).consecutive()
            } else {                                    // times or more
              newPattern.timesOrMore(startNum).consecutive()
            }

          case PATTERN_ALTER =>
            throw TableException("Currently, CEP doesn't support branching patterns.")

          case PATTERN_PERMUTE =>
            throw TableException("Currently, CEP doesn't support PERMUTE patterns.")

          case PATTERN_EXCLUDE =>
            throw TableException("Currently, CEP doesn't support '{-' '-}' patterns.")
        }

      case _ =>
        throw TableException("")
    }

    val patternNames: ListBuffer[String] = ListBuffer()
    val cepPattern = translatePattern(pattern, null, patternNames)
    if (interval != null) {
      val intervalLiteral = interval.asInstanceOf[RexLiteral]
      val intervalValue = interval.asInstanceOf[RexLiteral].getValueAs(classOf[java.lang.Long])
      val intervalMs: Long = intervalLiteral.getTypeName match {
        case INTERVAL_YEAR | INTERVAL_YEAR_MONTH | INTERVAL_MONTH =>
        // convert from months to milliseconds, suppose 1 month = 30 days
        intervalValue * 30L * 24 * 3600 * 1000
        case _ => intervalValue
      }

      cepPattern.within(Time.milliseconds(intervalMs))
    }
    val patternStream: PatternStream[Row] = CEP.pattern[Row](inputDS, cepPattern)

    val outTypeInfo = CRowTypeInfo(schema.typeInfo)
    if (allRows) {
      val patternFlatSelectFunction =
        MatchUtil.generatePatternFlatSelectFunction(
          config,
          schema,
          patternNames,
          partitionKeys,
          orderKeys,
          measures,
          inputTypeInfo)
      patternStream.flatSelect[CRow](patternFlatSelectFunction, outTypeInfo)
    } else {
      val patternSelectFunction =
        MatchUtil.generatePatternSelectFunction(
          config,
          schema,
          patternNames,
          partitionKeys,
          measures,
          inputTypeInfo)
      patternStream.select[CRow](patternSelectFunction, outTypeInfo)
    }
  }

  private def next(currentPattern: Pattern[Row, Row], patternName: String): Pattern[Row, Row] = {
    if (currentPattern == null) {
      Pattern.begin(patternName)
    } else {
      currentPattern.next(patternName)
    }
  }
}
