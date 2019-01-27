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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, RichIterativeCondition}
import org.apache.flink.cep._
import org.apache.flink.table.api.{TableConfig, ValidationException}
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.codegen.{CodeGeneratorContext, Compiler, GeneratedSorter, MatchCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.logical.MatchRecognize
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.runtime.`match`._
import org.apache.flink.table.runtime.aggregate.{CollectionBaseRowComparator, SorterHelper}
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.util.Logging

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.core.Match
import org.apache.calcite.rex.{RexCall, RexNode, RexPatternFieldRef}
import org.apache.calcite.tools.RelBuilder

import java.util
import java.util.Comparator
import org.apache.flink.table.codegen.MatchCodeGenerator.ALL_PATTERN_VARIABLE

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * An util class to generate match functions.
  */
object MatchUtil {

  def isDeterministic(`match`: Match): Boolean = {
    FlinkRexUtil.isDeterministicOperator(`match`.getAfter) &&
      FlinkRexUtil.isDeterministicOperator(`match`.getPattern) &&
      FlinkRexUtil.isDeterministicOperator(`match`.getRowsPerMatch) &&
      `match`.getPatternDefinitions.values().asScala.forall(FlinkRexUtil.isDeterministicOperator) &&
      `match`.getPartitionKeys.asScala.forall(FlinkRexUtil.isDeterministicOperator) &&
      FlinkRexUtil.isDeterministicOperator(`match`.getInterval)
  }

  def isDeterministic(logicalMatch: MatchRecognize): Boolean = {
    FlinkRexUtil.isDeterministicOperator(logicalMatch.after) &&
      FlinkRexUtil.isDeterministicOperator(logicalMatch.pattern) &&
      FlinkRexUtil.isDeterministicOperator(logicalMatch.rowsPerMatch) &&
      logicalMatch.patternDefinitions.values().forall(FlinkRexUtil.isDeterministicOperator) &&
      logicalMatch.partitionKeys.forall(FlinkRexUtil.isDeterministicOperator) &&
      FlinkRexUtil.isDeterministicOperator(logicalMatch.interval)
  }

  private[flink] def generateIterativeCondition(
    config: TableConfig,
    relBuilder: RelBuilder,
    patternName: String,
    patternNames: Seq[String],
    patternDefinition: RexNode,
    inputTypeInfo: TypeInformation[_]): IterativeCondition[BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames,
      Some(patternName))
        .bindInput(TypeConverters.createInternalTypeFromTypeInfo(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]
    val condition = generator.generateCondition(patternDefinition)
    val body =
      s"""
        |${condition.code}
        |return ${condition.resultTerm};
        |""".stripMargin

    val genCondition = generator
      .generateMatchFunction("MatchRecognizeCondition",
        config,
        classOf[RichIterativeCondition[_]],
        body)
    new IterativeConditionRunner(genCondition)
  }

  private[flink] def generatePatternSelectFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternSelectFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames)
        .bindInput(TypeConverters.createInternalTypeFromTypeInfo(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateOneRowPerMatchExpression(
      partitionKeys,
      measures,
      returnType)
    val body =
      s"""
        |${resultExpression.code}
        |return ${resultExpression.resultTerm};
        |""".stripMargin

    val genFunction = generator.generateMatchFunction(
      "MatchRecognizePatternSelectFunction",
      config,
      classOf[RichPatternSelectFunction[_, _]],
      body)
    new PatternSelectFunctionRunner(genFunction)
  }

  private[flink] def generatePatternTimeoutFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternTimeoutFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames)
        .bindInput(TypeConverters.createInternalTypeFromTypeInfo(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateOneRowPerMatchExpression(
      partitionKeys,
      measures,
      returnType)
    val body =
      s"""
         |${resultExpression.code}
         |return ${resultExpression.resultTerm};
         |""".stripMargin

    val genFunction = generator.generateMatchFunction(
      "MatchRecognizePatternTimeoutFunction",
      config,
      classOf[RichPatternTimeoutFunction[_, _]],
      body)
    new PatternTimeoutFunctionRunner(genFunction)
  }

  private[flink] def generatePatternFlatSelectFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternFlatSelectFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames)
        .bindInput(TypeConverters.createInternalTypeFromTypeInfo(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateAllRowsPerMatchExpression(
      partitionKeys,
      orderKeys,
      measures,
      returnType)
    val body =
      s"""
        |${resultExpression.code}
        |""".stripMargin

    val genFunction = generator.generateMatchFunction(
      "MatchRecognizePatternFlatSelectFunction",
      config,
      classOf[RichPatternFlatSelectFunction[_, _]],
      body)
    new PatternFlatSelectFunctionRunner(genFunction)
  }

  private[flink] def generatePatternFlatTimeoutFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternFlatTimeoutFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames)
        .bindInput(TypeConverters.createInternalTypeFromTypeInfo(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateAllRowsPerMatchExpression(
      partitionKeys,
      orderKeys,
      measures,
      returnType)
    val body =
      s"""
         |${resultExpression.code}
         |""".stripMargin

    val genFunction = generator.generateMatchFunction(
      "MatchRecognizePatternFlatTimeoutFunction",
      config,
      classOf[RichPatternFlatTimeoutFunction[_, _]],
      body)
    new PatternFlatTimeoutFunctionRunner(genFunction)
  }

  private[flink] def createRowTimeSortFunction(
      orderKeys: RelCollation,
      inputSchema: BaseRowSchema): EventComparator[BaseRow] = {
    val sorter = SorterHelper.createSorter(
      inputSchema.internalType(),
      orderKeys.getFieldCollations.asScala.tail) // strip off time collation

    new CustomEventComparator(sorter)
  }

  private[flink] def createProcTimeSortFunction(
      orderKeys: RelCollation,
      inputSchema: BaseRowSchema): EventComparator[BaseRow] = {
    val sorter = SorterHelper.createSorter(
      inputSchema.internalType(),
      orderKeys.getFieldCollations.asScala.tail) // strip off time collation

    new CustomEventComparator(sorter)
  }

  /**
    * Custom EventComparator.
    */
  class CustomEventComparator(private val gSorter: GeneratedSorter)
    extends EventComparator[BaseRow]
    with Compiler[RecordComparator]
    with Logging {

    private var rowComp: Comparator[BaseRow] = _

    override def compare(arg0: BaseRow, arg1: BaseRow):Int = {
      if (rowComp == null) {
        val name = gSorter.comparator.name
        val code = gSorter.comparator.code
        LOG.debug(s"Compiling Sorter: $name \n\n Code:\n$code")
        val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
        gSorter.comparator.code = null
        LOG.debug("Instantiating Sorter.")
        val comparator = clazz.newInstance()
        comparator.init(gSorter.serializers, gSorter.comparators)
        rowComp = new CollectionBaseRowComparator(comparator)
      }

      rowComp.compare(arg0, arg1)
    }
  }

  class AggregationPatternVariableFinder extends RexDefaultVisitor[Option[String]] {

    override def visitPatternFieldRef(patternFieldRef: RexPatternFieldRef): Option[String] = Some(
      patternFieldRef.getAlpha)

    override def visitCall(call: RexCall): Option[String] = {
      if (call.operands.size() == 0) {
        Some(ALL_PATTERN_VARIABLE)
      } else {
        call.operands.asScala.map(n => n.accept(this)).reduce((op1, op2) => (op1, op2) match {
          case (None, None) => None
          case (x, None) => x
          case (None, x) => x
          case (Some(var1), Some(var2)) if var1.equals(var2) =>
            Some(var1)
          case _ =>
            throw new ValidationException(s"Aggregation must be applied to a single pattern " +
              s"variable. Malformed expression: $call")
        })
      }
    }

    override def visitNode(rexNode: RexNode): Option[String] = None
  }
}
