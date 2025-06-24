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

import org.apache.flink.api.common.functions.{DefaultOpenContext, MapFunction, OpenContext, RichMapFunction}
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.data.{DecimalDataUtils, GenericRowData, StringData, TimestampData}
import org.apache.flink.table.planner.codegen.{ConstantCodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.codegen.CodeGenUtils.DEFAULT_COLLECTOR_TERM
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.{BooleanType, DecimalType, LogicalType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.utils.DateTimeUtils

import org.apache.calcite.rex.RexNode

import java.time.ZoneId
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConversions._

/**
 * Utility class for partition pruning.
 *
 * Creates partition filter instance (a [[RichMapFunction]]) with partition predicates by code-gen,
 * and then evaluates all partition values against the partition filter to get final partitions.
 */
object PartitionPruner {

  // current supports partition field type
  val supportedPartitionFieldTypes = Array(
    VARCHAR,
    CHAR,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME_WITHOUT_TIME_ZONE,
    TIMESTAMP_WITHOUT_TIME_ZONE,
    TIMESTAMP_WITH_LOCAL_TIME_ZONE
  )

  /**
   * get pruned partitions from all partitions by partition filters
   *
   * @param partitionFieldNames
   *   Partition field names.
   * @param partitionFieldTypes
   *   Partition field types.
   * @param allPartitions
   *   All partition values.
   * @param partitionPredicate
   *   A predicate that will be applied against partition values.
   * @return
   *   Pruned partitions.
   */
  def prunePartitions(
      tableConfig: TableConfig,
      classLoader: ClassLoader,
      partitionFieldNames: Array[String],
      partitionFieldTypes: Array[LogicalType],
      allPartitions: JList[JMap[String, String]],
      partitionPredicate: RexNode): JList[JMap[String, String]] = {

    if (allPartitions.isEmpty || partitionPredicate.isAlwaysTrue) {
      return allPartitions
    }

    val inputType = InternalTypeInfo.ofFields(partitionFieldTypes, partitionFieldNames).toRowType
    val returnType: LogicalType = new BooleanType(false)

    val ctx = new ConstantCodeGeneratorContext(tableConfig, classLoader)
    val collectorTerm = DEFAULT_COLLECTOR_TERM

    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType)

    val filterExpression = exprGenerator.generateExpression(partitionPredicate)

    val filterFunctionBody =
      s"""
         |${filterExpression.code}
         |return ${filterExpression.resultTerm};
         |""".stripMargin

    val genFunction = FunctionCodeGenerator.generateFunction(
      ctx,
      "PartitionPruner",
      classOf[MapFunction[GenericRowData, Boolean]],
      filterFunctionBody,
      returnType,
      inputType,
      collectorTerm = collectorTerm)

    val function = genFunction.newInstance(classLoader)
    val richMapFunction = function match {
      case r: RichMapFunction[GenericRowData, Boolean] => r
      case _ => throw new TableException("RichMapFunction[GenericRowData, Boolean] required here")
    }

    val results: JList[Boolean] = new JArrayList[Boolean](allPartitions.size)
    val collector = new ListCollector[Boolean](results)

    try {
      richMapFunction.open(DefaultOpenContext.INSTANCE)
      // do filter against all partitions
      allPartitions.foreach {
        partition =>
          val row = convertPartitionToRow(
            TableConfigUtils.getLocalTimeZone(tableConfig),
            partitionFieldNames,
            partitionFieldTypes,
            partition)
          collector.collect(richMapFunction.map(row))
      }
    } finally {
      richMapFunction.close()
    }

    // get pruned partitions
    allPartitions.zipWithIndex.filter { case (_, index) => results.get(index) }.map(_._1)
  }

  /** create new Row from partition, set partition values to corresponding positions of row. */
  private def convertPartitionToRow(
      timeZone: ZoneId,
      partitionFieldNames: Array[String],
      partitionFieldTypes: Array[LogicalType],
      partition: JMap[String, String]): GenericRowData = {
    val row = new GenericRowData(partitionFieldNames.length)
    partitionFieldNames.zip(partitionFieldTypes).zipWithIndex.foreach {
      case ((fieldName, fieldType), index) =>
        val value = convertPartitionFieldValue(timeZone, partition(fieldName), fieldType)
        row.setField(index, value)
    }
    row
  }

  private def convertPartitionFieldValue(timeZone: ZoneId, v: String, t: LogicalType): Any = {
    if (v == null) {
      return null
    }
    t.getTypeRoot match {
      case VARCHAR | CHAR => StringData.fromString(v)
      case BOOLEAN => Boolean
      case TINYINT => v.toByte
      case SMALLINT => v.toShort
      case INTEGER => v.toInt
      case BIGINT => v.toLong
      case FLOAT => v.toFloat
      case DOUBLE => v.toDouble
      case DECIMAL =>
        val decimalType = t.asInstanceOf[DecimalType]
        DecimalDataUtils.castFrom(v, decimalType.getPrecision, decimalType.getScale)
      case DATE => DateTimeUtils.parseDate(v)
      case TIME_WITHOUT_TIME_ZONE => DateTimeUtils.parseTime(v)
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        DateTimeUtils.parseTimestampData(v, LogicalTypeChecks.getPrecision(t))
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        TimestampData.fromInstant(
          DateTimeUtils
            .parseTimestampData(v, LogicalTypeChecks.getPrecision(t))
            .toLocalDateTime
            .atZone(timeZone)
            .toInstant)
      case _ =>
        throw new TableException(s"$t is not supported in PartitionPruner")
    }
  }

}
