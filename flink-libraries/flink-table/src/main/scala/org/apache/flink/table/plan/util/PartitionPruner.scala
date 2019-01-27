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

import java.math.{BigInteger => JBigInteger}
import java.util.{ArrayList => JArrayList, List => JList}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, BigDecimalTypeInfo, TypeInformation}
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGeneratorContext, Compiler, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.dataformat.{BinaryString, Decimal, GenericRow}
import org.apache.flink.table.sources.Partition
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._

/**
  * The base class for partition pruning.
  *
  * Creates partition filter instance (a [[FlatMapFunction]]) with partition predicates by code-gen,
  * and then evaluates all partition values against the partition filter to get final partitions.
  *
  */
abstract class PartitionPruner extends Compiler[FlatMapFunction[GenericRow, Boolean]] {

  /**
    * get pruned partitions from all partitions by partition filters
    *
    * @param partitionFieldNames Partition field names.
    * @param partitionFieldTypes Partition field types.
    * @param allPartitions       All partition values.
    * @param partitionPredicates A filter expression that will be applied against partition values.
    * @param relBuilder          Builder for relational expressions.
    * @return Pruned partitions.
    */
  def getPrunedPartitions(
    partitionFieldNames: Array[String],
    partitionFieldTypes: Array[TypeInformation[_]],
    allPartitions: JList[Partition],
    partitionPredicates: Array[Expression],
    relBuilder: RelBuilder): JList[Partition] = {

    if (allPartitions.isEmpty || partitionPredicates.isEmpty) {
      return allPartitions
    }

    // convert predicates to RexNode
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val relDataType = typeFactory.buildLogicalRowType(partitionFieldNames, partitionFieldTypes)
    val predicateRexNode = convertPredicatesToRexNode(partitionPredicates, relBuilder, relDataType)

    // TODO use TableEnvironment's config
    val config = new TableConfig
    val rowType = new BaseRowTypeInfo(partitionFieldTypes, partitionFieldNames)
    val returnType = BasicTypeInfo.BOOLEAN_TYPE_INFO.asInstanceOf[TypeInformation[Any]]

    val ctx = CodeGeneratorContext(config)
    val collectorTerm = CodeGeneratorContext.DEFAULT_COLLECTOR_TERM

    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(TypeConverters.createInternalTypeFromTypeInfo(rowType))

    val filterExpression = exprGenerator.generateExpression(predicateRexNode)

    val filterFunctionBody =
      s"""
         |${filterExpression.code}
         |if (${filterExpression.resultTerm}) {
         |  $collectorTerm.collect(true);
         |} else {
         |  $collectorTerm.collect(false);
         |}
         |""".stripMargin

    val genFunction = FunctionCodeGenerator.generateFunction(
      ctx,
      "PartitionPruner",
      classOf[FlatMapFunction[GenericRow, Boolean]],
      filterFunctionBody,
      TypeConverters.createInternalTypeFromTypeInfo(returnType),
      TypeConverters.createInternalTypeFromTypeInfo(rowType),
      config,
      collectorTerm = collectorTerm)

    // create filter class instance
    val clazz = compile(getClass.getClassLoader, genFunction.name, genFunction.code)
    val function = clazz.newInstance()

    val results: JList[Boolean] = new JArrayList[Boolean](allPartitions.size)
    val collector = new ListCollector[Boolean](results)

    // do filter against all partitions
    allPartitions.asScala.foreach {
      partition =>
        val row = convertPartitionToRow(partitionFieldNames, partitionFieldTypes, partition)
        function.flatMap(row, collector)
    }

    // get pruned partitions
    allPartitions.asScala.zipWithIndex.filter {
      case (_, index) => results.get(index)
    }.unzip._1.asJava
  }

  /**
    * create new Row from partition, set partition values to corresponding positions of row.
    */
  def convertPartitionToRow(
    partitionFieldNames: Array[String],
    partitionFieldTypes: Array[TypeInformation[_]],
    partition: Partition): GenericRow = {

    val row = new GenericRow(partitionFieldNames.length)
    partitionFieldNames.zip(partitionFieldTypes).zipWithIndex.foreach {
      case ((fieldName, fieldType), index) =>
        val value = convertPartitionFieldValue(partition.getFieldValue(fieldName), fieldType)
        row.update(index, value)
    }
    row
  }

  /**
    * Converts a collection of expressions into an AND RexNode.
    */
  private def convertPredicatesToRexNode(
    predicates: Array[Expression],
    relBuilder: RelBuilder,
    relDataType: RelDataType): RexNode = {

    relBuilder.values(relDataType)
    predicates.map(expr => expr.toRexNode(relBuilder)).reduce((l, r) => relBuilder.and(l, r))
  }

  /**
    * Convert partition field value to expect type object value
    *
    * @param partitionFieldValue partition field value
    * @param partitionFieldType  partition field types
    * @return The expect type object value
    */
  def convertPartitionFieldValue(
    partitionFieldValue: Any,
    partitionFieldType: TypeInformation[_]): Any

}

/**
  * Default implementation of PartitionPruner
  */
class DefaultPartitionPrunerImpl extends PartitionPruner {

  // by default supports BasicTypeInfo conversion, excluding DATE_TYPE_INFO and VOID_TYPE_INFO
  override def convertPartitionFieldValue(partitionFieldValue: Any,
    partitionFieldType: TypeInformation[_]): Any = {
    partitionFieldValue match {
      case null => null
      case _ =>
        val value = partitionFieldValue.toString
        partitionFieldType match {
          case BasicTypeInfo.STRING_TYPE_INFO => BinaryString.fromString(value)
          case BasicTypeInfo.BOOLEAN_TYPE_INFO => value.toBoolean
          case BasicTypeInfo.BYTE_TYPE_INFO => value.toByte
          case BasicTypeInfo.SHORT_TYPE_INFO => value.toShort
          case BasicTypeInfo.INT_TYPE_INFO => value.toInt
          case BasicTypeInfo.LONG_TYPE_INFO => value.toLong
          case BasicTypeInfo.FLOAT_TYPE_INFO => value.toFloat
          case BasicTypeInfo.DOUBLE_TYPE_INFO => value.toDouble
          case BasicTypeInfo.CHAR_TYPE_INFO =>
            Preconditions.checkArgument(value.length == 1)
            value.charAt(0)
          case BasicTypeInfo.BIG_INT_TYPE_INFO => new JBigInteger(value)
          case dt: BigDecimalTypeInfo => Decimal.castFrom(value, dt.precision, dt.scale)
          case _ => throw new TableException(s"Unsupported Type: $partitionFieldType, " +
            s"please extends PartitionPruner to support it.")
        }
    }
  }
}

object PartitionPruner {
  val INSTANCE = new DefaultPartitionPrunerImpl
}
