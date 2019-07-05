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

package org.apache.flink.table.codegen

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.FunctionCodeGenerator.generateFunction
import org.apache.flink.table.dataformat.BinaryStringUtil.safeToString
import org.apache.flink.table.dataformat.{BinaryString, BinaryStringUtil, Decimal, GenericRow}
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rex.{RexBuilder, RexExecutor, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.commons.lang3.StringEscapeUtils

import java.io.File

import scala.collection.JavaConverters._

/**
  * Evaluates constant expressions with code generator.
  *
  * @param allowChangeNullability If the reduced expr's nullability can be changed, e.g. a null
  *                               literal is definitely nullable and the other literals are
  *                               not null.
  */
class ExpressionReducer(
    config: TableConfig,
    allowChangeNullability: Boolean = false)
  extends RexExecutor {

  private val EMPTY_ROW_TYPE = RowType.of()
  private val EMPTY_ROW = new GenericRow(0)

  override def reduce(
      rexBuilder: RexBuilder,
      constExprs: java.util.List[RexNode],
      reducedValues: java.util.List[RexNode]): Unit = {

    val literals = constExprs.asScala.map(e => (e.getType.getSqlTypeName, e)).flatMap {

      // we don't support object literals yet, we skip those constant expressions
      case (SqlTypeName.ANY, _) |
           (SqlTypeName.ROW, _) |
           (SqlTypeName.ARRAY, _) |
           (SqlTypeName.MAP, _) |
           (SqlTypeName.MULTISET, _) => None

      case (_, e) => Some(e)
    }

    val literalTypes = literals.map(e => FlinkTypeFactory.toLogicalType(e.getType))
    val resultType = RowType.of(literalTypes: _*)

    // generate MapFunction
    val ctx = new ConstantCodeGeneratorContext(config)

    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(EMPTY_ROW_TYPE)

    val literalExprs = literals.map(exprGenerator.generateExpression)
    val result = exprGenerator.generateResultExpression(
      literalExprs, resultType, classOf[GenericRow])

    val generatedFunction = generateFunction[MapFunction[GenericRow, GenericRow]](
      ctx,
      "ExpressionReducer",
      classOf[MapFunction[GenericRow, GenericRow]],
      s"""
         |${result.code}
         |return ${result.resultTerm};
         |""".stripMargin,
      resultType,
      EMPTY_ROW_TYPE)

    val function = generatedFunction.newInstance(getClass.getClassLoader)
    val richMapFunction = function match {
      case r: RichMapFunction[GenericRow, GenericRow] => r
      case _ => throw new TableException("RichMapFunction[GenericRow, GenericRow] required here")
    }

    val parameters = if (config.getConf != null) config.getConf else new Configuration()
    val reduced = try {
      richMapFunction.open(parameters)
      // execute
      richMapFunction.map(EMPTY_ROW)
    } finally {
      richMapFunction.close()
    }

    // add the reduced results or keep them unreduced
    var i = 0
    var reducedIdx = 0
    while (i < constExprs.size()) {
      val unreduced = constExprs.get(i)
      unreduced.getType.getSqlTypeName match {
        // we insert the original expression for object literals
        case SqlTypeName.ANY |
             SqlTypeName.ROW |
             SqlTypeName.ARRAY |
             SqlTypeName.MAP |
             SqlTypeName.MULTISET =>
          reducedValues.add(unreduced)
        case SqlTypeName.VARCHAR | SqlTypeName.CHAR =>
          val escapeVarchar = StringEscapeUtils
            .escapeJava(safeToString(reduced.getField(reducedIdx).asInstanceOf[BinaryString]))
          reducedValues.add(maySkipNullLiteralReduce(rexBuilder, escapeVarchar, unreduced))
          reducedIdx += 1
        case SqlTypeName.VARBINARY | SqlTypeName.BINARY =>
          val reducedValue = reduced.getField(reducedIdx)
          val value = if (null != reducedValue) {
            new ByteString(reduced.getField(reducedIdx).asInstanceOf[Array[Byte]])
          } else {
            reducedValue
          }
          reducedValues.add(maySkipNullLiteralReduce(rexBuilder, value, unreduced))
          reducedIdx += 1
        case SqlTypeName.DECIMAL =>
          val reducedValue = reduced.getField(reducedIdx)
          val value = if (reducedValue != null) {
            reducedValue.asInstanceOf[Decimal].toBigDecimal
          } else {
            reducedValue
          }
          reducedValues.add(maySkipNullLiteralReduce(rexBuilder, value, unreduced))
          reducedIdx += 1
        case _ =>
          val reducedValue = reduced.getField(reducedIdx)
          // RexBuilder handle double literal incorrectly, convert it into BigDecimal manually
          val value = if (reducedValue != null &&
            unreduced.getType.getSqlTypeName == SqlTypeName.DOUBLE) {
            new java.math.BigDecimal(reducedValue.asInstanceOf[Number].doubleValue())
          } else {
            reducedValue
          }

          reducedValues.add(maySkipNullLiteralReduce(rexBuilder, value, unreduced))
          reducedIdx += 1
      }
      i += 1
    }
  }

  // We may skip the reduce if the original constant is invalid and casted as a null literal,
  // cause now this may change the RexNode's and it's parent node's nullability.
  def maySkipNullLiteralReduce(
      rexBuilder: RexBuilder,
      value: Object,
      unreduced: RexNode): RexNode = {
    if (!allowChangeNullability
      && value == null
      && !unreduced.getType.isNullable) {
      return unreduced
    }

    // used for table api to '+' of two strings.
    val valueArg = if (SqlTypeName.CHAR_TYPES.contains(unreduced.getType.getSqlTypeName) &&
      value != null) {
      value.toString
    } else {
      value
    }

    // if allowChangeNullability is allowed, we can reduce the outer abstract cast if the unreduced
    // expr type is nullable.
    val targetType = if (allowChangeNullability && unreduced.getType.isNullable) {
      rexBuilder.getTypeFactory.createTypeWithNullability(unreduced.getType, false)
    } else {
      unreduced.getType
    }
    rexBuilder.makeLiteral(
      valueArg,
      targetType,
      true)
  }
}

/**
  * A [[ConstantFunctionContext]] allows to obtain user-defined configuration information set
  * in [[TableConfig]].
  *
  * @param parameters User-defined configuration set in [[TableConfig]].
  */
private class ConstantFunctionContext(parameters: Configuration) extends FunctionContext(null) {

  override def getMetricGroup: MetricGroup = {
    throw new UnsupportedOperationException(
      "getMetricGroup is not supported when reducing expression")
  }

  override def getCachedFile(name: String): File = {
    throw new UnsupportedOperationException(
      "getCachedFile is not supported when reducing expression")
  }

  /**
    * Gets the user-defined configuration value associated with the given key as a string.
    *
    * @param key          key pointing to the associated value
    * @param defaultValue default value which is returned in case user-defined configuration
    *                     value is null or there is no value associated with the given key
    * @return (default) value associated with the given key
    */
  override def getJobParameter(key: String, defaultValue: String): String = {
    parameters.getString(key, defaultValue)
  }
}

/**
  * Constant expression code generator context.
  */
private class ConstantCodeGeneratorContext(tableConfig: TableConfig)
  extends CodeGeneratorContext(tableConfig) {
  override def addReusableFunction(
      function: UserDefinedFunction,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
      runtimeContextTerm: String = null): String = {
    super.addReusableFunction(function, classOf[ConstantFunctionContext], "parameters")
  }
}
