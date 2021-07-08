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

package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.data.binary.{BinaryStringData, BinaryStringDataUtil}
import org.apache.flink.table.data.{DecimalData, GenericRowData, TimestampData}
import org.apache.flink.table.functions.{ConstantFunctionContext, FunctionContext, UserDefinedFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.FunctionCodeGenerator.generateFunction
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.util.TimestampStringUtils.fromLocalDateTime

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rex.{RexBuilder, RexExecutor, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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
  extends RexExecutor
  with Logging {

  private val EMPTY_ROW_TYPE = RowType.of()
  private val EMPTY_ROW = new GenericRowData(0)

  override def reduce(
      rexBuilder: RexBuilder,
      constExprs: java.util.List[RexNode],
      reducedValues: java.util.List[RexNode]): Unit = {

    val pythonUDFExprs = new ListBuffer[RexNode]()

    val literals = constExprs.asScala.map(e => (e.getType.getSqlTypeName, e)).flatMap {

      // Skip expressions that contain python functions because it's quite expensive to
      // call Python UDFs during optimization phase. They will be optimized during the runtime.
      case (_, e) if containsPythonCall(e) =>
        pythonUDFExprs += e
        None

      // we don't support object literals yet, we skip those constant expressions
      case (SqlTypeName.ANY, _) |
           (SqlTypeName.OTHER, _) |
           (SqlTypeName.ROW, _) |
           (SqlTypeName.STRUCTURED, _) |
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
      literalExprs, resultType, classOf[GenericRowData])

    val generatedFunction = generateFunction[MapFunction[GenericRowData, GenericRowData]](
      ctx,
      "ExpressionReducer",
      classOf[MapFunction[GenericRowData, GenericRowData]],
      s"""
         |${result.code}
         |return ${result.resultTerm};
         |""".stripMargin,
      resultType,
      EMPTY_ROW_TYPE)

    val function = generatedFunction.newInstance(Thread.currentThread().getContextClassLoader)
    val richMapFunction = function match {
      case r: RichMapFunction[GenericRowData, GenericRowData] => r
      case _ =>
        throw new TableException("RichMapFunction[GenericRowData, GenericRowData] required here")
    }

    val parameters = config.getConfiguration
    val reduced = try {
      richMapFunction.open(parameters)
      // execute
      richMapFunction.map(EMPTY_ROW)
    } catch { case t: Throwable =>
      // maybe a function accesses some cluster specific context information
      // skip the expression reduction and try it again during runtime
      LOG.warn(
        "Unable to perform constant expression reduction. " +
          "An exception occurred during the evaluation. " +
          "One or more expressions will be executed unreduced.",
        t)
      reducedValues.addAll(constExprs)
      return
    } finally {
      richMapFunction.close()
    }

    // add the reduced results or keep them unreduced
    var i = 0
    var reducedIdx = 0
    while (i < constExprs.size()) {
      val unreduced = constExprs.get(i)
      // use eq to compare reference
      if (pythonUDFExprs.exists(_ eq unreduced)) {
        // if contains python function then just insert the original expression.
        reducedValues.add(unreduced)
      } else {
        unreduced.getType.getSqlTypeName match {
          // we insert the original expression for object literals
          case SqlTypeName.ANY |
               SqlTypeName.OTHER |
               SqlTypeName.ROW |
               SqlTypeName.STRUCTURED |
               SqlTypeName.ARRAY |
               SqlTypeName.MAP |
               SqlTypeName.MULTISET =>
            reducedValues.add(unreduced)
          case SqlTypeName.VARCHAR | SqlTypeName.CHAR =>
            val escapeVarchar = BinaryStringDataUtil.safeToString(
              reduced.getField(reducedIdx).asInstanceOf[BinaryStringData])
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
          case SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
            val reducedValue = reduced.getField(reducedIdx)
            val value = if (reducedValue != null) {
              val dt = reducedValue.asInstanceOf[TimestampData].toLocalDateTime
              fromLocalDateTime(dt)
            } else {
              reducedValue
            }
            reducedValues.add(maySkipNullLiteralReduce(rexBuilder, value, unreduced))
            reducedIdx += 1
          case SqlTypeName.DECIMAL =>
            val reducedValue = reduced.getField(reducedIdx)
            val value = if (reducedValue != null) {
              reducedValue.asInstanceOf[DecimalData].toBigDecimal
            } else {
              reducedValue
            }
            reducedValues.add(maySkipNullLiteralReduce(rexBuilder, value, unreduced))
            reducedIdx += 1
          case SqlTypeName.TIMESTAMP =>
            val reducedValue = reduced.getField(reducedIdx)
            val value = if (reducedValue != null) {
              val dt = reducedValue.asInstanceOf[TimestampData].toLocalDateTime
              fromLocalDateTime(dt)
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
  * Constant expression code generator context.
  */
class ConstantCodeGeneratorContext(tableConfig: TableConfig)
  extends CodeGeneratorContext(tableConfig) {
  override def addReusableFunction(
      function: UserDefinedFunction,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
      runtimeContextTerm: String = null): String = {
    super.addReusableFunction(function, classOf[ConstantFunctionContext], "parameters")
  }

  override def addReusableConverter(
      dataType: DataType,
      classLoaderTerm: String = null)
    : String = {
    super.addReusableConverter(dataType, "this.getClass().getClassLoader()")
  }
}
