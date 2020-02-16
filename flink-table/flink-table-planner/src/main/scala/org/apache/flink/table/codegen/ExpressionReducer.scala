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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.{Configuration, PipelineOptions}
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.plan.util.PythonUtil.containsPythonCall
import org.apache.flink.types.Row

import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Evaluates constant expressions using Flink's [[ConstantFunctionCodeGenerator]].
  */
class ExpressionReducer(config: TableConfig)
  extends RelOptPlanner.Executor with Compiler[MapFunction[Row, Row]] {

  private val EMPTY_ROW_INFO = new RowTypeInfo()
  private val EMPTY_ROW = new Row(0)

  override def reduce(
    rexBuilder: RexBuilder,
    constExprs: util.List[RexNode],
    reducedValues: util.List[RexNode]): Unit = {

    val typeFactory = rexBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val pythonUDFExprs = ListBuffer[RexNode]()

    val literals = constExprs.asScala.map(e => (e.getType.getSqlTypeName, e)).flatMap {

      // Skip expressions that contain python functions because it's quite expensive to
      // call Python UDFs during optimization phase. They will be optimized during the runtime.
      case (_, e) if containsPythonCall(e) =>
        pythonUDFExprs += e
        None

      // we need to cast here for RexBuilder.makeLiteral
      case (SqlTypeName.DATE, e) =>
        Some(
          rexBuilder.makeCast(
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, e.getType.isNullable),
            e)
        )
      case (SqlTypeName.TIME, e) =>
        Some(
          rexBuilder.makeCast(
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, e.getType.isNullable),
            e)
        )
      case (SqlTypeName.TIMESTAMP, e) =>
        Some(
          rexBuilder.makeCast(
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, e.getType.isNullable),
            e)
        )

      // we don't support object literals yet, we skip those constant expressions
      case (SqlTypeName.ANY, _) |
           (SqlTypeName.ROW, _) |
           (SqlTypeName.ARRAY, _) |
           (SqlTypeName.MAP, _) |
           (SqlTypeName.MULTISET, _) => None

      case (_, e) => Some(e)
    }

    val literalTypes = literals.map(e => FlinkTypeFactory.toTypeInfo(e.getType))
    val resultType = new RowTypeInfo(literalTypes: _*)

    // generate MapFunction
    val generator = new ConstantFunctionCodeGenerator(config, false, EMPTY_ROW_INFO)

    val result = generator.generateResultExpression(
      resultType,
      resultType.getFieldNames,
      literals)

    val generatedFunction = generator.generateFunction[MapFunction[Row, Row], Row](
      "ExpressionReducer",
      classOf[MapFunction[Row, Row]],
      s"""
        |${result.code}
        |return ${result.resultTerm};
        |""".stripMargin,
      resultType)

    val clazz = compile(
      Thread.currentThread().getContextClassLoader,
      generatedFunction.name,
      generatedFunction.code)
    val function = clazz.newInstance()

    val reduced = try {
      val parameters = config.getConfiguration.getOptional(PipelineOptions.GLOBAL_JOB_PARAMETERS)
      val configuration = new Configuration()
      if (parameters.isPresent) {
        parameters.get().asScala.foreach(e =>
          configuration.setString(e._1, e._2)
        )
      }
      FunctionUtils.openFunction(function, configuration)
      function.map(EMPTY_ROW)
    } finally {
      FunctionUtils.closeFunction(function)
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
               SqlTypeName.ROW |
               SqlTypeName.ARRAY |
               SqlTypeName.MAP |
               SqlTypeName.MULTISET =>
            reducedValues.add(unreduced)

          case _ =>
            val reducedValue = reduced.getField(reducedIdx)
            // RexBuilder handle double literal incorrectly, convert it into BigDecimal manually
            val value = if (unreduced.getType.getSqlTypeName == SqlTypeName.DOUBLE) {
              if (reducedValue == null) {
                reducedValue
              } else {
                new java.math.BigDecimal(reducedValue.asInstanceOf[Number].doubleValue())
              }
            } else {
              reducedValue
            }

            val literal = rexBuilder.makeLiteral(
              value,
              unreduced.getType,
              true)
            reducedValues.add(literal)
            reducedIdx += 1
        }
      }
      i += 1
    }
  }
}

/**
  * A [[ConstantFunctionContext]] allows to obtain user-defined configuration information set
  * in [[TableConfig]].
  *
  * @param parameters User-defined configuration set in [[TableConfig]].
  */
class ConstantFunctionContext(parameters: Configuration) extends FunctionContext(null) {

  override def getMetricGroup: MetricGroup = {
    throw new UnsupportedOperationException("getMetricGroup is not supported when optimizing")
  }

  override def getCachedFile(name: String): File = {
    throw new UnsupportedOperationException("getCachedFile is not supported when optimizing")
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
  * A [[ConstantFunctionCodeGenerator]] used for constant expression code generator
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input1 type information about the first input of the Function
  */
class ConstantFunctionCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input1: TypeInformation[_ <: Any])
  extends FunctionCodeGenerator(config, nullableInput, input1) {
  /**
    * Adds a reusable [[UserDefinedFunction]] to the member area of the generated [[Function]].
    *
    * @param function    [[UserDefinedFunction]] object to be instantiated during runtime
    * @param contextTerm term to access the Context
    * @return member variable term
    */
  override def addReusableFunction(
      function: UserDefinedFunction,
      contextTerm: String = null,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext]): String = {
    super.addReusableFunction(function, "parameters", classOf[ConstantFunctionContext])
  }
}
