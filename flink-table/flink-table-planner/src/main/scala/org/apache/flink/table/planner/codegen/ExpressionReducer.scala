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

import org.apache.flink.api.common.functions.{MapFunction, OpenContext, RichMapFunction}
import org.apache.flink.configuration.{Configuration, PipelineOptions, ReadableConfig}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.data.{DecimalData, GenericRowData, TimestampData}
import org.apache.flink.table.data.binary.{BinaryStringData, BinaryStringDataUtil}
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.FunctionCodeGenerator.generateFunction
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.{JSON_ARRAY, JSON_OBJECT}
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.planner.utils.TimestampStringUtils.fromLocalDateTime
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlKind

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Evaluates constant expressions with code generator.
 *
 * @param allowChangeNullability
 *   If the reduced expr's nullability can be changed, e.g. a null literal is definitely nullable
 *   and the other literals are not null.
 */
class ExpressionReducer(
    tableConfig: TableConfig,
    classLoader: ClassLoader,
    allowChangeNullability: Boolean = false)
  extends RexExecutor
  with Logging {

  private val EMPTY_ROW_TYPE = RowType.of()
  private val EMPTY_ROW = new GenericRowData(0)

  private val nonReducibleJsonFunctions = Seq(JSON_OBJECT, JSON_ARRAY)

  override def reduce(
      rexBuilder: RexBuilder,
      constExprs: java.util.List[RexNode],
      reducedValues: java.util.List[RexNode]): Unit = {

    val pythonUDFExprs = new ListBuffer[RexNode]()

    val literals = skipAndValidateExprs(rexBuilder, constExprs, pythonUDFExprs)

    val literalTypes = literals.map(e => FlinkTypeFactory.toLogicalType(e.getType))
    val resultType = RowType.of(literalTypes: _*)

    // generate MapFunction
    val ctx = new ConstantCodeGeneratorContext(tableConfig, classLoader)

    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(EMPTY_ROW_TYPE)

    val literalExprs = literals.map(exprGenerator.generateExpression)
    val result =
      exprGenerator.generateResultExpression(literalExprs, resultType, classOf[GenericRowData])

    val generatedFunction = generateFunction[MapFunction[GenericRowData, GenericRowData]](
      ctx,
      "ExpressionReducer",
      classOf[MapFunction[GenericRowData, GenericRowData]],
      s"""
         |${result.code}
         |return ${result.resultTerm};
         |""".stripMargin,
      resultType,
      EMPTY_ROW_TYPE
    )

    val function = generatedFunction.newInstance(classLoader)
    val richMapFunction = function match {
      case r: RichMapFunction[GenericRowData, GenericRowData] => r
      case _ =>
        throw new TableException("RichMapFunction[GenericRowData, GenericRowData] required here")
    }

    val parameters = toScala(tableConfig.getOptional(PipelineOptions.GLOBAL_JOB_PARAMETERS))
      .map(Configuration.fromMap)
      .getOrElse(new Configuration)
    val reduced =
      try {
        richMapFunction.open(parameters)
        // execute
        richMapFunction.map(EMPTY_ROW)
      } catch {
        case t: Throwable =>
          // maybe a function accesses some cluster specific context information
          // skip the expression reduction and try it again during runtime
          LOG.warn(
            "Unable to perform constant expression reduction. " +
              "An exception occurred during the evaluation. " +
              "One or more expressions will be executed unreduced.",
            t
          )
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
      } else
        unreduced match {
          case call: RexCall if nonReducibleJsonFunctions.contains(call.getOperator) =>
            reducedValues.add(unreduced)
          case _ =>
            unreduced.getType.getSqlTypeName match {
              // we insert the original expression for object literals
              case SqlTypeName.ANY | SqlTypeName.OTHER | SqlTypeName.ROW | SqlTypeName.STRUCTURED |
                  SqlTypeName.ARRAY | SqlTypeName.MAP | SqlTypeName.MULTISET =>
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
                if (
                  reducedValue != null &&
                  unreduced.getType.getSqlTypeName == SqlTypeName.DOUBLE
                ) {
                  val doubleVal = reducedValue.asInstanceOf[Number].doubleValue()
                  if (doubleVal.isInfinity || doubleVal.isNaN) {
                    reducedValues.add(unreduced)
                  } else {
                    reducedValues.add(
                      maySkipNullLiteralReduce(
                        rexBuilder,
                        new java.math.BigDecimal(doubleVal),
                        unreduced))
                  }
                } else {
                  reducedValues.add(maySkipNullLiteralReduce(rexBuilder, reducedValue, unreduced))
                }
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
    if (
      !allowChangeNullability
      && value == null
      && !unreduced.getType.isNullable
    ) {
      return unreduced
    }

    // used for table api to '+' of two strings.
    val valueArg =
      if (
        SqlTypeName.CHAR_TYPES.contains(unreduced.getType.getSqlTypeName) &&
        value != null
      ) {
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
    rexBuilder.makeLiteral(valueArg, targetType, true)
  }

  /** skip the expressions that can't be reduced now and validate the expressions */
  private def skipAndValidateExprs(
      rexBuilder: RexBuilder,
      constExprs: java.util.List[RexNode],
      pythonUDFExprs: ListBuffer[RexNode]): List[RexNode] = {
    constExprs.asScala
      .map(e => (e.getType.getSqlTypeName, e))
      .flatMap {

        // Skip expressions that contain python functions because it's quite expensive to
        // call Python UDFs during optimization phase. They will be optimized during the runtime.
        case (_, e) if containsPythonCall(e) =>
          pythonUDFExprs += e
          None

        // we don't support object literals yet, we skip those constant expressions
        case (SqlTypeName.ANY, _) | (SqlTypeName.OTHER, _) | (SqlTypeName.ROW, _) |
            (SqlTypeName.STRUCTURED, _) | (SqlTypeName.ARRAY, _) | (SqlTypeName.MAP, _) |
            (SqlTypeName.MULTISET, _) =>
          None

        case (_, call: RexCall) => {
          // to ensure the division is non-zero when the operator is DIVIDE
          if (call.getOperator.getKind.equals(SqlKind.DIVIDE)) {
            val ops = call.getOperands
            val divisionLiteral = ops.get(ops.size() - 1)

            // according to BuiltInFunctionDefinitions, the DEVIDE's second op must be numeric
            assert(RexUtil.isDeterministic(divisionLiteral))
            val divisionComparable =
              divisionLiteral.asInstanceOf[RexLiteral].getValue.asInstanceOf[Comparable[Any]]
            val zeroComparable = rexBuilder
              .makeExactLiteral(new java.math.BigDecimal(0))
              .getValue
              .asInstanceOf[Comparable[Any]]
            if (divisionComparable.compareTo(zeroComparable) == 0) {
              throw new ArithmeticException("Division by zero")
            }
          }
          // Exclude some JSON functions which behave differently
          // when called as an argument of another call of one of these functions.
          if (nonReducibleJsonFunctions.contains(call.getOperator)) {
            None
          } else {
            Some(call)
          }
        }
        case (_, e) => Some(e)
      }
      .toList
  }
}

/** Constant expression code generator context. */
class ConstantCodeGeneratorContext(tableConfig: ReadableConfig, classLoader: ClassLoader)
  extends CodeGeneratorContext(tableConfig, classLoader) {
  override def addReusableFunction(
      function: UserDefinedFunction,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
      contextArgs: Seq[String] = null): String = {
    super.addReusableFunction(
      function,
      classOf[FunctionContext],
      Seq("null", "this.getClass().getClassLoader()", "parameters"))
  }

  override def addReusableConverter(dataType: DataType, classLoaderTerm: String = null): String = {
    super.addReusableConverter(dataType, "this.getClass().getClassLoader()")
  }
}
