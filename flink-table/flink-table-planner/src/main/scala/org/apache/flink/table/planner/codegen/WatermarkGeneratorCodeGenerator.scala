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

import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, ROW_DATA}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.{GeneratedWatermarkGenerator, WatermarkGenerator, WatermarkGeneratorCodeGeneratorFunctionContextWrapper}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalTypeRoot, RowType}

import org.apache.calcite.rex.RexNode

/** A code generator for generating [[WatermarkGenerator]]s. */
object WatermarkGeneratorCodeGenerator {

  /**
   * Generates a [[WatermarkGenerator]]. The generator is also able to provide the event-time
   * timestamp for advanced watermark strategies if the given parameter has been specified.
   */
  def generateWatermarkGenerator(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      inputType: RowType,
      watermarkExpr: RexNode,
      rowtimeExpr: Option[RexNode] = None,
      contextTerm: Option[String] = None): GeneratedWatermarkGenerator = {
    // validation
    val watermarkOutputType = FlinkTypeFactory.toLogicalType(watermarkExpr.getType)
    if (
      !(watermarkOutputType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE ||
        watermarkOutputType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
    ) {
      throw new CodeGenException(
        "WatermarkGenerator only accepts output data type of TIMESTAMP or TIMESTAMP_LTZ," +
          " but is " + watermarkOutputType)
    }
    val ctx = if (contextTerm.isDefined) {
      new WatermarkGeneratorFunctionContext(tableConfig, classLoader, contextTerm.get)
    } else {
      new CodeGeneratorContext(tableConfig, classLoader)
    }
    val funcName = newName(ctx, "WatermarkGenerator")
    val generator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = "row")
      .bindConstructorTerm(contextTerm.orNull)
    val generatedWatermarkExpr = generator.generateExpression(watermarkExpr)

    val generatedTimestampCode = if (rowtimeExpr.isDefined) {
      val generatedRowtimeExpr = generator.generateExpression(rowtimeExpr.get)
      j"""
         |${ctx.reusePerRecordCode()}
         |${ctx.reuseLocalVariableCode()}
         |${ctx.reuseInputUnboxingCode()}
         |${generatedRowtimeExpr.code}
         |if (${generatedRowtimeExpr.nullTerm}) {
         |  return Long.MIN_VALUE;
         |} else {
         |  return ${generatedRowtimeExpr.resultTerm}.getMillisecond();
         |}
         |""".stripMargin
    } else {
      "return Long.MIN_VALUE;"
    }

    if (contextTerm.isDefined) {
      ctx.addReusableMember(
        s"""
           |private transient ${classOf[WatermarkGeneratorSupplier.Context].getCanonicalName}
           |${contextTerm.get};
           |""".stripMargin
      )
      ctx.addReusableInitStatement(
        s"""
           |int len = references.length;
           |${contextTerm.get} =
           |(${classOf[WatermarkGeneratorSupplier.Context].getCanonicalName}) references[len-1];
           |""".stripMargin
      )
    }

    val funcCode =
      j"""
      public final class $funcName
          extends ${classOf[WatermarkGenerator].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${classOf[OpenContext].getCanonicalName} openContext) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public long extractTimestamp($ROW_DATA row) {
          $generatedTimestampCode
        }

        @Override
        public Long currentWatermark($ROW_DATA row) throws Exception {
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${generatedWatermarkExpr.code}
          if (${generatedWatermarkExpr.nullTerm}) {
            return null;
          } else {
            return ${generatedWatermarkExpr.resultTerm}.getMillisecond();
          }
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin

    new GeneratedWatermarkGenerator(funcName, funcCode, ctx.references.toArray, ctx.tableConfig)
  }
}

class WatermarkGeneratorFunctionContext(
    tableConfig: ReadableConfig,
    classLoader: ClassLoader,
    contextTerm: String)
  extends CodeGeneratorContext(tableConfig, classLoader) {

  override def addReusableFunction(
      function: UserDefinedFunction,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
      contextArgs: Seq[String] = null): String = {
    super.addReusableFunction(
      function,
      classOf[WatermarkGeneratorCodeGeneratorFunctionContextWrapper],
      Seq(contextTerm))
  }

  override def addReusableConverter(dataType: DataType, classLoaderTerm: String = null): String = {
    super.addReusableConverter(dataType, "this.getClass().getClassLoader()")
  }
}
