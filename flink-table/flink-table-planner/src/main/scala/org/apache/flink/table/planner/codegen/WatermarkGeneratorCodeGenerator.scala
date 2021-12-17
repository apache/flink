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
import org.apache.flink.api.common.externalresource.ExternalResourceInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils.{ROW_DATA, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.{GeneratedWatermarkGenerator, WatermarkGenerator}
import org.apache.flink.table.types.logical.{LogicalTypeRoot, RowType}
import org.apache.flink.table.types.DataType

import org.apache.calcite.rex.RexNode

import java.io.File
import java.util

/**
  * A code generator for generating [[WatermarkGenerator]]s.
  */
object WatermarkGeneratorCodeGenerator {

  def generateWatermarkGenerator(
      config: TableConfig,
      inputType: RowType,
      watermarkExpr: RexNode,
      contextTerm: Option[String] = None): GeneratedWatermarkGenerator = {
    // validation
    val watermarkOutputType = FlinkTypeFactory.toLogicalType(watermarkExpr.getType)
    if (!(watermarkOutputType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE ||
      watermarkOutputType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
      throw new CodeGenException(
        "WatermarkGenerator only accepts output data type of TIMESTAMP or TIMESTAMP_LTZ," +
          " but is " + watermarkOutputType)
    }
    val funcName = newName("WatermarkGenerator")
    val ctx = if (contextTerm.isDefined) {
      new WatermarkGeneratorFunctionContext(config, contextTerm.get)
    } else {
      CodeGeneratorContext(config)
    }
    val generator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = "row")
      .bindConstructorTerm(contextTerm.orNull)
    val generatedExpr = generator.generateExpression(watermarkExpr)

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
        public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public Long currentWatermark($ROW_DATA row) throws Exception {
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${generatedExpr.code}
          if (${generatedExpr.nullTerm}) {
            return null;
          } else {
            return ${generatedExpr.resultTerm}.getMillisecond();
          }
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin

    new GeneratedWatermarkGenerator(
      funcName, funcCode, ctx.references.toArray, ctx.tableConfig.getConfiguration)
  }
}

class WatermarkGeneratorFunctionContext(
    tableConfig: TableConfig,
    contextTerm: String = "parameters") extends CodeGeneratorContext(tableConfig) {

  override def addReusableFunction(
      function: UserDefinedFunction,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
      runtimeContextTerm: String = null): String = {
    super.addReusableFunction(
      function, classOf[WatermarkGeneratorCodeGeneratorFunctionContextWrapper], this.contextTerm)
  }

  override def addReusableConverter(
      dataType: DataType,
      classLoaderTerm: String = null): String = {
    super.addReusableConverter(dataType, "this.getClass().getClassLoader()")
  }
}

class WatermarkGeneratorCodeGeneratorFunctionContextWrapper(
    context: WatermarkGeneratorSupplier.Context) extends FunctionContext(null) {

  override def getMetricGroup: MetricGroup = context.getMetricGroup

  override def getCachedFile(name: String): File = {
    throw new TableException(
      "Calls to FunctionContext.getCachedFile are not available " +
        "for WatermarkGeneratorCodeGeneratorFunctionContextWrapper.")
  }

  override def getJobParameter(key: String, defaultValue: String): String = {
    throw new TableException(
      "Calls to FunctionContext.getJobParameter are not available " +
        "for WatermarkGeneratorCodeGeneratorFunctionContextWrapper")

  }

  override def getExternalResourceInfos(resourceName: String): util.Set[ExternalResourceInfo] = {
    throw new TableException(
      "Calls to FunctionContext.getExternalResourceInfos are not available " +
        "for WatermarkGeneratorCodeGeneratorFunctionContextWrapper")
  }
}
