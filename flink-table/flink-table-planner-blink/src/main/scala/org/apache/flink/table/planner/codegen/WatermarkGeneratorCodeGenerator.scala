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

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils.{BASE_ROW, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.{GeneratedWatermarkGenerator, WatermarkGenerator}
import org.apache.flink.table.types.logical.{LogicalTypeRoot, RowType}
import org.apache.calcite.rex.RexNode

/**
  * A code generator for generating [[WatermarkGenerator]]s.
  */
object WatermarkGeneratorCodeGenerator {

  def generateWatermarkGenerator(
      config: TableConfig,
      inputType: RowType,
      watermarkExpr: RexNode): GeneratedWatermarkGenerator = {
    // validation
    val watermarkOutputType = FlinkTypeFactory.toLogicalType(watermarkExpr.getType)
    if (watermarkOutputType.getTypeRoot != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
      throw new CodeGenException(
        "WatermarkGenerator only accepts output data type of TIMESTAMP," +
          " but is " + watermarkOutputType)
    }
    val funcName = newName("WatermarkGenerator")
    val ctx = CodeGeneratorContext(config)
    val generator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = "row")
    val generatedExpr = generator.generateExpression(watermarkExpr)

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
        public Long currentWatermark($BASE_ROW row) throws Exception {
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
    new GeneratedWatermarkGenerator(funcName, funcCode, ctx.references.toArray)
  }
}
