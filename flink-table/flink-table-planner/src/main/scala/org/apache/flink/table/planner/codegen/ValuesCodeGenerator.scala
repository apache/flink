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

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.connector.datagen.source.GeneratorFunction
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.planner.codegen.CodeGenUtils.newName
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.values.ValuesInputFormat
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex.RexLiteral

import java.util

import scala.collection.JavaConversions._

object ValuesCodeGenerator {

  def generatorInputFormat(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      outputType: RowType,
      tuples: util.List[util.List[RexLiteral]],
      description: String): ValuesInputFormat = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    // generate code for every record
    val generatedRecords = generateRecords(ctx, outputType, tuples, reusedOutRow = true)

    // generate input format
    val generatedFunction = InputFormatCodeGenerator.generateValuesInputFormat[RowData](
      ctx,
      description,
      generatedRecords.map(_.code),
      outputType)

    new ValuesInputFormat(generatedFunction, InternalTypeInfo.of(outputType))
  }

  /**
   * Generates a [[GeneratorFunction]] that returns the record with the given row number. It creates
   * a new record on every call.
   */
  def generatorFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      outputType: RowType,
      tuples: util.List[util.List[RexLiteral]],
      description: String): GeneratedFunction[GeneratorFunction[java.lang.Long, RowData]] = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val generatedRecords = generateRecords(ctx, outputType, tuples, reusedOutRow = false)
    val funcName = newName(ctx, description)

    val funcCode = j"""
      public class $funcName implements ${classOf[GeneratorFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public Object map(Object rowNumber) throws Exception {
          final long index = ((Long) rowNumber).longValue();
          ${generatedRecords.zipWithIndex
                       .map {
                         case (r, i) =>
                           s"""
                              |if (index == ${i}L) {
                              |  ${r.code}
                              |  return ${r.resultTerm};
                              |}
                              |""".stripMargin
                       }
                       .mkString("")}
          throw new IllegalArgumentException(
            "Row " + index + " requested, but the VALUES clause has ${tuples.size()} rows.");
        }
      }
    """.stripMargin

    new GeneratedFunction[GeneratorFunction[java.lang.Long, RowData]](
      funcName,
      funcCode,
      ctx.references.toArray,
      ctx.tableConfig)
  }

  private def generateRecords(
      ctx: CodeGeneratorContext,
      outputType: RowType,
      tuples: util.List[util.List[RexLiteral]],
      reusedOutRow: Boolean): Seq[GeneratedExpression] = {
    val exprGenerator = new ExprCodeGenerator(ctx, false)
    tuples.map {
      r =>
        exprGenerator.generateResultExpression(
          r.map(exprGenerator.generateExpression),
          outputType,
          classOf[GenericRowData],
          reusedOutRow = reusedOutRow)
    }
  }
}
