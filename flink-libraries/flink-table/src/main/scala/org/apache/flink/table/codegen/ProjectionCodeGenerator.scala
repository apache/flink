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

import org.apache.flink.table.api.types.RowType
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataformat._

/**
  * CodeGenerator for projection.
  */
object ProjectionCodeGenerator {

  /**
    * CodeGenerator for projection.
    * @param reusedOutRecord If objects or variables can be reused, they will be added a reusable
    * output record to the member area of the generated class. If not they will be as temp
    * variables.
    * @return
    */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true,
      nullCheck: Boolean = true): GeneratedProjection = {
    val className = newName(name)
    val baseClass = classOf[Projection[_, _]]

    // The BinaryRow never is POJO field, so we don't need set resultFieldNames.
    val resultGenerator = new ExprCodeGenerator(ctx, false, nullCheck)
      .bindInput(inType, inputTerm = inputTerm, inputFieldMapping = Some(inputMapping))
    val accessExprs = inputMapping.map(
      idx => CodeGenUtils.generateFieldAccess(
        ctx, inType.toInternalType, inputTerm, idx, nullCheck))

    val expression = resultGenerator.generateResultExpression(
      accessExprs,
      outType,
      classOf[BinaryRow],
      outRow = outRecordTerm,
      outRowWriter = Option(outRecordWriterTerm),
      reusedOutRow = reusedOutRecord)

    val code =
      j"""
      public class $className extends ${baseClass.getCanonicalName}<$BASE_ROW, $BINARY_ROW> {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public $BINARY_ROW apply($BASE_ROW $inputTerm) {
          ${ctx.reuseFieldCode()}
          ${expression.code}
          return ${expression.resultTerm};
        }
      }
    """.stripMargin

    GeneratedProjection(className, code, expression, inputMapping)
  }

  /**
    * For java invoke.
    */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int]): GeneratedProjection =
    generateProjection(
      ctx, name, inputType, outputType, inputMapping, inputTerm = DEFAULT_INPUT1_TERM)
}

abstract class Projection[IN <: BaseRow, OUT <: BaseRow] {
  def apply(row: IN): OUT
}
