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

import org.apache.flink.table.api.types.{RowType, DataTypes}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataformat._
import org.apache.flink.table.runtime.conversion.DataStructureConverters.genToExternal

/**
  * CodeGenerator for single field access of BaseRow
  */
object FieldAccessCodeGenerator {

  def generateRowFieldExtractor(
    ctx: CodeGeneratorContext,
    name: String,
    inType: RowType,
    index: Int,
    inputTerm: String = DEFAULT_INPUT1_TERM,
    outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
    outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM): GeneratedFieldExtractor = {

    val className = newName(name)

    val expr = CodeGenUtils.generateFieldAccess(
      ctx, inType.toInternalType, inputTerm, index, nullCheck = true)

    val outType = expr.resultType
    val resultTypeTerm = externalBoxedTermForType(outType)
    val baseClass = s"${classOf[FieldAccess[_,_]].getCanonicalName}<_, $resultTypeTerm>"

    // TODO gen To External will copy, so we don't need copy now...
    val code =
      j"""
      public class $className extends $baseClass {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public $resultTypeTerm extract($BASE_ROW $inputTerm) {
          ${ctx.reuseFieldCode()}
          ${expr.code}
          return ${genToExternal(ctx, outType, expr.resultTerm)};
        }
      }
    """.stripMargin

    GeneratedFieldExtractor(className, code, ctx.references.toArray)
  }

  /**
    * For java invoke.
    */
  def generateRowFieldExtractor(
    ctx: CodeGeneratorContext,
    name: String,
    inputType: RowType,
    index: Int): GeneratedFieldExtractor ={
    generateRowFieldExtractor(
      ctx, name, inputType, index, inputTerm = DEFAULT_INPUT1_TERM)
  }
}

abstract class FieldAccess[IN <: BaseRow, OUT] {
  def extract(row: IN): OUT
}
