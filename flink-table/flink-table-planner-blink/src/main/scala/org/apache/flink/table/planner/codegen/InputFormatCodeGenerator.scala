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

import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.planner.codegen.CodeGenUtils.newName
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.GeneratedInput
import org.apache.flink.table.types.logical.LogicalType

/**
  * A code generator for generating Flink [[GenericInputFormat]]s.
  */
object InputFormatCodeGenerator {

  /**
    * Generates a values input format that can be passed to Java compiler.
    *
    * @param ctx The code generator context
    * @param name Class name of the input format. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param records code for creating records
    * @param returnType expected return type
    * @param outRecordTerm term of the output
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  def generateValuesInputFormat[T](
      ctx: CodeGeneratorContext,
      name: String,
      records: Seq[String],
      returnType: LogicalType,
      outRecordTerm: String = CodeGenUtils.DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = CodeGenUtils.DEFAULT_OUT_RECORD_WRITER_TERM)
    : GeneratedInput[GenericInputFormat[T]] = {
    val funcName = newName(name)

    ctx.addReusableOutputRecord(returnType, classOf[GenericRow], outRecordTerm,
                                Some(outRecordWriterTerm))

    val funcCode = j"""
      public class $funcName extends ${classOf[GenericInputFormat[_]].getCanonicalName} {

        private int nextIdx = 0;

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public boolean reachedEnd() throws java.io.IOException {
          return nextIdx >= ${records.length};
        }

        @Override
        public Object nextRecord(Object reuse) {
          switch (nextIdx) {
            ${records.zipWithIndex.map { case (r, i) =>
              s"""
                 |case $i:
                 |  $r
                 |break;
                       """.stripMargin
            }.mkString("\n")}
          }
          nextIdx++;
          return $outRecordTerm;
        }
      }
    """.stripMargin

    new GeneratedInput(funcName, funcCode, ctx.references.toArray)
  }

}
