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

import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.types.Row

/**
  * A code generator for generating Flink [[GenericInputFormat]]s.
  *
  * @param config configuration that determines runtime behavior
  */
class InputFormatCodeGenerator(
    config: TableConfig)
  extends CodeGenerator(config, false, new RowTypeInfo(), None, None) {


  /**
    * Generates a values input format that can be passed to Java compiler.
    *
    * @param name Class name of the input format. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param records code for creating records
    * @param returnType expected return type
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  def generateValuesInputFormat[T <: Row](
    name: String,
    records: Seq[String],
    returnType: TypeInformation[T])
  : GeneratedInput[GenericInputFormat[T], T] = {
    val funcName = newName(name)

    addReusableOutRecord(returnType)

    val funcCode = j"""
      public class $funcName extends ${classOf[GenericInputFormat[_]].getCanonicalName} {

        private int nextIdx = 0;

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public boolean reachedEnd() throws java.io.IOException {
          return nextIdx >= ${records.length};
        }

        @Override
        public Object nextRecord(Object reuse) throws java.io.IOException {
          switch (nextIdx) {
            ${records.zipWithIndex.map { case (r, i) =>
              s"""
                 |case $i:
                 |try {
                 |  $r
                 |} catch (Exception e) {
                 |  throw new java.io.IOException(e);
                 |}
                 |break;
                       """.stripMargin
            }.mkString("\n")}
          }
          nextIdx++;
          return $outRecordTerm;
        }
      }
    """.stripMargin

    GeneratedInput(funcName, returnType, funcCode)
  }
}
