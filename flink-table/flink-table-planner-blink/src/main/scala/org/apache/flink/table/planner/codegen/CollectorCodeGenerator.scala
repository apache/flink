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
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.collector.TableFunctionCollector
import org.apache.flink.table.runtime.generated.GeneratedCollector
import org.apache.flink.table.types.logical.LogicalType

/**
  * A code generator for generating [[org.apache.flink.util.Collector]]s.
  */
object CollectorCodeGenerator {

  /**
   * Generates a [[TableFunctionCollector]] that can be passed to Java compiler.
   *
   * @param ctx The context of the code generator
   * @param name Class name of the table function collector. Must not be unique but has to be a
   *             valid Java class identifier.
   * @param bodyCode body code for the collector method
   * @param inputType The type of the element being collected
   * @param collectedType The type of the element collected by the collector
   * @param inputTerm The term of the input element
   * @param collectedTerm The term of the collected element
   * @return instance of GeneratedCollector
   */
  def generateTableFunctionCollector(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      inputType: LogicalType,
      collectedType: LogicalType,
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectedTerm: String = CodeGenUtils.DEFAULT_INPUT2_TERM,
      converter: String => String = (a) => a): GeneratedCollector[TableFunctionCollector[_]] = {

    val funcName = newName(name)
    val input1TypeClass = boxedTypeTermForType(inputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)

    val funcCode =
      j"""
      public class $funcName extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${className[Configuration]} parameters) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) ${converter("record")};
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${ctx.reusePerRecordCode()}
          $bodyCode
        }

        @Override
        public void close() {
          try {
            ${ctx.reuseCloseCode()}
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    """.stripMargin

    new GeneratedCollector(funcName, funcCode, ctx.references.toArray)
  }

}
