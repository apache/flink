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

import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.runtime.collector.{TableAsyncCollector, TableFunctionCollector}

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
      inputType: InternalType,
      collectedType: InternalType,
      config: TableConfig,
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      collectedTerm: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM,
      converter: String => String = (a) => a)
    : GeneratedCollector = {

    val className = newName(name)
    val input1TypeClass = boxedTypeTermForType(inputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$input1TypeClass $inputTerm, $input2TypeClass $collectedTerm",
      callingParams = s"$inputTerm, $collectedTerm"
    )


    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          super.collect(record);
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) ${converter("record")};
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }

        @Override
        public void close() {
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          super.collect(record);
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) ${converter("record")};
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${ctx.reusePerRecordCode()}
          $bodyCode
        }

        @Override
        public void close() {
        }
      }
    """.stripMargin
    }

    GeneratedCollector(className, funcCode)
  }

  /**
   * Generates a [[TableFunctionCollector]] that can be passed to Java compiler.
   *
   * @param ctx The context of the code generator
   * @param name Class name of the table function collector. Must not be unique but has to be a
   *             valid Java class identifier.
   * @param bodyCode body code for the collector method
   * @param inputType The type information of the element being collected
   * @param collectedType The type information of the element collected by the collector
   * @param inputTerm The term of the input element
   * @param collectedTerm The term of the collected element
   * @return instance of GeneratedCollector
   */
  def generateTableAsyncCollector(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      inputType: InternalType,
      collectedType: InternalType,
      config: TableConfig,
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      collectedTerm: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM)
    : GeneratedCollector = {

    val className = newName(name)
    val input1TypeClass = boxedTypeTermForType(inputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$input1TypeClass $inputTerm, $input2TypeClass $collectedTerm",
      callingParams = s"$inputTerm, $collectedTerm"
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $className extends ${classOf[TableAsyncCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void complete(java.util.Collection records) throws Exception {
          if (records == null || records.size() == 0) {
            getCollector().complete(java.util.Collections.emptyList());
            return;
          }
          try {
            $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
            // TODO: currently, collection should only contain one element
            $input2TypeClass $collectedTerm = ($input2TypeClass) records.iterator().next();
            ${unboxingCodeSplit.callings.mkString("\n")}
            $bodyCode
          } catch (Exception e) {
            getCollector().completeExceptionally(e);
          }
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $className extends ${classOf[TableAsyncCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void complete(java.util.Collection records) throws Exception {
          if (records == null || records.size() == 0) {
            getCollector().complete(java.util.Collections.emptyList());
            return;
          }
          try {
            $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
            // TODO: currently, collection should only contain one element
            $input2TypeClass $collectedTerm = ($input2TypeClass) records.iterator().next();
            ${ctx.reuseFieldCode()}
            ${ctx.reuseInputUnboxingCode()}
            $bodyCode
          } catch (Exception e) {
            getCollector().completeExceptionally(e);
          }
        }
      }
    """.stripMargin
    }

    GeneratedCollector(className, funcCode)
  }

}
