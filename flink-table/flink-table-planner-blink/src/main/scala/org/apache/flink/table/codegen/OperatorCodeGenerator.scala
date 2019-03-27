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

import org.apache.flink.streaming.api.operators.{OneInputStreamOperator, StreamOperator}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.`type`.InternalType
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.generated.GeneratedOperator
import org.apache.flink.table.util.Logging

/**
  * A code generator for generating Flink [[StreamOperator]]s.
  */
object OperatorCodeGenerator extends Logging {

  val ELEMENT = "element"
  val OUT_ELEMENT = "outElement"

  val STREAM_RECORD: String = classOf[StreamRecord[_]].getCanonicalName

  def addReuseOutElement(ctx: CodeGeneratorContext): Unit = {
    ctx.addReusableMember(s"private final $STREAM_RECORD $OUT_ELEMENT = new $STREAM_RECORD(null);")
  }

  def generateOneInputStreamOperator[IN <: Any, OUT <: Any](
      ctx: CodeGeneratorContext,
      name: String,
      processCode: String,
      endInputCode: String,
      inputType: InternalType,
      config: TableConfig,
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      lazyInputUnboxingCode: Boolean = false,
      converter: String => String = a => a): GeneratedOperator[OneInputStreamOperator[IN, OUT]] = {
    addReuseOutElement(ctx)
    val operatorName = newName(name)
    val abstractBaseClass = ctx.getOperatorBaseClass
    val baseClass = classOf[OneInputStreamOperator[IN, OUT]]
    val inputTypeTerm = boxedTypeTermForType(inputType)
    val operatorCode =
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName} {

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $operatorName(Object[] references) throws Exception {
          this.references = references;
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open() throws Exception {
          super.open();
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void processElement($STREAM_RECORD $ELEMENT) throws Exception {
          $inputTypeTerm $inputTerm = ($inputTypeTerm) ${converter(s"$ELEMENT.getValue()")};
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${if (lazyInputUnboxingCode) "" else ctx.reuseInputUnboxingCode()}
          $processCode
        }

        // TODO @Override
        public void endInput() throws Exception {
          ${
            if (endInputCode.nonEmpty) {
              s"""
                 |${ctx.reuseLocalVariableCode()}
                 |$endInputCode
                 """.stripMargin
            } else {
              ""
            }
          }
          ${ctx.reuseEndInputCode()}
        }

        @Override
        public void close() throws Exception {
           super.close();
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    LOG.debug(s"Compiling OneInputStreamOperator Code:\n$name")
    new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray)
  }

  private def generateInputTerm(inputTypeTerm: String): String = {
    s"($inputTypeTerm) $ELEMENT.getValue();"
  }

  def generateCollect(emit: String): String =
    s"$DEFAULT_OPERATOR_COLLECTOR_TERM.collect($OUT_ELEMENT.replace($emit));"
}
