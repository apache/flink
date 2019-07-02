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

import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.operators.{OneInputStreamOperator, Output, StreamOperator, TwoInputStreamOperator}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks.StreamTask
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.generated.GeneratedOperator
import org.apache.flink.table.types.logical.LogicalType
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
      inputType: LogicalType,
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

        public $operatorName(
            Object[] references,
            ${classOf[StreamTask[_, _]].getCanonicalName} task,
            ${classOf[StreamConfig].getCanonicalName} config,
            ${classOf[Output[_]].getCanonicalName} output) throws Exception {
          this.references = references;
          ${ctx.reuseInitCode()}
          this.setup(task, config, output);
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
           // TODO remove it after introduce endInput in runtime.
           endInput();
           super.close();
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    LOG.debug(s"Compiling OneInputStreamOperator Code:\n$name")
    new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray)
  }

  def generateTwoInputStreamOperator[IN1 <: Any, IN2 <: Any, OUT <: Any](
      ctx: CodeGeneratorContext,
      name: String,
      processCode1: String,
      endInputCode1: String,
      processCode2: String,
      endInputCode2: String,
      input1Type: LogicalType,
      input2Type: LogicalType,
      input1Term: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      input2Term: String = CodeGenUtils.DEFAULT_INPUT2_TERM,
      useTimeCollect: Boolean = false)
    : GeneratedOperator[TwoInputStreamOperator[IN1, IN2, OUT]] = {
    addReuseOutElement(ctx)
    val operatorName = newName(name)
    val abstractBaseClass = ctx.getOperatorBaseClass
    val baseClass = classOf[TwoInputStreamOperator[IN1, IN2, OUT]]
    val inputTypeTerm1 = boxedTypeTermForType(input1Type)
    val inputTypeTerm2 = boxedTypeTermForType(input2Type)

    val operatorCode =
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName} {

        public static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger("$operatorName");

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $operatorName(
            Object[] references,
            ${classOf[StreamTask[_, _]].getCanonicalName} task,
            ${classOf[StreamConfig].getCanonicalName} config,
            ${classOf[Output[_]].getCanonicalName} output) throws Exception {
          this.references = references;
          ${ctx.reuseInitCode()}
          this.setup(task, config, output);
        }

        @Override
        public void open() throws Exception {
          super.open();
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void processElement1($STREAM_RECORD $ELEMENT)
         throws Exception {
          ${ctx.reuseLocalVariableCode()}
          $inputTypeTerm1 $input1Term = ${generateInputTerm(inputTypeTerm1)}
          $processCode1
        }

        public void endInput1() throws Exception {
          $endInputCode1
        }

        @Override
        public void processElement2($STREAM_RECORD $ELEMENT)
         throws Exception {
          ${ctx.reuseLocalVariableCode()}
          $inputTypeTerm2 $input2Term = ${generateInputTerm(inputTypeTerm2)}
          $processCode2
        }

        public void endInput2() throws Exception {
          $endInputCode2
        }

        @Override
        public void close() throws Exception {
          super.close();
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    LOG.debug(s"Compiling TwoInputStreamOperator Code:\n$name")
    new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray)
  }

  private def generateInputTerm(inputTypeTerm: String): String = {
    s"($inputTypeTerm) $ELEMENT.getValue();"
  }

  def generateCollect(emit: String): String =
    s"$DEFAULT_OPERATOR_COLLECTOR_TERM.collect($OUT_ELEMENT.replace($emit));"
}
