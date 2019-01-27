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
package org.apache.flink.table.codegen.operator

import org.apache.flink.streaming.api.operators.{OneInputStreamOperator, StreamOperator, TwoInputStreamOperator}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext.DEFAULT_OPERATOR_COLLECTOR_TERM
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen._
import org.apache.flink.table.runtime.WithReferences
import org.apache.flink.table.util.Logging

/**
  * A code generator for generating Flink [[StreamOperator]]s.
  */
object OperatorCodeGenerator extends Logging {

  val ELEMENT = "element"
  val OUT_ELEMENT = "outElement"

  val STREAM_RECORD = "org.apache.flink.streaming.runtime.streamrecord.StreamRecord"

  val SELECTION = "org.apache.flink.streaming.api.operators.TwoInputSelection"

  val ANY: String =
    "org.apache.flink.streaming.api.operators.TwoInputSelection.ANY"

  val FIRST: String =
    "org.apache.flink.streaming.api.operators.TwoInputSelection.FIRST"

  val SECOND: String =
    "org.apache.flink.streaming.api.operators.TwoInputSelection.SECOND"

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
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      splitFunc: GeneratedSplittableExpression =
        GeneratedSplittableExpression.UNSPLIT_EXPRESSION,
      filterSplitFunc: GeneratedSplittableExpression =
        GeneratedSplittableExpression.UNSPLIT_EXPRESSION,
      // TODO inputUnboxCode is in processCode if lazyInputUnboxingCode is true,
      // it shall be pull out
      lazyInputUnboxingCode: Boolean = false,
      converter: String => String = a => a): GeneratedOperator = {
    addReuseOutElement(ctx)
    val operatorName = newName(name)
    val abstractBaseClass = ctx.getOperatorBaseClass
    val baseClass = classOf[OneInputStreamOperator[IN, OUT]]
    val inputTypeTerm = boxedTypeTermForType(inputType)
    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $inputTerm",
      callingParams = inputTerm
    )
    val shallSplit = unboxingCodeSplit.isSplit || splitFunc.isSplit || filterSplitFunc.isSplit
    val operatorCode = if (shallSplit) {
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName}, ${classOf[WithReferences].getCanonicalName} {

        private final Object[] references;
        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

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
          ${
            if (lazyInputUnboxingCode) {
              ""
            } else {
              unboxingCodeSplit.callings.mkString("\n")
            }
          }
          $processCode
        }

        ${
          if (!lazyInputUnboxingCode) {
            unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
              case (define, body) =>
                s"""
                   |$define throws Exception {
                   |  ${ctx.reusePerRecordCode()}
                   |  $body
                   | }
                   """.stripMargin
              } mkString "\n"
          } else {
            ""
          }
        }

        ${
          if (splitFunc.isSplit) {
            splitFunc.definitions.zip(splitFunc.bodies) map {
              case (define, body) =>
                s"""
                   |$define throws Exception {
                   |  ${ctx.reusePerRecordCode()}
                   |  $body
                   |}
                   """.stripMargin
            } mkString "\n"
          } else {
            ""
          }
        }

        ${
          if (filterSplitFunc.isSplit) {
            filterSplitFunc.definitions.zip(filterSplitFunc.bodies) map {
              case (define, body) =>
                s"""
                   |$define throws Exception {
                   |  ${ctx.reusePerRecordCode()}
                   |  $body
                   |}
                     """.stripMargin
            } mkString "\n"
          } else {
            ""
          }
        }

        @Override
        public void endInput() throws Exception {
          $endInputCode
          ${ctx.reuseEndInputCode()}
        }

        @Override
        public void close() throws Exception {
           super.close();
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }""".stripMargin
    } else {
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName}, ${classOf[WithReferences].getCanonicalName} {

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
          ${ctx.reuseFieldCode()}
          ${
            if (lazyInputUnboxingCode) {
              ""
            } else {
              ctx.reuseInputUnboxingCode()
            }
          }
          $processCode
        }

        @Override
        public void endInput() throws Exception {
          ${
            if (endInputCode.nonEmpty) {
              s"""
                 |${ctx.reuseFieldCode()}
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
    }

    LOG.debug(s"Compiling OneInputStreamOperator Code:\n$name")
    GeneratedOperator(operatorName, operatorCode)
  }

  def generateTwoInputStreamOperator[IN1 <: Any, IN2 <: Any, OUT <: Any](
      ctx: CodeGeneratorContext,
      name: String,
      firstInputCode: String,
      processCode1: String,
      endInputCode1: String,
      processCode2: String,
      endInputCode2: String,
      input1Type: InternalType,
      input2Type: InternalType,
      input1Term: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      input2Term: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM,
      useTimeCollect: Boolean = false): GeneratedOperator = {
    addReuseOutElement(ctx)
    val operatorName = newName(name)
    val abstractBaseClass = ctx.getOperatorBaseClass
    val baseClass = classOf[TwoInputStreamOperator[IN1, IN2, OUT]]
    val inputTypeTerm1 = boxedTypeTermForType(input1Type)
    val inputTypeTerm2 = boxedTypeTermForType(input2Type)

    val operatorCode =
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName}, ${classOf[WithReferences].getCanonicalName} {

        public static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger("$operatorName");

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
        public $SELECTION firstInputSelection(){
          $firstInputCode
        }

        @Override
        public $SELECTION processElement1($STREAM_RECORD $ELEMENT)
         throws Exception {
          ${ctx.reuseFieldCode()}
          $inputTypeTerm1 $input1Term = ${generatorInputTerm(inputTypeTerm1)}
          $processCode1
        }

        @Override
        public void endInput1() throws Exception {
          $endInputCode1
        }

        @Override
        public $SELECTION processElement2($STREAM_RECORD $ELEMENT)
         throws Exception {
          ${ctx.reuseFieldCode()}
          $inputTypeTerm2 $input2Term = ${generatorInputTerm(inputTypeTerm2)}
          $processCode2
        }

        @Override
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
    GeneratedOperator(operatorName, operatorCode)
  }

  private def generatorInputTerm(inputTypeTerm: String): String = {
    s"($inputTypeTerm) $ELEMENT.getValue();"
  }

  def generatorCollect(emit: String): String =
    s"$DEFAULT_OPERATOR_COLLECTOR_TERM.collect($OUT_ELEMENT.replace($emit));"
}
