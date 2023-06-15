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
package org.apache.flink.table.planner.plan.fusion

import org.apache.flink.streaming.api.operators.{Input, InputSelection, StreamOperatorParameters}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{OUT_ELEMENT, STREAM_RECORD}
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator.Context
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.runtime.operators.fusion.{FusionStreamOperatorBase, OperatorFusionCodegenFactory}
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputSpec
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler

import java.util

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object FusionCodegenUtil {

  def generateFusionOperator(
      outputGenerator: OpFusionCodegenSpecGenerator,
      inputSpecs: util.List[MultipleInputSpec]): (OperatorFusionCodegenFactory[RowData], Long) = {
    // Must initialize operator managedMemoryFraction before produce-consume call, codegen need it
    val (opSpecGenerators, totalManagedMemory) = setupOpSpecGenerator(outputGenerator)

    val fusionCtx = new CodeGeneratorContext(
      outputGenerator.getOpFusionCodegenSpec.getOperatorCtx.tableConfig,
      outputGenerator.getOpFusionCodegenSpec.getOperatorCtx.classLoader)

    // generate process code
    outputGenerator.processProduce(fusionCtx)
    // generate endInput code
    outputGenerator.endInputProduce(fusionCtx)

    var iter = opSpecGenerators.iterator
    while (iter.hasNext) {
      val op = iter.next
      // init code
      op.addReusableInitCode(fusionCtx)
      // open code from sink to leaf
      op.addReusableOpenCode(fusionCtx)
    }

    iter = opSpecGenerators.descendingIterator
    while (iter.hasNext) {
      val op = iter.next
      // close code from leaf to sink
      op.addReusableCloseCode(fusionCtx)
    }

    fusionCtx.addReusableMember(
      s"private final $STREAM_RECORD $OUT_ELEMENT = new $STREAM_RECORD(null);")
    fusionCtx.addReusableMember(
      s"private final ${className[InputSelectionHandler]} inputSelectionHandler;")

    val inputSpecRefs = fusionCtx.addReusableObject(inputSpecs, "inputSpecRefs")
    fusionCtx.addReusableInitStatement(
      s"this.inputSelectionHandler = new ${className[InputSelectionHandler]}($inputSpecRefs);")

    val operatorName = newName("FusionStreamOperator")
    val operatorCode =
      s"""
      public final class $operatorName extends ${className[FusionStreamOperatorBase]} {

        ${fusionCtx.reuseMemberCode()}

        public $operatorName(
          Object[] references,
          ${className[StreamOperatorParameters[_]]} parameters) throws Exception {
          super(parameters, ${inputSpecs.size()});
          ${fusionCtx.reuseInitCode()}
        }

        @Override
        public void open() throws Exception {
          super.open();
          ${fusionCtx.reuseOpenCode()}
        }

        @Override
        public ${className[util.List[Input[_]]]} getInputs() {
          return ${className[util.Arrays]}.asList(${fusionCtx.reuseFusionProcessCode()});
        }

        @Override
        public void endInput(int inputId) throws Exception {
          inputSelectionHandler.endInput(inputId);
          ${fusionCtx.reuseFusionEndInputCode("inputId")}
        }
        
        @Override
        public ${className[InputSelection]} nextSelection() {
          return inputSelectionHandler.getInputSelection();
        }

        @Override
        public void finish() throws Exception {
            ${fusionCtx.reuseFinishCode()}
            super.finish();
        }

        @Override
        public void close() throws Exception {
           super.close();
           ${fusionCtx.reuseCloseCode()}
        }

        ${fusionCtx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    (
      new OperatorFusionCodegenFactory(
        new GeneratedOperator(
          operatorName,
          operatorCode,
          fusionCtx.references.toArray,
          fusionCtx.tableConfig)),
      totalManagedMemory)
  }

  private def setupOpSpecGenerator(outputGenerator: OpFusionCodegenSpecGenerator)
      : (util.ArrayDeque[OpFusionCodegenSpecGenerator], Long) = {
    val opSpecGenerators = new util.ArrayDeque[OpFusionCodegenSpecGenerator]
    getAllOpSpecGenerator(outputGenerator, opSpecGenerators)

    var totalManagedMemory: Long = 0
    opSpecGenerators.forEach(op => totalManagedMemory += op.getManagedMemory)

    // setup the operator generator needed context before call produce-consume
    opSpecGenerators.forEach(
      op => {
        val context = new Context {
          override def getManagedMemoryFraction: Double = if (totalManagedMemory != 0) {
            op.getManagedMemory * 1.0 / totalManagedMemory
          } else {
            0
          }
        }

        op.setup(context)
      })

    (opSpecGenerators, totalManagedMemory)
  }

  private def getAllOpSpecGenerator(
      outputOp: OpFusionCodegenSpecGenerator,
      operators: util.ArrayDeque[OpFusionCodegenSpecGenerator]): Unit = {
    operators.add(outputOp)
    // visit all input operator from output to leaf recursively
    outputOp.getInputs.foreach(op => getAllOpSpecGenerator(op, operators))
  }
}
