/*
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.api.scala

import eu.stratosphere.api.common.operators.util.FieldSet
import eu.stratosphere.api.common.operators._
import eu.stratosphere.api.common.operators.base.{GroupReduceOperatorBase, DeltaIterationBase, BulkIterationBase, GenericDataSourceBase}
import eu.stratosphere.api.java.record.functions.FunctionAnnotation
import eu.stratosphere.api.java.record.operators.BulkIteration.PartialSolutionPlaceHolder
import eu.stratosphere.api.java.record.operators.DeltaIteration.{WorksetPlaceHolder, SolutionSetPlaceHolder}
import eu.stratosphere.api.java.record.operators.GenericDataSink
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable
import collection.JavaConversions.asScalaIterator

object AnnotationUtil {
  val visited = collection.mutable.Set[Operator[_]]()

  def setAnnotations(sinks: Seq[ScalaSink[_]]): Seq[ScalaSink[_]] = {
    visited.clear()

    sinks foreach setAnnotations

    sinks
  }

  def setAnnotations(sink: ScalaSink[_]):Unit = {
    setAnnotations(sink.sink.getInput)
  }

  def setAnnotations(operator: Operator[_]):Unit = {
    if(operator != null && !visited.contains(operator)){
      visited.add(operator)

      operator match {
        case op: GenericDataSourceBase[_,_] =>
        case op: GenericDataSink =>
          setAnnotations(op.getInput)
        case op: PartialSolutionPlaceHolder =>
        case op: SolutionSetPlaceHolder =>
        case op: WorksetPlaceHolder =>
        case op: DeltaIterationBase[_, _] =>
          updateDualSemanticProperties(op)
          setAnnotations(op.getSolutionSetDelta)
          setAnnotations(op.getNextWorkset)
          setAnnotations(op.getInitialWorkset)
          setAnnotations(op.getInitialSolutionSet)
        case op: DualInputOperator[_, _, _, _] =>
          updateDualSemanticProperties(op)
          setAnnotations(op.getFirstInput)
          setAnnotations(op.getSecondInput)
        case op: BulkIterationBase[_] =>
          updateSingleSemanticProperties(op)
          setAnnotations(op.getInput)
          setAnnotations(op.getNextPartialSolution)
          setAnnotations(op.getTerminationCriterion)
        case op: GroupReduceOperatorBase[_, _, _] =>
          updateCombinable(op)
          setAnnotations(op.getInput)
        case op: SingleInputOperator[_, _, _] =>
          updateSingleSemanticProperties(op)
          setAnnotations(op.getInput)
      }
    }
  }

  def updateCombinable(op: GroupReduceOperatorBase[_, _, _]){
    if(op.isInstanceOf[ScalaOperator[_,_]]) {
      val scalaOp = op.asInstanceOf[ScalaOperator[_, _]]

      val combinableAnnotaion = scalaOp.getUserCodeAnnotation(classOf[Combinable])

      if (combinableAnnotaion != null) {
        op.setCombinable(true)
      }
    }
  }

  def updateDualSemanticProperties(op: DualInputOperator[_, _, _, _]){
    if(op.isInstanceOf[ScalaOperator[_,_]]) {
      val scalaOp = op.asInstanceOf[ScalaOperator[_, _]]
      val properties = op.getSemanticProperties

      // get readSet annotation from stub
      val constantSet1Annotation: FunctionAnnotation.ConstantFieldsFirst = scalaOp.getUserCodeAnnotation(
        classOf[FunctionAnnotation.ConstantFieldsFirst])
      val constantSet2Annotation: FunctionAnnotation.ConstantFieldsSecond = scalaOp.getUserCodeAnnotation(
        classOf[FunctionAnnotation.ConstantFieldsSecond])

      // get readSet annotation from stub
      val notConstantSet1Annotation: FunctionAnnotation.ConstantFieldsFirstExcept = scalaOp.getUserCodeAnnotation(
        classOf[FunctionAnnotation.ConstantFieldsFirstExcept])
      val notConstantSet2Annotation: FunctionAnnotation.ConstantFieldsSecondExcept = scalaOp.getUserCodeAnnotation(
        classOf[FunctionAnnotation.ConstantFieldsSecondExcept])

      if (notConstantSet1Annotation != null && constantSet1Annotation != null) {
        throw new RuntimeException("Either ConstantFieldsFirst or ConstantFieldsFirstExcept can be specified, not both.")
      }

      if (constantSet2Annotation != null && notConstantSet2Annotation != null) {
        throw new RuntimeException("Either ConstantFieldsSecond or ConstantFieldsSecondExcept can be specified, not both.")
      }

      // extract readSets from annotations
      if (notConstantSet1Annotation != null) {
        for (element <- notConstantSet1Annotation.value()) {
          if (properties.getForwardedField1(element) != null) {
            throw new RuntimeException("Field " + element + " cannot be forwarded and non constant at the same time.")
          }
        }

        val fieldSet = new FieldSet(notConstantSet1Annotation.value(): _*)

        for (i <- 0 until scalaOp.getUDF().getOutputLength) {
          if (!fieldSet.contains(i)) {
            properties.addForwardedField1(i, i)
          }
        }
      } else if (constantSet1Annotation != null) {
        for (value <- constantSet1Annotation.value) {
          properties.addForwardedField1(value, value)
        }
      }

      if (notConstantSet2Annotation != null) {
        for (element <- notConstantSet2Annotation.value()) {
          if (properties.getForwardedField2(element) != null) {
            throw new RuntimeException("Field " + element + " cannot be forwarded and non constant at the same time.")
          }
        }

        val fieldSet = new FieldSet(notConstantSet2Annotation.value(): _*)

        for (i <- 0 until scalaOp.getUDF().getOutputLength) {
          if (!fieldSet.contains(i)) {
            properties.addForwardedField2(i, i)
          }
        }
      } else if (constantSet2Annotation != null) {
        for (value <- constantSet2Annotation.value) {
          properties.addForwardedField2(value, value)
        }
      }

      op.setSemanticProperties(properties)
    }
  }

  def updateSingleSemanticProperties(op: SingleInputOperator[_, _, _]) {
    if (op.isInstanceOf[ScalaOperator[_, _]]) {
      val scalaOp = op.asInstanceOf[ScalaOperator[_, _]]
      var properties = op.getSemanticProperties

      if (properties == null) {
        properties = new SingleInputSemanticProperties()
      }

      // get constantSet annotation from stub
      val constantSet: FunctionAnnotation.ConstantFields = scalaOp.getUserCodeAnnotation(classOf[FunctionAnnotation
      .ConstantFields])
      val notConstantSet: FunctionAnnotation.ConstantFieldsExcept = scalaOp.getUserCodeAnnotation(
        classOf[FunctionAnnotation.ConstantFieldsExcept])

      if (notConstantSet != null && constantSet != null) {
        throw new RuntimeException("Either ConstantFields or ConstantFieldsExcept can be specified, not both.")
      }

      // extract notConstantSet from annotation
      if (notConstantSet != null) {
        val nonConstant: FieldSet = new FieldSet(notConstantSet.value: _*)

        for (element <- nonConstant.iterator()) {
          if (properties.getForwardedField(element) != null) {
            throw new RuntimeException("Field " + element + " is non constant and at the same time forwarded. This " +
              "cannot happen.")
          }
        }

        for (i <- 0 until scalaOp.getUDF().getOutputLength) {
          if (!nonConstant.contains(i)) {
            properties.addForwardedField(i, i)
          }
        }

      } else if (constantSet != null) {
        // extract constantSet from annotation
        for (value <- constantSet.value) {
          properties.addForwardedField(value, value)
        }
      }

      op.setSemanticProperties(properties)
    }
  }
}
