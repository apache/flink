/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.scala.analysis

import scala.collection.mutable

abstract class UDF[R] extends Serializable {

  val outputUDT: UDT[R]
  val outputFields = FieldSet.newOutputSet(outputUDT)

  def getOutputSerializer = outputUDT.getSerializer(outputFields.toSerializerIndexArray)

  def getOutputLength = {
    val indexes = outputFields.toIndexSet
    if (indexes.isEmpty) {
      0 
    } else {
      indexes.max + 1
    }
  }

  def allocateOutputGlobalIndexes(startPos: Int): Int = {

    outputFields.setGlobalized()

    outputFields.map(_.globalPos).foldLeft(startPos) {
      case (i, gPos @ GlobalPos.Unknown()) => gPos.setIndex(i); i + 1
      case (i, _)                          => i + 1
    }
    startPos
  }

  def assignOutputGlobalIndexes(sameAs: FieldSet[Field]): Unit = {

    outputFields.setGlobalized()

    outputFields.foreach {
      case OutputField(localPos, globalPos) => globalPos.setReference(sameAs(localPos).globalPos)
    }
  }

  def setOutputGlobalIndexes(startPos: Int, sameAs: Option[FieldSet[Field]]): Int = sameAs match {
    case None         => allocateOutputGlobalIndexes(startPos)
    case Some(sameAs) => assignOutputGlobalIndexes(sameAs); startPos
  }

  def attachOutputsToInputs(inputFields: FieldSet[InputField]): Unit = {

    inputFields.setGlobalized()

    inputFields.foreach {
      case InputField(localPos, globalPos) => globalPos.setReference(outputFields(localPos).globalPos)
    }
  }

  protected def markFieldCopied(inputGlobalPos: GlobalPos, outputLocalPos: Int): Unit = {
    val outputField = outputFields(outputLocalPos)
    outputField.globalPos.setReference(inputGlobalPos)
    outputField.isUsed = false
  }
}

class UDF0[R](val outputUDT: UDT[R]) extends UDF[R]

class UDF1[T, R](val inputUDT: UDT[T], val outputUDT: UDT[R]) extends UDF[R] {

  val inputFields = FieldSet.newInputSet(inputUDT)
  val forwardSet = mutable.Set[GlobalPos]()
  val discardSet = mutable.Set[GlobalPos]()

  def getInputDeserializer = inputUDT.getSerializer(inputFields.toSerializerIndexArray)
  def getForwardIndexArray = forwardSet.map(_.getValue).toArray
  def getDiscardIndexArray = discardSet.map(_.getValue).toArray

  override def getOutputLength = {
    val forwardMax = if (forwardSet.isEmpty) -1 else forwardSet.map(_.getValue).max
    math.max(super.getOutputLength, forwardMax + 1)
  }

  def markInputFieldUnread(localPos: Int): Unit = {
    inputFields(localPos).isUsed = false
  }

  def markFieldCopied(inputLocalPos: Int, outputLocalPos: Int): Unit = {
    val inputGlobalPos = inputFields(inputLocalPos).globalPos
    forwardSet.add(inputGlobalPos)
    markFieldCopied(inputGlobalPos, outputLocalPos)
  }
}

class UDF2[T1, T2, R](val leftInputUDT: UDT[T1], val rightInputUDT: UDT[T2], val outputUDT: UDT[R]) extends UDF[R] {

  val leftInputFields = FieldSet.newInputSet(leftInputUDT)
  val leftForwardSet = mutable.Set[GlobalPos]()
  val leftDiscardSet = mutable.Set[GlobalPos]()

  val rightInputFields = FieldSet.newInputSet(rightInputUDT)
  val rightForwardSet = mutable.Set[GlobalPos]()
  val rightDiscardSet = mutable.Set[GlobalPos]()

  def getLeftInputDeserializer = leftInputUDT.getSerializer(leftInputFields.toSerializerIndexArray)
  def getLeftForwardIndexArray = leftForwardSet.map(_.getValue).toArray
  def getLeftDiscardIndexArray = leftDiscardSet.map(_.getValue).toArray

  def getRightInputDeserializer = rightInputUDT.getSerializer(rightInputFields.toSerializerIndexArray)
  def getRightForwardIndexArray = rightForwardSet.map(_.getValue).toArray
  def getRightDiscardIndexArray = rightDiscardSet.map(_.getValue).toArray

  override def getOutputLength = {
    val leftForwardMax = if (leftForwardSet.isEmpty) -1 else leftForwardSet.map(_.getValue).max
    val rightForwardMax = if (rightForwardSet.isEmpty) -1 else rightForwardSet.map(_.getValue).max
    math.max(super.getOutputLength, math.max(leftForwardMax, rightForwardMax) + 1)
  }

  private def getInputField(localPos: Either[Int, Int]): InputField = localPos match {
    case Left(pos)  => leftInputFields(pos)
    case Right(pos) => rightInputFields(pos)
  }

  def markInputFieldUnread(localPos: Either[Int, Int]): Unit = {
    localPos.fold(leftInputFields(_), rightInputFields(_)).isUsed = false
  }

  def markFieldCopied(inputLocalPos: Either[Int, Int], outputLocalPos: Int): Unit = {
    val (fields, forwardSet) = inputLocalPos.fold(_ => (leftInputFields, leftForwardSet), _ => (rightInputFields, rightForwardSet))
    val inputGlobalPos = fields(inputLocalPos.merge).globalPos
    forwardSet.add(inputGlobalPos)
    markFieldCopied(inputGlobalPos, outputLocalPos)
  }
}

