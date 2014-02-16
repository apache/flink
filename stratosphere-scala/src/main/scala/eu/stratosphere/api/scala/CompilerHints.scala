/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.api.scala

import language.experimental.macros
import scala.util.DynamicVariable
import eu.stratosphere.api.scala.analysis._
import eu.stratosphere.api.scala.operators.Annotations
import eu.stratosphere.api.common.operators.util.{ FieldSet => PactFieldSet }
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.scala.codegen.MacroContextHolder
import scala.reflect.macros.Context

case class KeyCardinality(key: FieldSelector, isUnique: Boolean, distinctCount: Option[Long], avgNumRecords: Option[Float]) {

  private class RefreshableFieldSet extends PactFieldSet {
    def refresh(indexes: Set[Int]) = {
      this.collection.clear()
      for (index <- indexes)
        this.add(index)
    }
  }

  @transient private var pactFieldSets = collection.mutable.Map[Operator with ScalaOperator[_], RefreshableFieldSet]()

  def getPactFieldSet(contract: Operator with ScalaOperator[_]): PactFieldSet = {

    if (pactFieldSets == null)
      pactFieldSets = collection.mutable.Map[Operator with ScalaOperator[_], RefreshableFieldSet]()

    val keyCopy = key.copy
    contract.getUDF.attachOutputsToInputs(keyCopy.inputFields)
    val keySet = keyCopy.selectedFields.toIndexSet

    val fieldSet = pactFieldSets.getOrElseUpdate(contract, new RefreshableFieldSet())
    fieldSet.refresh(keySet)
    fieldSet
  }
}

trait OutputHintable[Out] { this: DataSet[Out] =>
  def getContract = contract
  
  private var _cardinalities: List[KeyCardinality] = List[KeyCardinality]()
  
  def addCardinality(card: KeyCardinality) { _cardinalities = card :: _cardinalities }

  def degreeOfParallelism = contract.getDegreeOfParallelism()
  def degreeOfParallelism_=(value: Int) = contract.setDegreeOfParallelism(value)
  def degreeOfParallelism(value: Int): this.type = { contract.setDegreeOfParallelism(value); this }
    
  def outputSize = contract.getCompilerHints().getOutputSize()
  def outputSize_=(value: Long) = contract.getCompilerHints().setOutputSize(value)
  def outputSize(value: Long): this.type = { contract.getCompilerHints().setOutputSize(value); this }
  
  def outputCardinality = contract.getCompilerHints().getOutputCardinality()
  def outputCardinality_=(value: Long) = contract.getCompilerHints().setOutputCardinality(value)
  def outputCardinality(value: Long): this.type = { contract.getCompilerHints().setOutputCardinality(value); this }
  
  def avgBytesPerRecord = contract.getCompilerHints().getAvgBytesPerOutputRecord()
  def avgBytesPerRecord_=(value: Float) = contract.getCompilerHints().setAvgBytesPerOutputRecord(value)
  def avgBytesPerRecord(value: Float): this.type = { contract.getCompilerHints().setAvgBytesPerOutputRecord(value); this }

  def filterFactor = contract.getCompilerHints().getFilterFactor()
  def filterFactor_=(value: Float) = contract.getCompilerHints().setFilterFactor(value)
  def filterFactor(value: Float): this.type = { contract.getCompilerHints().setFilterFactor(value); this }

  def uniqueKey[Key](fields: Out => Key) = macro OutputHintableMacros.uniqueKey[Out, Key]

  def applyHints(contract: Operator with ScalaOperator[_]): Unit = {
    val hints = contract.getCompilerHints

    if (hints.getUniqueFields != null)
      hints.getUniqueFields.clear()

    _cardinalities.foreach { card =>

      val fieldSet = card.getPactFieldSet(contract)

      if (card.isUnique) {
        hints.addUniqueField(fieldSet)
      }
    }
  }
}

object OutputHintableMacros {
  
  def uniqueKey[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, true, None, None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def uniqueKeyWithDistinctCount[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], distinctCount: c.Expr[Long]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, true, Some(distinctCount.splice), None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinality[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, None, None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinalityWithDistinctCount[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], distinctCount: c.Expr[Long]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, Some(distinctCount.splice), None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinalityWithAvgNumRecords[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], avgNumRecords: c.Expr[Float]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, None, Some(avgNumRecords.splice))
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinalityWithAll[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], distinctCount: c.Expr[Long], avgNumRecords: c.Expr[Float]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, Some(distinctCount.splice), Some(avgNumRecords.splice))
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
}

trait InputHintable[In, Out] { this: DataSet[Out] =>
  def markUnread: Int => Unit
  def markCopied: (Int, Int) => Unit
  
  def getInputUDT: UDT[In]
  def getOutputUDT: UDT[Out]

  def neglects[Fields](fields: In => Fields): Unit = macro InputHintableMacros.neglects[In, Out, Fields]
  def observes[Fields](fields: In => Fields): Unit = macro InputHintableMacros.observes[In, Out, Fields]
  def preserves[Fields](from: In => Fields, to: Out => Fields) = macro InputHintableMacros.preserves[In, Out, Fields]
}

object InputHintable {

  private val enabled = new DynamicVariable[Boolean](true)

  def withEnabled[T](isEnabled: Boolean)(thunk: => T): T = enabled.withValue(isEnabled) { thunk }
  
}

object InputHintableMacros {
  
  def neglects[In: c.WeakTypeTag, Out: c.WeakTypeTag, Fields: c.WeakTypeTag](c: Context { type PrefixType = InputHintable[In, Out] })(fields: c.Expr[In => Fields]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedFieldSelector = slave.getSelector(fields)

    val result = reify {
      val fieldSelection = generatedFieldSelector.splice
      val fieldSelector = new FieldSelector(c.prefix.splice.getInputUDT, fieldSelection)
      val unreadFields = fieldSelector.selectedFields.map(_.localPos).toSet
      unreadFields.foreach(c.prefix.splice.markUnread(_))
    }
    return result
  }
  
  def observes[In: c.WeakTypeTag, Out: c.WeakTypeTag, Fields: c.WeakTypeTag](c: Context { type PrefixType = InputHintable[In, Out] })(fields: c.Expr[In => Fields]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedFieldSelector = slave.getSelector(fields)

    val result = reify {
      val fieldSelection = generatedFieldSelector.splice
      val fieldSelector = new FieldSelector(c.prefix.splice.getInputUDT, fieldSelection)
      val fieldSet = fieldSelector.selectedFields.map(_.localPos).toSet
      val unreadFields = fieldSelector.inputFields.map(_.localPos).toSet.diff(fieldSet)
      unreadFields.foreach(c.prefix.splice.markUnread(_))
    }
    return result
  }
  
  def preserves[In: c.WeakTypeTag, Out: c.WeakTypeTag, Fields: c.WeakTypeTag](c: Context { type PrefixType = InputHintable[In, Out] })(from: c.Expr[In => Fields], to: c.Expr[Out => Fields]): c.Expr[Unit] = {
    import c.universe._

     val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedFromFieldSelector = slave.getSelector(from)
    val generatedToFieldSelector = slave.getSelector(to)

    val result = reify {
      val fromSelection = generatedFromFieldSelector.splice
      val fromSelector = new FieldSelector(c.prefix.splice.getInputUDT, fromSelection)
      val toSelection = generatedToFieldSelector.splice
      val toSelector = new FieldSelector(c.prefix.splice.getOutputUDT, toSelection)
      val pairs = fromSelector.selectedFields.map(_.localPos).zip(toSelector.selectedFields.map(_.localPos))
      pairs.foreach(c.prefix.splice.markCopied.tupled)
    }
    return result
  }
}

trait OneInputHintable[In, Out] extends InputHintable[In, Out] with OutputHintable[Out] { this: DataSet[Out] =>
	override def markUnread = contract.getUDF.asInstanceOf[UDF1[In, Out]].markInputFieldUnread _ 
	override def markCopied = contract.getUDF.asInstanceOf[UDF1[In, Out]].markFieldCopied _ 
	
	override def getInputUDT = contract.getUDF.asInstanceOf[UDF1[In, Out]].inputUDT
	override def getOutputUDT = contract.getUDF.asInstanceOf[UDF1[In, Out]].outputUDT
}

trait TwoInputHintable[LeftIn, RightIn, Out] extends OutputHintable[Out] { this: DataSet[Out] =>
  val left = new DataSet[Out](contract) with OneInputHintable[LeftIn, Out] {
	override def markUnread = { pos: Int => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markInputFieldUnread(Left(pos))}
	override def markCopied = { (from: Int, to: Int) => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markFieldCopied(Left(from), to)} 
	override def getInputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].leftInputUDT
	override def getOutputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].outputUDT
  }
  
  val right = new DataSet[Out](contract) with OneInputHintable[RightIn, Out] {
	override def markUnread = { pos: Int => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markInputFieldUnread(Right(pos))}
	override def markCopied = { (from: Int, to: Int) => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markFieldCopied(Right(from), to)} 
	override def getInputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].rightInputUDT
	override def getOutputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].outputUDT
  }
}
