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

package eu.stratosphere.scala.operators

import java.util.{ Iterator => JIterator }

import language.experimental.macros
import scala.reflect.macros.Context

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector
import eu.stratosphere.api.operators.Operator
import eu.stratosphere.api.record.operators.MapOperator
import eu.stratosphere.types.PactRecord
import eu.stratosphere.types.PactInteger
import eu.stratosphere.api.record.operators.ReduceOperator
import eu.stratosphere.api.record.functions.{ReduceFunction => JReduceFunction}

import eu.stratosphere.scala._
import eu.stratosphere.scala.analysis._
import eu.stratosphere.scala.codegen.{MacroContextHolder, Util}
import eu.stratosphere.scala.functions.{ReduceFunction, ReduceFunctionBase, CombinableGroupReduceFunction, GroupReduceFunction}

class KeyedDataSet[In](val keySelection: List[Int], val input: DataSet[In]) {
  def reduceGroup[Out](fun: Iterator[In] => Out): DataSet[Out] with OneInputHintable[In, Out] = macro ReduceMacros.reduceGroup[In, Out]
  def combinableReduceGroup(fun: Iterator[In] => In): DataSet[In] with OneInputHintable[In, In] = macro ReduceMacros.combinableReduceGroup[In]
  
  def reduce(fun: (In, In) => In): DataSet[In] with OneInputHintable[In, In] = macro ReduceMacros.reduce[In]
  
  def count() : DataSet[(In, Int)] with OneInputHintable[In, (In, Int)] = macro ReduceMacros.count[In]
}

object ReduceMacros {
  
  def groupBy[In: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })
                                                    (keyFun: c.Expr[In => Key]): c.Expr[KeyedDataSet[In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelection = slave.getSelector(keyFun)

    val helper = reify {
    	new KeyedDataSet[In](keySelection.splice, c.prefix.splice)
    }

    return helper
  }

  def reduce[In: c.WeakTypeTag](c: Context { type PrefixType = KeyedDataSet[In] })
                               (fun: c.Expr[(In, In) => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    reduceImpl(c)(c.prefix, fun)
  }

  def globalReduce[In: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(fun: c.Expr[(In, In) => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    reduceImpl(c)(reify { new KeyedDataSet[In](List[Int](), c.prefix.splice) }, fun)
  }

  def reduceGroup[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = KeyedDataSet[In] })
                                                        (fun: c.Expr[Iterator[In] => Out]): c.Expr[DataSet[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    reduceGroupImpl(c)(c.prefix, fun)
  }

  def globalReduceGroup[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(fun: c.Expr[Iterator[In] => Out]): c.Expr[DataSet[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    reduceGroupImpl(c)(reify { new KeyedDataSet[In](List[Int](), c.prefix.splice) }, fun)
  }

  def combinableReduceGroup[In: c.WeakTypeTag](c: Context { type PrefixType = KeyedDataSet[In] })
                                              (fun: c.Expr[Iterator[In] => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    combinableReduceGroupImpl(c)(c.prefix, fun)
  }

  def combinableGlobalReduceGroup[In: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })
                                              (fun: c.Expr[Iterator[In] => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    combinableReduceGroupImpl(c)(reify { new KeyedDataSet[In](List[Int](), c.prefix.splice) }, fun)
  }

  def reduceImpl[In: c.WeakTypeTag](c: Context)
                               (groupedInput: c.Expr[KeyedDataSet[In]], fun: c.Expr[(In, In) => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]

    val stub: c.Expr[ReduceFunctionBase[In, In]] = if (fun.actualType <:< weakTypeOf[ReduceFunction[In]])
      reify { fun.splice.asInstanceOf[ReduceFunctionBase[In, In]] }
    else reify {
      implicit val inputUDT: UDT[In] = c.Expr[UDT[In]](createUdtIn).splice

      new ReduceFunctionBase[In, In] {
        override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {
          reduce(records, out)
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = reduceIterator.reduce(fun.splice)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }
      }

    }
    val contract = reify {
      val helper = groupedInput.splice
      val input = helper.input.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val keySelection = helper.keySelection
      val keySelector = new FieldSelector(generatedStub.inputUDT, keySelection)

      val builder = ReduceOperator.builder(generatedStub).input(input)

      val keyPositions = keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceOperator(builder) with OneInputKeyedScalaContract[In, In] {
        override val key: FieldSelector = keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[In](ret) with OneInputHintable[In, In] {}
    }

    val result = c.Expr[DataSet[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))
    
    return result
  }

  def reduceGroupImpl[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context)
                                                            (groupedInput: c.Expr[KeyedDataSet[In]], fun: c.Expr[Iterator[In] => Out]): c.Expr[DataSet[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val stub: c.Expr[ReduceFunctionBase[In, Out]] = if (fun.actualType <:< weakTypeOf[GroupReduceFunction[In, Out]])
      reify { fun.splice.asInstanceOf[ReduceFunctionBase[In, Out]] }
    else reify {
      implicit val inputUDT: UDT[In] = c.Expr[UDT[In]](createUdtIn).splice
      implicit val outputUDT: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice

      new ReduceFunctionBase[In, Out] {
        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {
          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = fun.splice.apply(reduceIterator)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }
      }
    }
    val contract = reify {
      val helper = groupedInput.splice
      val input = helper.input.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val keySelection = helper.keySelection
      val keySelector = new FieldSelector(generatedStub.inputUDT, keySelection)
      val builder = ReduceOperator.builder(generatedStub).input(input)

      val keyPositions = keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceOperator(builder) with OneInputKeyedScalaContract[In, Out] {
        override val key: FieldSelector = keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[Out](ret) with OneInputHintable[In, Out] {}
    }

    val result = c.Expr[DataSet[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))
    
    return result
  }
  
  def combinableReduceGroupImpl[In: c.WeakTypeTag](c: Context)
                                              (groupedInput: c.Expr[KeyedDataSet[In]], fun: c.Expr[Iterator[In] => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]

    val stub: c.Expr[ReduceFunctionBase[In, In]] = if (fun.actualType <:< weakTypeOf[CombinableGroupReduceFunction[In, In]])
      reify { fun.splice.asInstanceOf[ReduceFunctionBase[In, In]] }
    else reify {
      implicit val inputUDT: UDT[In] = c.Expr[UDT[In]](createUdtIn).splice

      new ReduceFunctionBase[In, In] {
        override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {
          reduce(records, out)
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {
          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = fun.splice.apply(reduceIterator)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }
      }
    }
    val contract = reify {
      val helper = groupedInput.splice
      val input = helper.input.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val keySelection = helper.keySelection
      val keySelector = new FieldSelector(generatedStub.inputUDT, keySelection)
      val builder = ReduceOperator.builder(generatedStub).input(input)

      val keyPositions = keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceOperator(builder) with OneInputKeyedScalaContract[In, In] {
        override val key: FieldSelector = keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[In](ret) with OneInputHintable[In, In] {}
    }

    val result = c.Expr[DataSet[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))
    
    return result
  }

  def count[In: c.WeakTypeTag](c: Context { type PrefixType = KeyedDataSet[In] })() : c.Expr[DataSet[(In, Int)] with OneInputHintable[In, (In, Int)]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[(In, Int)]
    
    val contract = reify {
      val helper: KeyedDataSet[In] = c.prefix.splice
      val keySelection = helper.keySelection

      val generatedStub = new JReduceFunction with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = c.Expr[UDT[(In, Int)]](createUdtOut).splice
        val keySelector = new FieldSelector(inputUDT, keySelection)
        val udf: UDF1[In, (In, Int)] = new UDF1(inputUDT, outputUDT)
        
        private val reduceRecord = new PactRecord()
        private val pactInt = new PactInteger()

        private var countPosition: Int = 0;

        override def open(config: Configuration) = {
          super.open(config)
          this.countPosition = udf.getOutputLength - 1;
        }
        
        override def reduce(records: JIterator[PactRecord], result: Collector[PactRecord]) : Unit = {
          
          var record : PactRecord = null
          var counter: Int = 0
          while (records.hasNext()) {
            record = records.next()
            val count = if (record.getNumFields() <= countPosition || record.isNull(countPosition)) 1 else record.getField(countPosition, pactInt).getValue()
            counter = counter + count
          }
          
          pactInt.setValue(counter)
          record.setField(countPosition, pactInt)
          result.collect(record)
        }
        
        override def combine(records: JIterator[PactRecord], result: Collector[PactRecord]) : Unit = {
          reduce(records, result)
        }

      }
      
      val builder = ReduceOperator.builder(generatedStub).input(helper.input.contract)

      val keyPositions = generatedStub.keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceOperator(builder) with OneInputKeyedScalaContract[In, (In, Int)] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(Annotations.getConstantFieldsExcept(Array[Int]()))
      }
      new DataSet[(In, Int)](ret) with OneInputHintable[In, (In, Int)] {}
    }

    val result = c.Expr[DataSet[(In, Int)] with OneInputHintable[In, (In, Int)]](Block(List(udtIn, udtOut), contract.tree))
    
    return result
  }
}
