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

package eu.stratosphere.api.scala.analysis

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.language.experimental.macros
import eu.stratosphere.types.{Key => PactKey}
import eu.stratosphere.types.Record
import eu.stratosphere.types.{Value => PactValue}
import eu.stratosphere.types.StringValue
import eu.stratosphere.api.scala.codegen.Util

abstract class UDT[T] extends Serializable {
  protected def createSerializer(indexMap: Array[Int]): UDTSerializer[T]
  val fieldTypes: Array[Class[_ <: eu.stratosphere.types.Value]]
  val udtIdMap: Map[Int, Int]
  
  def numFields = fieldTypes.length

  def getSelectionIndices(selection: List[Int]) = { 
    selection map { udtIdMap.getOrElse(_, -1) }
  }

  def getKeySet(fields: Seq[Int]): Array[Class[_ <: PactKey]] = {
    fields map { fieldNum => fieldTypes(fieldNum).asInstanceOf[Class[_ <: PactKey]] } toArray
  }

  def getSerializer(indexMap: Array[Int]): UDTSerializer[T] = {
    val ser = createSerializer(indexMap)
    ser
  }

  @transient private var defaultSerializer: UDTSerializer[T] = null

  def getSerializerWithDefaultLayout: UDTSerializer[T] = {
    // This method will be reentrant if T is a recursive type
    if (defaultSerializer == null) {
      defaultSerializer = createSerializer((0 until numFields) toArray)
    }
    defaultSerializer
  }
}

abstract class UDTSerializer[T](val indexMap: Array[Int]) {
  def serialize(item: T, record: Record)
  def deserializeRecyclingOff(record: Record): T
  def deserializeRecyclingOn(record: Record): T
}

trait UDTLowPriorityImplicits {
  implicit def createUDT[T]: UDT[T] = macro Util.createUDTImpl[T]
}

object UDT extends UDTLowPriorityImplicits {

  // UDTs needed by library code

  object NothingUDT extends UDT[Nothing] {
    override val fieldTypes = Array[Class[_ <: PactValue]]()
    override val udtIdMap: Map[Int, Int] = Map()
    override def createSerializer(indexMap: Array[Int]) = throw new UnsupportedOperationException("Cannot create UDTSerializer for type Nothing")
  }

  object StringUDT extends UDT[String] {

    override val fieldTypes = Array[Class[_ <: PactValue]](classOf[StringValue])
    override val udtIdMap: Map[Int, Int] = Map()

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[String](indexMap) {

      private val index = indexMap(0)

      @transient private var pactField = new StringValue()

//      override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
//        case Seq() => List(index)
////        case _     => invalidSelection(selection)
//        case _     => throw new RuntimeException("Invalid selection: " + selection)
//      }

      override def serialize(item: String, record: Record) = {
        if (index >= 0) {
          pactField.setValue(item)
          record.setField(index, pactField)
        }
      }

      override def deserializeRecyclingOff(record: Record): String = {
        if (index >= 0) {
          record.getFieldInto(index, pactField)
          pactField.getValue()
        } else {
          null
        }
      }

      override def deserializeRecyclingOn(record: Record): String = {
        if (index >= 0) {
          record.getFieldInto(index, pactField)
          pactField.getValue()
        } else {
          null
        }
      }

      private def readObject(in: java.io.ObjectInputStream) = {
        in.defaultReadObject()
        pactField = new StringValue()
      }
    }
  }
}

