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

package eu.stratosphere.api.scala.codegen

import scala.reflect.macros.Context
import scala.reflect.classTag
import scala.reflect.ClassTag
import scala.Option.option2Iterable

trait UDTDescriptors[C <: Context] { this: MacroContextHolder[C] => 
  import c.universe._

  abstract sealed class UDTDescriptor {
    val id: Int
    val tpe: Type
    val isPrimitiveProduct: Boolean = false
    
    def canBeKey: Boolean

    def mkRoot: UDTDescriptor = this

    def flatten: Seq[UDTDescriptor]
    def getters: Seq[FieldAccessor] = Seq()

    def select(member: String): Option[UDTDescriptor] = getters find { _.getter.name.toString == member } map { _.desc }
    
    def select(path: List[String]): Seq[Option[UDTDescriptor]] = path match {
      case Nil => Seq(Some(this))
      case head :: tail => getters find { _.getter.name.toString == head } match {
        case None => Seq(None)
        case Some(d : FieldAccessor) => d.desc.select(tail)
      }
    }

    def findById(id: Int): Option[UDTDescriptor] = flatten.find { _.id == id }

    def findByType[T <: UDTDescriptor: ClassTag]: Seq[T] = {
      val clazz = classTag[T].runtimeClass
      flatten filter { item => clazz.isAssignableFrom(item.getClass) } map { _.asInstanceOf[T] }
    }

    def getRecursiveRefs: Seq[UDTDescriptor] = findByType[RecursiveDescriptor] flatMap { rd => findById(rd.refId) } map { _.mkRoot } distinct
  }

  case class UnsupportedDescriptor(id: Int, tpe: Type, errors: Seq[String]) extends UDTDescriptor {
    override def flatten = Seq(this)
    
    def canBeKey = false
  }

  case class PrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapper: Type) extends UDTDescriptor {
    override val isPrimitiveProduct = true
    override def flatten = Seq(this)
    override def canBeKey = wrapper <:< typeOf[eu.stratosphere.types.Key[_]]
  }

  case class BoxedPrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapper: Type, box: Tree => Tree, unbox: Tree => Tree) extends UDTDescriptor {

    override val isPrimitiveProduct = true
    override def flatten = Seq(this)
    override def canBeKey = wrapper <:< typeOf[eu.stratosphere.types.Key[_]]

    override def hashCode() = (id, tpe, default, wrapper, "BoxedPrimitiveDescriptor").hashCode()
    override def equals(that: Any) = that match {
      case BoxedPrimitiveDescriptor(thatId, thatTpe, thatDefault, thatWrapper, _, _) => (id, tpe, default, wrapper).equals(thatId, thatTpe, thatDefault, thatWrapper)
      case _ => false
    }
  }

  case class ListDescriptor(id: Int, tpe: Type, iter: Tree => Tree, elem: UDTDescriptor) extends UDTDescriptor {
    override def canBeKey = false
    override def flatten = this +: elem.flatten

    def getInnermostElem: UDTDescriptor = elem match {
      case list: ListDescriptor => list.getInnermostElem
      case _                    => elem
    }

    override def hashCode() = (id, tpe, elem).hashCode()
    override def equals(that: Any) = that match {
      case that @ ListDescriptor(thatId, thatTpe,  _, thatElem) => (id, tpe, elem).equals((thatId, thatTpe, thatElem))
      case _ => false
    }
  }

  case class BaseClassDescriptor(id: Int, tpe: Type, override val getters: Seq[FieldAccessor], subTypes: Seq[UDTDescriptor]) extends UDTDescriptor {
    override def flatten = this +: ((getters flatMap { _.desc.flatten }) ++ (subTypes flatMap { _.flatten }))
    override def canBeKey = flatten forall { f => f.canBeKey }
    
    override def select(path: List[String]): Seq[Option[UDTDescriptor]] = path match {
      case Nil => getters flatMap { g => g.desc.select(Nil) }
      case head :: tail => getters find { _.getter.name.toString == head } match {
        case None => Seq(None)
        case Some(d : FieldAccessor) => d.desc.select(tail)
      }
    }
  }

  case class CaseClassDescriptor(id: Int, tpe: Type, mutable: Boolean, ctor: Symbol, override val getters: Seq[FieldAccessor]) extends UDTDescriptor {

    override val isPrimitiveProduct = !getters.isEmpty && getters.forall(_.desc.isPrimitiveProduct)

    override def mkRoot = this.copy(getters = getters map { _.copy(isBaseField = false) })
    override def flatten = this +: (getters flatMap { _.desc.flatten })
    
    override def canBeKey = flatten forall { f => f.canBeKey }

    // Hack: ignore the ctorTpe, since two Type instances representing
    // the same ctor function type don't appear to be considered equal. 
    // Equality of the tpe and ctor fields implies equality of ctorTpe anyway.
    override def hashCode = (id, tpe, ctor, getters).hashCode
    override def equals(that: Any) = that match {
      case CaseClassDescriptor(thatId, thatTpe, thatMutable, thatCtor, thatGetters) => (id, tpe, mutable, ctor, getters).equals(thatId, thatTpe, thatMutable, thatCtor, thatGetters)
      case _ => false
    }
    
    override def select(path: List[String]): Seq[Option[UDTDescriptor]] = path match {
      case Nil => getters flatMap { g => g.desc.select(Nil) }
      case head :: tail => getters find { _.getter.name.toString == head } match {
        case None => Seq(None)
        case Some(d : FieldAccessor) => d.desc.select(tail)
      }
    }
  }

  case class FieldAccessor(getter: Symbol, setter: Symbol, tpe: Type, isBaseField: Boolean, desc: UDTDescriptor)

  case class RecursiveDescriptor(id: Int, tpe: Type, refId: Int) extends UDTDescriptor {
    override def flatten = Seq(this)
    override def canBeKey = tpe <:< typeOf[eu.stratosphere.types.Key[_]]
  }
  
  case class PactValueDescriptor(id: Int, tpe: Type) extends UDTDescriptor {
    override val isPrimitiveProduct = true
    override def flatten = Seq(this)
    override def canBeKey = tpe <:< typeOf[eu.stratosphere.types.Key[_]]
  }
}

