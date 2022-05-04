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
package org.apache.flink.api.scala.codegen

import org.apache.flink.annotation.Internal

import scala.collection.Map
import scala.language.postfixOps
import scala.reflect.macros.whitebox.Context

// These are only used internally while analyzing Scala types in TypeAnalyzer and TypeInformationGen

@Internal
private[flink] trait TypeDescriptors[C <: Context] { this: MacroContextHolder[C] =>
  import c.universe._

  sealed abstract class UDTDescriptor {
    val id: Int
    val tpe: Type
  }

  case class GenericClassDescriptor(id: Int, tpe: Type) extends UDTDescriptor

  case class UnsupportedDescriptor(id: Int, tpe: Type, errors: Seq[String]) extends UDTDescriptor

  case class TypeParameterDescriptor(id: Int, tpe: Type) extends UDTDescriptor

  case class PrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapper: Type)
    extends UDTDescriptor

  case class NothingDescriptor(id: Int, tpe: Type) extends UDTDescriptor

  case class UnitDescriptor(id: Int, tpe: Type) extends UDTDescriptor

  case class EitherDescriptor(id: Int, tpe: Type, left: UDTDescriptor, right: UDTDescriptor)
    extends UDTDescriptor

  case class EnumValueDescriptor(id: Int, tpe: Type, enum: ModuleSymbol) extends UDTDescriptor

  case class TryDescriptor(id: Int, tpe: Type, elem: UDTDescriptor) extends UDTDescriptor

  case class FactoryTypeDescriptor(id: Int, tpe: Type, baseType: Type, params: Seq[UDTDescriptor])
    extends UDTDescriptor

  case class OptionDescriptor(id: Int, tpe: Type, elem: UDTDescriptor) extends UDTDescriptor

  case class BoxedPrimitiveDescriptor(
      id: Int,
      tpe: Type,
      default: Literal,
      wrapper: Type,
      box: Tree => Tree,
      unbox: Tree => Tree)
    extends UDTDescriptor {

    override def hashCode() = (id, tpe, default, wrapper, "BoxedPrimitiveDescriptor").hashCode()

    override def equals(that: Any) = that match {
      case BoxedPrimitiveDescriptor(thatId, thatTpe, thatDefault, thatWrapper, _, _) =>
        (id, tpe, default, wrapper).equals(thatId, thatTpe, thatDefault, thatWrapper)
      case _ => false
    }
  }

  case class ArrayDescriptor(id: Int, tpe: Type, elem: UDTDescriptor) extends UDTDescriptor {

    override def hashCode() = (id, tpe, elem).hashCode()

    override def equals(that: Any) = that match {
      case that @ ArrayDescriptor(thatId, thatTpe, thatElem) =>
        (id, tpe, elem).equals((thatId, thatTpe, thatElem))
      case _ => false
    }
  }

  case class TraversableDescriptor(id: Int, tpe: Type, elem: UDTDescriptor) extends UDTDescriptor {

//    def getInnermostElem: UDTDescriptor = elem match {
//      case list: TraversableDescriptor => list.getInnermostElem
//      case _                    => elem
//    }

    override def hashCode() = (id, tpe, elem).hashCode()

    override def equals(that: Any) = that match {
      case that @ TraversableDescriptor(thatId, thatTpe, thatElem) =>
        (id, tpe, elem).equals((thatId, thatTpe, thatElem))
      case _ => false
    }
  }

  case class PojoDescriptor(id: Int, tpe: Type, getters: Seq[FieldDescriptor])
    extends UDTDescriptor {

    // Hack: ignore the ctorTpe, since two Type instances representing
    // the same ctor function type don't appear to be considered equal.
    // Equality of the tpe and ctor fields implies equality of ctorTpe anyway.
    override def hashCode = (id, tpe, getters).hashCode

    override def equals(that: Any) = that match {
      case PojoDescriptor(thatId, thatTpe, thatGetters) =>
        (id, tpe, getters).equals(thatId, thatTpe, thatGetters)
      case _ => false
    }

  }

  case class CaseClassDescriptor(
      id: Int,
      tpe: Type,
      mutable: Boolean,
      ctor: Symbol,
      getters: Seq[FieldDescriptor])
    extends UDTDescriptor {

    // Hack: ignore the ctorTpe, since two Type instances representing
    // the same ctor function type don't appear to be considered equal.
    // Equality of the tpe and ctor fields implies equality of ctorTpe anyway.
    override def hashCode = (id, tpe, ctor, getters).hashCode

    override def equals(that: Any) = that match {
      case CaseClassDescriptor(thatId, thatTpe, thatMutable, thatCtor, thatGetters) =>
        (id, tpe, mutable, ctor, getters).equals(
          thatId,
          thatTpe,
          thatMutable,
          thatCtor,
          thatGetters)
      case _ => false
    }

  }

  case class FieldDescriptor(
      name: String,
      getter: Symbol,
      setter: Symbol,
      tpe: Type,
      desc: UDTDescriptor)

  case class RecursiveDescriptor(id: Int, tpe: Type, refId: Int) extends UDTDescriptor

  case class ValueDescriptor(id: Int, tpe: Type) extends UDTDescriptor

  case class WritableDescriptor(id: Int, tpe: Type) extends UDTDescriptor

  case class JavaTupleDescriptor(id: Int, tpe: Type, fields: Seq[UDTDescriptor])
    extends UDTDescriptor {

    // Hack: ignore the ctorTpe, since two Type instances representing
    // the same ctor function type don't appear to be considered equal.
    // Equality of the tpe and ctor fields implies equality of ctorTpe anyway.
    override def hashCode = (id, tpe, fields).hashCode

    override def equals(that: Any) = that match {
      case JavaTupleDescriptor(thatId, thatTpe, thatFields) =>
        (id, tpe, fields).equals(thatId, thatTpe, thatFields)
      case _ => false
    }

  }
}
