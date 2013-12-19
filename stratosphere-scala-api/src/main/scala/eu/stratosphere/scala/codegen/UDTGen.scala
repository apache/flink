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

package eu.stratosphere.scala.codegen

import scala.reflect.macros.Context
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.types.ListValue
import eu.stratosphere.types.Record

trait UDTGen[C <: Context] { this: MacroContextHolder[C] with UDTDescriptors[C] with UDTAnalyzer[C] with TreeGen[C] with SerializerGen[C] with SerializeMethodGen[C] with DeserializeMethodGen[C] with Loggers[C] =>
  import c.universe._

  def mkUdtClass[T: c.WeakTypeTag](): (ClassDef, Tree) = {
    val desc = getUDTDescriptor(weakTypeOf[T])

    val udtName = c.fresh[TypeName]("GeneratedUDTDescriptor")
    val udt = mkClass(udtName, Flag.FINAL, List(weakTypeOf[UDT[T]]), {
      val (ser, createSer) = mkUdtSerializerClass[T](creatorName = "createSerializer")
      val ctor = mkMethod(nme.CONSTRUCTOR.toString(), NoFlags, List(), NoType, {
        Block(List(mkSuperCall(Nil)), mkUnit)
      })

      List(ser, createSer, ctor, mkFieldTypes(desc), mkUDTIdToIndexMap(desc))
    })
    
    val (_, udtTpe) = typeCheck(udt)
    
    (udt, mkCtorCall(udtTpe, Nil))
  }
  
  private def mkFieldTypes(desc: UDTDescriptor): Tree = {

    mkVal("fieldTypes", Flag.OVERRIDE | Flag.FINAL, false, typeOf[Array[Class[_ <: eu.stratosphere.types.Value]]], {

      val fieldTypes = getIndexFields(desc).toList map {
        case PrimitiveDescriptor(_, _, _, wrapper) => Literal(Constant(wrapper))
        case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, _) => Literal(Constant(wrapper))
        case PactValueDescriptor(_, tpe) => Literal(Constant(tpe))
        case ListDescriptor(_, _, _, _) => Literal(Constant(typeOf[ListValue[eu.stratosphere.types.Value]]))
        // Box inner instances of recursive types
        case RecursiveDescriptor(_, _, _) => Literal(Constant(typeOf[Record]))
        case BaseClassDescriptor(_, _, _, _) => throw new RuntimeException("Illegal descriptor for basic record field.")
        case CaseClassDescriptor(_, _, _, _, _) => throw new RuntimeException("Illegal descriptor for basic record field.")
        case UnsupportedDescriptor(_, _, _) => throw new RuntimeException("Illegal descriptor for basic record field.")
      }
      Apply(Select(Select(Ident("scala": TermName), "Array": TermName), "apply": TermName), fieldTypes)
    })
  }
  
  private def mkUDTIdToIndexMap(desc: UDTDescriptor): Tree = {

    mkVal("udtIdMap", Flag.OVERRIDE | Flag.FINAL, false, typeOf[Map[Int, Int]], {

      val fieldIds = getIndexFields(desc).toList map {
        case PrimitiveDescriptor(id, _, _, _) => Literal(Constant(id))
        case BoxedPrimitiveDescriptor(id, _, _, _, _, _) => Literal(Constant(id))
        case ListDescriptor(id, _, _, _) => Literal(Constant(id))
        case RecursiveDescriptor(id, _, _) => Literal(Constant(id))
        case PactValueDescriptor(id, _) => Literal(Constant(id))
        case BaseClassDescriptor(_, _, _, _) => throw new RuntimeException("Illegal descriptor for basic record field.")
        case CaseClassDescriptor(_, _, _, _, _) => throw new RuntimeException("Illegal descriptor for basic record field.")
        case UnsupportedDescriptor(_, _, _) => throw new RuntimeException("Illegal descriptor for basic record field.")
      }
      val fields = fieldIds.zipWithIndex map { case (id, idx) =>
        val idExpr = c.Expr[Int](id)
        val idxExpr = c.Expr[Int](Literal(Constant(idx)))
        reify { (idExpr.splice, idxExpr.splice) }.tree
      }
      Apply(Select(Select(Select(Ident("scala": TermName), "Predef": TermName), "Map": TermName), "apply": TermName), fields)
    })
  }

}