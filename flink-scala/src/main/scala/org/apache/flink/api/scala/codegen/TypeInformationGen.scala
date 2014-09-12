/**
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

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.{ScalaTupleSerializer, ScalaTupleTypeInfo}
import org.apache.flink.types.{Value, TypeInformation}
import org.apache.hadoop.io.Writable

import scala.reflect.macros.Context

private[flink] trait TypeInformationGen[C <: Context] {
  this: MacroContextHolder[C]
  with TypeDescriptors[C]
  with TypeAnalyzer[C]
  with TreeGen[C] =>

  import c.universe._

  // This is for external calling by TypeUtils.createTypeInfo
  def mkTypeInfo[T: c.WeakTypeTag]: c.Expr[TypeInformation[T]] = {
    val desc = getUDTDescriptor(weakTypeOf[T])
    val result: c.Expr[TypeInformation[T]] = mkTypeInfo(desc)(c.WeakTypeTag(desc.tpe))
    result
  }

  // We have this for internal use so that we can use it to recursively generate a tree of
  // TypeInformation from a tree of UDTDescriptor
  def mkTypeInfo[T: c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = desc match {
    case cc@CaseClassDescriptor(_, tpe, _, _, _) =>
      mkTupleTypeInfo(cc)(c.WeakTypeTag(tpe).asInstanceOf[c.WeakTypeTag[Product]])
        .asInstanceOf[c.Expr[TypeInformation[T]]]
    case p : PrimitiveDescriptor => mkPrimitiveTypeInfo(p.tpe)
    case p : BoxedPrimitiveDescriptor => mkPrimitiveTypeInfo(p.tpe)
    case l : ListDescriptor if l.tpe <:< typeOf[Array[_]] => mkListTypeInfo(l)
    case v : ValueDescriptor =>
      mkValueTypeInfo(v)(c.WeakTypeTag(v.tpe).asInstanceOf[c.WeakTypeTag[Value]])
        .asInstanceOf[c.Expr[TypeInformation[T]]]
    case d : WritableDescriptor =>
      mkWritableTypeInfo(d)(c.WeakTypeTag(d.tpe).asInstanceOf[c.WeakTypeTag[Writable]])
        .asInstanceOf[c.Expr[TypeInformation[T]]]
    case d => mkGenericTypeInfo(d)
  }

  def mkTupleTypeInfo[T <: Product : c.WeakTypeTag](
      desc: CaseClassDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val fields = desc.getters.toList map { field =>
      mkTypeInfo(field.desc)(c.WeakTypeTag(field.tpe)).tree
    }
    val fieldsExpr = c.Expr[Seq[TypeInformation[_]]](mkList(fields))
    val instance = mkCreateTupleInstance[T](desc)(c.WeakTypeTag(desc.tpe))
    reify {
      new ScalaTupleTypeInfo[T](tpeClazz.splice, fieldsExpr.splice) {
        override def createSerializer: TypeSerializer[T] = {
          val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
          for (i <- 0 until getArity) {
            fieldSerializers(i) = types(i).createSerializer
          }

          new ScalaTupleSerializer[T](tupleType, fieldSerializers) {
            override def createInstance(fields: Array[AnyRef]): T = {
              instance.splice
            }
          }
        }
      }
    }
  }

  def mkListTypeInfo[T: c.WeakTypeTag](desc: ListDescriptor): c.Expr[TypeInformation[T]] = {
    val arrayClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val elementClazz = c.Expr[Class[T]](Literal(Constant(desc.elem.tpe)))
    val elementTypeInfo = mkTypeInfo(desc.elem)
    desc.elem match {
      // special case for string, which in scala is a primitive, but not in java
      case p: PrimitiveDescriptor if p.tpe <:< typeOf[String] =>
        reify {
          BasicArrayTypeInfo.getInfoFor(arrayClazz.splice)
        }
      case p: PrimitiveDescriptor =>
        reify {
          PrimitiveArrayTypeInfo.getInfoFor(arrayClazz.splice)
        }
      case bp: BoxedPrimitiveDescriptor =>
        reify {
          BasicArrayTypeInfo.getInfoFor(arrayClazz.splice)
        }
      case _ =>
        reify {
          ObjectArrayTypeInfo.getInfoFor(
            arrayClazz.splice,
            elementTypeInfo.splice).asInstanceOf[TypeInformation[T]]
        }
    }
  }

  def mkValueTypeInfo[T <: Value : c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    reify {
      new ValueTypeInfo[T](tpeClazz.splice)
    }
  }

  def mkWritableTypeInfo[T <: Writable : c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    reify {
      new WritableTypeInfo[T](tpeClazz.splice)
    }
  }

  def mkGenericTypeInfo[T: c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    reify {
      TypeExtractor.createTypeInfo(tpeClazz.splice).asInstanceOf[TypeInformation[T]]
    }
  }

  def mkPrimitiveTypeInfo[T: c.WeakTypeTag](tpe: Type): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(tpe)))
    reify {
      BasicTypeInfo.getInfoFor(tpeClazz.splice)
    }
  }

  def mkCreateTupleInstance[T: c.WeakTypeTag](desc: CaseClassDescriptor): c.Expr[T] = {
    val fields = desc.getters.zipWithIndex.map { case (field, i) =>
      val call = mkCall(Ident(newTermName("fields")), "apply")(List(Literal(Constant(i))))
      mkAsInstanceOf(call)(c.WeakTypeTag(field.tpe))
    }
    val result = Apply(Select(New(TypeTree(desc.tpe)), nme.CONSTRUCTOR), fields.toList)
    c.Expr[T](result)
  }

//    def mkCaseClassTypeInfo[T: c.WeakTypeTag](desc: CaseClassDescriptor): c.Expr[TypeInformation[T]] = {
//      val tpeClazz = c.Expr[Class[_]](Literal(Constant(desc.tpe)))
//      val caseFields = mkCaseFields(desc)
//      reify {
//        new ScalaTupleTypeInfo[T] {
//          def createSerializer: TypeSerializer[T] = {
//            null
//          }
//
//          val fields: Map[String, TypeInformation[_]] = caseFields.splice
//          val clazz = tpeClazz.splice
//        }
//      }
//    }
//
//  private def mkCaseFields(desc: UDTDescriptor): c.Expr[Map[String, TypeInformation[_]]] = {
//    val fields = getFields("_root_", desc).toList map { case (fieldName, fieldDesc) =>
//      val nameTree = c.Expr(Literal(Constant(fieldName)))
//      val fieldTypeInfo = mkTypeInfo(fieldDesc)(c.WeakTypeTag(fieldDesc.tpe))
//      reify { (nameTree.splice, fieldTypeInfo.splice) }.tree
//    }
//
//    c.Expr(mkMap(fields))
//  }
//
//  protected def getFields(name: String, desc: UDTDescriptor): Seq[(String, UDTDescriptor)] = desc match {
//    // Flatten product types
//    case CaseClassDescriptor(_, _, _, _, getters) =>
//      getters filterNot { _.isBaseField } flatMap { f => getFields(name + "." + f.getter.name, f.desc) }
//    case _ => Seq((name, desc))
//  }
}