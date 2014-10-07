/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala.codegen

import scala.collection.GenTraversableOnce
import scala.collection.mutable
import scala.reflect.macros.Context
import scala.util.DynamicVariable

import org.apache.flink.types.BooleanValue
import org.apache.flink.types.ByteValue
import org.apache.flink.types.CharValue
import org.apache.flink.types.DoubleValue
import org.apache.flink.types.FloatValue
import org.apache.flink.types.IntValue
import org.apache.flink.types.StringValue
import org.apache.flink.types.LongValue
import org.apache.flink.types.ShortValue


private[flink] trait TypeAnalyzer[C <: Context] { this: MacroContextHolder[C]
  with TypeDescriptors[C] =>

  import c.universe._

  // This value is controlled by the udtRecycling compiler option
  var enableMutableUDTs = false

  private val mutableTypes = mutable.Set[Type]()

  def getUDTDescriptor(tpe: Type): UDTDescriptor = new UDTAnalyzerInstance().analyze(tpe)

  private def typeArgs(tpe: Type) = tpe match { case TypeRef(_, _, args) => args }

  private class UDTAnalyzerInstance {

    private val cache = new UDTAnalyzerCache()

    def analyze(tpe: Type): UDTDescriptor = {

      cache.getOrElseUpdate(tpe) { id =>
        tpe match {
          case PrimitiveType(default, wrapper) => PrimitiveDescriptor(id, tpe, default, wrapper)
          case BoxedPrimitiveType(default, wrapper, box, unbox) =>
            BoxedPrimitiveDescriptor(id, tpe, default, wrapper, box, unbox)
          case ListType(elemTpe, iter) =>
            analyzeList(id, tpe, elemTpe, iter)
          case CaseClassType() => analyzeCaseClass(id, tpe)
          case ValueType() => ValueDescriptor(id, tpe)
          case WritableType() => WritableDescriptor(id, tpe)
          case JavaType() =>
            // It's a Java Class, let the TypeExtractor deal with it...
            c.warning(c.enclosingPosition, s"Type $tpe is a java class. Will be analyzed by " +
              s"TypeExtractor at runtime.")
            GenericClassDescriptor(id, tpe)
          case _ => analyzePojo(id, tpe)
        }
      }
    }

    private def analyzeList(
        id: Int,
        tpe: Type,
        elemTpe: Type,
        iter: Tree => Tree): UDTDescriptor = analyze(elemTpe) match {
      case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
      case desc => ListDescriptor(id, tpe, iter, desc)
    }

    private def analyzePojo(id: Int, tpe: Type): UDTDescriptor = {
      val immutableFields = tpe.members filter { _.isTerm } map { _.asTerm } filter { _.isVal }
      if (immutableFields.nonEmpty) {
        // We don't support POJOs with immutable fields
        c.warning(
          c.enclosingPosition,
          s"Type $tpe is no POJO, has immutable fields: ${immutableFields.mkString(", ")}.")
        return GenericClassDescriptor(id, tpe)
      }

      val fields = tpe.members
        .filter { _.isTerm }
        .map { _.asTerm }
        .filter { _.isVar }
        .filterNot { _.annotations.exists( _.tpe <:< typeOf[scala.transient]) }

      if (fields.isEmpty) {
        c.warning(c.enclosingPosition, "Type $tpe has no fields that are visible from Scala Type" +
          " analysis. Falling back to Java Type Analysis (TypeExtractor).")
        return GenericClassDescriptor(id, tpe)
      }

      // check whether all fields are either: 1. public, 2. have getter/setter
      val invalidFields = fields filterNot {
        f =>
          f.isPublic ||
            (f.getter != NoSymbol && f.getter.isPublic && f.setter != NoSymbol && f.setter.isPublic)
      }

      if (invalidFields.nonEmpty) {
        c.warning(c.enclosingPosition, s"Type $tpe is no POJO because it has non-public fields '" +
          s"${invalidFields.mkString(", ")}' that don't have public getters/setters.")
        return GenericClassDescriptor(id, tpe)
      }

      // check whether we have a zero-parameter ctor
      val hasZeroCtor = tpe.declarations exists  {
        case m: MethodSymbol
          if m.isConstructor && m.paramss.length == 1 && m.paramss(0).length == 0 => true
        case _ => false
      }

      if (!hasZeroCtor) {
        // We don't support POJOs without zero-paramter ctor
        c.warning(
          c.enclosingPosition,
          s"Class $tpe is no POJO, has no zero-parameters constructor.")
        return GenericClassDescriptor(id, tpe)
      }

      val fieldDescriptors = fields map {
        f =>
          val fieldTpe = f.getter.asMethod.returnType.asSeenFrom(tpe, tpe.typeSymbol)
          FieldDescriptor(f.name.toString.trim, f.getter, f.setter, fieldTpe, analyze(fieldTpe))
      }

      PojoDescriptor(id, tpe, fieldDescriptors.toSeq)
    }

    private def analyzeCaseClass(id: Int, tpe: Type): UDTDescriptor = {

      tpe.baseClasses exists { bc => !(bc == tpe.typeSymbol) && bc.asClass.isCaseClass } match {

        case true =>
          UnsupportedDescriptor(id, tpe, Seq("Case-to-case inheritance is not supported."))

        case false =>

          val ctors = tpe.declarations collect {
            case m: MethodSymbol if m.isPrimaryConstructor => m
          }

          ctors match {
            case c1 :: c2 :: _ =>
              UnsupportedDescriptor(
                id,
                tpe,
                Seq("Multiple constructors found, this is not supported."))
            case ctor :: Nil =>
              val caseFields = ctor.paramss.flatten.map {
                sym =>
                  {
                    val methodSym = tpe.member(sym.name).asMethod
                    val getter = methodSym.getter
                    val setter = methodSym.setter
                    val returnType = methodSym.returnType.asSeenFrom(tpe, tpe.typeSymbol)
                    (getter, setter, returnType)
                  }
              }
              val fields = caseFields map {
                case (fgetter, fsetter, fTpe) =>
                  FieldDescriptor(fgetter.name.toString.trim, fgetter, fsetter, fTpe, analyze(fTpe))
              }
              val mutable = enableMutableUDTs && (fields forall { f => f.setter != NoSymbol })
              if (mutable) {
                mutableTypes.add(tpe)
              }
              fields filter { _.desc.isInstanceOf[UnsupportedDescriptor] } match {
                case errs @ _ :: _ =>
                  val msgs = errs flatMap { f =>
                    (f: @unchecked) match {
                      case FieldDescriptor(
                        fName, _, _, _, UnsupportedDescriptor(_, fTpe, errors)) =>
                        errors map { err => "Field " + fName + ": " + fTpe + " - " + err }
                    }
                  }
                  UnsupportedDescriptor(id, tpe, msgs)

                case Nil => CaseClassDescriptor(id, tpe, mutable, ctor, fields.toSeq)
              }
          }
      }
    }

    private object PrimitiveType {
      def intPrimitive: (Type, Literal, Type) = {
        val (d, w) = primitives(definitions.IntClass)
        (definitions.IntTpe, d, w)
      }

      def unapply(tpe: Type): Option[(Literal, Type)] = primitives.get(tpe.typeSymbol)
    }

    private object BoxedPrimitiveType {
      def unapply(tpe: Type): Option[(Literal, Type, Tree => Tree, Tree => Tree)] =
        boxedPrimitives.get(tpe.typeSymbol)
    }

    private object ListType {

      def unapply(tpe: Type): Option[(Type, Tree => Tree)] = tpe match {

        case ArrayType(elemTpe) =>
          val iter = { source: Tree => 
            Select(source, newTermName("iterator"))
          }
          Some(elemTpe, iter)

        case TraversableType(elemTpe) =>
          val iter = { source: Tree => Select(source, newTermName("toIterator")) }
          Some(elemTpe, iter)

        case _ => None
      }

      private object ArrayType {
        def unapply(tpe: Type): Option[Type] = tpe match {
          case TypeRef(_, _, elemTpe :: Nil) if tpe <:< typeOf[Array[_]] => Some(elemTpe)
          case _ => None
        }
      }

      private object TraversableType {
        def unapply(tpe: Type): Option[Type] = tpe match {
          case _ if tpe <:< typeOf[GenTraversableOnce[_]] =>
            // val abstrElemTpe = genTraversableOnceClass.typeConstructor.typeParams.head.tpe
            // val elemTpe = abstrElemTpe.asSeenFrom(tpe, genTraversableOnceClass)
            // Some(elemTpe)
            // TODO make sure this works as it should
            tpe match {
              case TypeRef(_, _, elemTpe :: Nil) => Some(elemTpe.asSeenFrom(tpe, tpe.typeSymbol))
            }

          case _ => None
        }
      }
    }

    private object CaseClassType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.isCaseClass
    }

    private object ValueType {
      def unapply(tpe: Type): Boolean =
        tpe.typeSymbol.asClass.baseClasses exists {
          s => s.fullName == "org.apache.flink.types.Value"
        }
    }

    private object WritableType {
      def unapply(tpe: Type): Boolean =
        tpe.typeSymbol.asClass.baseClasses exists {
          s => s.fullName == "org.apache.hadoop.io.Writable"
        }
    }

    private object JavaType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.isJava
    }

    private class UDTAnalyzerCache {

      private val caches = new DynamicVariable[Map[Type, RecursiveDescriptor]](Map())
      private val idGen = new Counter

      def newId = idGen.next

      def getOrElseUpdate(tpe: Type)(orElse: Int => UDTDescriptor): UDTDescriptor = {

        val id = idGen.next
        val cache = caches.value

        cache.get(tpe) map { _.copy(id = id) } getOrElse {
          val ref = RecursiveDescriptor(id, tpe, id)
          caches.withValue(cache + (tpe -> ref)) {
            orElse(id)
          }
        }
      }
    }
  }

  lazy val primitives = Map[Symbol, (Literal, Type)](
    definitions.BooleanClass -> (Literal(Constant(false)), typeOf[BooleanValue]),
    definitions.ByteClass -> (Literal(Constant(0: Byte)), typeOf[ByteValue]),
    definitions.CharClass -> (Literal(Constant(0: Char)), typeOf[CharValue]),
    definitions.DoubleClass -> (Literal(Constant(0: Double)), typeOf[DoubleValue]),
    definitions.FloatClass -> (Literal(Constant(0: Float)), typeOf[FloatValue]),
    definitions.IntClass -> (Literal(Constant(0: Int)), typeOf[IntValue]),
    definitions.LongClass -> (Literal(Constant(0: Long)), typeOf[LongValue]),
    definitions.ShortClass -> (Literal(Constant(0: Short)), typeOf[ShortValue]),
    definitions.StringClass -> (Literal(Constant(null: String)), typeOf[StringValue]))

  lazy val boxedPrimitives = {

    def getBoxInfo(prim: Symbol, primName: String, boxName: String) = {
      val (default, wrapper) = primitives(prim)
      val box = { t: Tree => 
        Apply(
          Select(
            Select(Ident(newTermName("scala")), newTermName("Predef")),
            newTermName(primName + "2" + boxName)),
          List(t))
      }
      val unbox = { t: Tree =>
        Apply(
          Select(
            Select(Ident(newTermName("scala")), newTermName("Predef")),
            newTermName(boxName + "2" + primName)),
          List(t))
      }
      (default, wrapper, box, unbox)
    }

    Map(
      typeOf[java.lang.Boolean].typeSymbol ->
        getBoxInfo(definitions.BooleanClass, "boolean", "Boolean"),
      typeOf[java.lang.Byte].typeSymbol -> getBoxInfo(definitions.ByteClass, "byte", "Byte"),
      typeOf[java.lang.Character].typeSymbol ->
        getBoxInfo(definitions.CharClass, "char", "Character"),
      typeOf[java.lang.Double].typeSymbol ->
        getBoxInfo(definitions.DoubleClass, "double", "Double"),
      typeOf[java.lang.Float].typeSymbol -> getBoxInfo(definitions.FloatClass, "float", "Float"),
      typeOf[java.lang.Integer].typeSymbol -> getBoxInfo(definitions.IntClass, "int", "Integer"),
      typeOf[java.lang.Long].typeSymbol -> getBoxInfo(definitions.LongClass, "long", "Long"),
      typeOf[java.lang.Short].typeSymbol -> getBoxInfo(definitions.ShortClass, "short", "Short"))
  }

}

