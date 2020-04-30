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
import org.apache.flink.types.{BooleanValue, ByteValue, CharValue, DoubleValue, FloatValue, IntValue, LongValue, ShortValue, StringValue}

import scala.collection._
import scala.collection.generic.CanBuildFrom
import scala.reflect.macros.Context
import scala.util.DynamicVariable

@Internal
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

          case TypeParameter() => TypeParameterDescriptor(id, tpe)

          // type or super type defines type information factory
          case FactoryType(baseType) => analyzeFactoryType(id, tpe, baseType)

          case PrimitiveType(default, wrapper) => PrimitiveDescriptor(id, tpe, default, wrapper)

          case BoxedPrimitiveType(default, wrapper, box, unbox) =>
            BoxedPrimitiveDescriptor(id, tpe, default, wrapper, box, unbox)

          case ArrayType(elemTpe) => analyzeArray(id, tpe, elemTpe)

          case NothingType() => NothingDescriptor(id, tpe)

          case UnitType() => UnitDescriptor(id, tpe)

          case EitherType(leftTpe, rightTpe) => analyzeEither(id, tpe, leftTpe, rightTpe)

          case EnumValueType(enum) => EnumValueDescriptor(id, tpe, enum)

          case TryType(elemTpe) => analyzeTry(id, tpe, elemTpe)

          case OptionType(elemTpe) => analyzeOption(id, tpe, elemTpe)

          case CaseClassType() => analyzeCaseClass(id, tpe)

          case TraversableType(elemTpe) => analyzeTraversable(id, tpe, elemTpe)

          case ValueType() => ValueDescriptor(id, tpe)

          case WritableType() => WritableDescriptor(id, tpe)

          case TraitType() => GenericClassDescriptor(id, tpe)

          case JavaTupleType() => analyzeJavaTuple(id, tpe)

          case JavaType() =>
            // It's a Java Class, let the TypeExtractor deal with it...
            GenericClassDescriptor(id, tpe)

          case _ => analyzePojo(id, tpe)
        }
      }
    }

    private def analyzeFactoryType(
        id: Int,
        tpe: Type,
        baseType: Type): UDTDescriptor = {
      val params: Seq[UDTDescriptor] = baseType match {
        case TypeRef(_, _, args) =>
          args.map(analyze)
        case _ =>
          Seq[UDTDescriptor]()
      }
      FactoryTypeDescriptor(id, tpe, baseType, params)
    }

    private def analyzeArray(
        id: Int,
        tpe: Type,
        elemTpe: Type): UDTDescriptor = analyze(elemTpe) match {
      case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
      case desc => ArrayDescriptor(id, tpe, desc)
    }

    private def analyzeTraversable(
        id: Int,
        tpe: Type,
        elemTpe: Type): UDTDescriptor = analyze(elemTpe) match {
      case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
      case desc => TraversableDescriptor(id, tpe, desc)
    }

    private def analyzeEither(
        id: Int,
        tpe: Type,
        leftTpe: Type,
        rightTpe: Type): UDTDescriptor = analyze(leftTpe) match {
      case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
      case leftDesc => analyze(rightTpe) match {
        case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
        case rightDesc => EitherDescriptor(id, tpe, leftDesc, rightDesc)
      }
    }

    private def analyzeTry(
        id: Int,
        tpe: Type,
        elemTpe: Type): UDTDescriptor = analyze(elemTpe) match {
      case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
      case elemDesc => TryDescriptor(id, tpe, elemDesc)
    }

    private def analyzeOption(
        id: Int,
        tpe: Type,
        elemTpe: Type): UDTDescriptor = analyze(elemTpe) match {
      case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
      case elemDesc => OptionDescriptor(id, tpe, elemDesc)
    }

    private def analyzeJavaTuple(id: Int, tpe: Type): UDTDescriptor = {
      // check how many tuple fields we have and determine type
      val fields = (0 until org.apache.flink.api.java.tuple.Tuple.MAX_ARITY ) flatMap { i =>
        tpe.members find { m => m.name.toString.equals("f" + i)} match {
          case Some(m) => Some(analyze(m.typeSignatureIn(tpe)))

          case _ => None
        }
      }

      JavaTupleDescriptor(id, tpe, fields)
    }


    private def analyzePojo(id: Int, tpe: Type): UDTDescriptor = {
      val immutableFields = tpe.members filter { _.isTerm } map { _.asTerm } filter { _.isVal }
      if (immutableFields.nonEmpty) {
        // We don't support POJOs with immutable fields
        return GenericClassDescriptor(id, tpe)
      }

      val fields = tpe.members
        .filter { _.isTerm }
        .map { _.asTerm }
        .filter { _.isVar }
        .filter { !_.isStatic }
        .filterNot { _.annotations.exists( _.tpe <:< typeOf[scala.transient]) }

      if (fields.isEmpty) {
        c.warning(c.enclosingPosition, s"Type $tpe has no fields that are visible from Scala Type" +
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
        return GenericClassDescriptor(id, tpe)
      }

      // check whether we have a zero-parameter ctor
      val hasZeroCtor = tpe.declarations exists  {
        case m: MethodSymbol
          if m.isConstructor && m.paramss.length == 1 && m.paramss(0).length == 0 => true
        case _ => false
      }

      if (!hasZeroCtor) {
        // We don't support POJOs without zero-parameter ctor
        return GenericClassDescriptor(id, tpe)
      }

      val fieldDescriptors = fields map {
        f =>
          val fieldTpe = f.typeSignatureIn(tpe)
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

    private object ArrayType {
      def unapply(tpe: Type): Option[Type] = tpe match {
        case TypeRef(_, _, elemTpe :: Nil) if tpe <:< typeOf[Array[_]] => Some(elemTpe)
        case _ => None
      }
    }


    private object TraversableType {
      def unapply(tpe: Type): Option[Type] = tpe match {
        case _ if tpe <:< typeOf[BitSet] => Some(typeOf[Int])

        case _ if tpe <:< typeOf[SortedMap[_, _]] =>
          // handled by generic serializer
          None
        case _ if tpe <:< typeOf[SortedSet[_]] =>
          // handled by generic serializer
          None

        case _ if tpe <:< typeOf[TraversableOnce[_]] =>
//          val traversable = tpe.baseClasses
//            .map(tpe.baseType)
//            .find(t => t.erasure =:= typeOf[TraversableOnce[_]].erasure)

          val traversable = tpe.baseType(typeOf[TraversableOnce[_]].typeSymbol)

          traversable match {
            case TypeRef(_, _, elemTpe :: Nil) =>

              import compat._ // this is needed in order to compile in Scala 2.11

              // determine whether we can find an implicit for the CanBuildFrom because
              // TypeInformationGen requires this. This catches the case where a user
              // has a custom class that implements Iterable[], for example.
              val cbfTpe = TypeRef(
                typeOf[CanBuildFrom[_, _, _]],
                typeOf[CanBuildFrom[_, _, _]].typeSymbol,
                tpe :: elemTpe :: tpe :: Nil)

              val cbf = c.inferImplicitValue(cbfTpe, silent = true)

              if (cbf == EmptyTree) {
                None
              } else {
                Some(elemTpe.asSeenFrom(tpe, tpe.typeSymbol))
              }
            case _ => None
          }

        case _ => None
      }
    }

    private object TypeParameter {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.isParameter
    }

    private object CaseClassType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.isCaseClass
    }

    private object NothingType {
      def unapply(tpe: Type): Boolean = tpe =:= typeOf[Nothing]
    }

    private object UnitType {
      def unapply(tpe: Type): Boolean = tpe =:= typeOf[Unit]
    }

    private object EitherType {
      def unapply(tpe: Type): Option[(Type, Type)] = {
        if (tpe <:< typeOf[Either[_, _]]) {
          val either = tpe.baseType(typeOf[Either[_, _]].typeSymbol)
          either match {
            case TypeRef(_, _, leftTpe :: rightTpe :: Nil) =>
              Some(leftTpe, rightTpe)
          }
        } else {
          None
        }
      }
    }

    private object EnumValueType {
      def unapply(tpe: Type): Option[ModuleSymbol] = {
        // somewhat hacky solution based on the 'org.example.MyEnum.Value' FQN
        // convention, compatible with Scala 2.10
        try {
          val m = c.universe.rootMirror
          // get fully-qualified type name, e.g. org.example.MyEnum.Value
          val fqn = tpe.normalize.toString.split('.')
          // get FQN parent
          val owner = m.staticModule(fqn.slice(0, fqn.size - 1).mkString("."))

          val enumerationSymbol = typeOf[scala.Enumeration].typeSymbol
          if (owner.typeSignature.baseClasses.contains(enumerationSymbol)) {
            Some(owner)
          } else {
            None
          }
        } catch {
          case e: Throwable => None
        }
        // TODO: use this once 2.10 is no longer supported
        // tpe is the Enumeration.Value alias, get the owner
        // val owner = tpe.typeSymbol.owner
        // if (owner.isModule &&
        //     owner.typeSignature.baseClasses.contains(typeOf[scala.Enumeration].typeSymbol))
        //   Some(owner.asModule)
        // else
        //   None
      }
    }

    private object TryType {
      def unapply(tpe: Type): Option[Type] = {
        if (tpe <:< typeOf[scala.util.Try[_]]) {
          val option = tpe.baseType(typeOf[scala.util.Try[_]].typeSymbol)
          option match {
            case TypeRef(_, _, elemTpe :: Nil) =>
              Some(elemTpe)
          }
        } else {
          None
        }
      }
    }

    private object OptionType {
      def unapply(tpe: Type): Option[Type] = {
        if (tpe <:< typeOf[Option[_]]) {
          val option = tpe.baseType(typeOf[Option[_]].typeSymbol)
          option match {
            case TypeRef(_, _, elemTpe :: Nil) =>
              Some(elemTpe)
          }
        } else {
          None
        }
      }
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

    private object TraitType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.isTrait
    }

    private object JavaType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.isJava
    }

    private object JavaTupleType {
      def unapply(tpe: Type): Boolean = tpe <:< typeOf[org.apache.flink.api.java.tuple.Tuple]
    }

    private object FactoryType {
      def unapply(tpe: Type): Option[Type] = {
        val definingType = tpe.typeSymbol.asClass.baseClasses find {
          _.annotations.exists(_.tpe =:= typeOf[org.apache.flink.api.common.typeinfo.TypeInfo])
        }
        definingType.map(tpe.baseType)
      }
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

