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

package eu.stratosphere.scala.codegen

import scala.collection.GenTraversableOnce
import scala.collection.mutable
import scala.reflect.macros.Context
import scala.util.DynamicVariable
import eu.stratosphere.types.PactBoolean
import eu.stratosphere.types.PactByte
import eu.stratosphere.types.PactCharacter
import eu.stratosphere.types.PactDouble
import eu.stratosphere.types.PactFloat
import eu.stratosphere.types.PactInteger
import eu.stratosphere.types.PactString
import eu.stratosphere.types.PactLong
import eu.stratosphere.types.PactShort
import scala.Option.option2Iterable

trait UDTAnalyzer[C <: Context] { this: MacroContextHolder[C] with UDTDescriptors[C] with Loggers[C] =>
  import c.universe._

  // This value is controlled by the udtRecycling compiler option
  var enableMutableUDTs = false

  private val mutableTypes = mutable.Set[Type]()

  def getUDTDescriptor(tpe: Type): UDTDescriptor = (new UDTAnalyzerInstance with Logger).analyze(tpe)

  private def normTpe(tpe: Type): Type = {
    // TODO Figure out what the heck this does
    //      val currentThis = ThisType(localTyper.context.enclClass.owner)
    //      currentThis.baseClasses.foldLeft(tpe map { _.dealias }) { (tpe, base) => tpe.substThis(base, currentThis) }
    tpe
  }

  private def typeArgs(tpe: Type) = tpe match { case TypeRef(_, _, args) => args }

  private class UDTAnalyzerInstance { this: Logger =>

    private val cache = new UDTAnalyzerCache()

    def analyze(tpe: Type): UDTDescriptor = {

      val normed = normTpe(tpe)

      cache.getOrElseUpdate(normed) { id =>
        normed match {
          case PrimitiveType(default, wrapper) => PrimitiveDescriptor(id, normed, default, wrapper)
          case BoxedPrimitiveType(default, wrapper, box, unbox) => BoxedPrimitiveDescriptor(id, normed, default, wrapper, box, unbox)
          case ListType(elemTpe, iter) => analyzeList(id, normed, elemTpe, iter)
          case CaseClassType() => analyzeCaseClass(id, normed)
          case BaseClassType() => analyzeClassHierarchy(id, normed)
          case PactValueType() => PactValueDescriptor(id, normed)
          case _ => UnsupportedDescriptor(id, normed, Seq("Unsupported type " + normed))
        }
      }
    }

    private def analyzeList(id: Int, tpe: Type, elemTpe: Type, iter: Tree => Tree): UDTDescriptor = analyze(elemTpe) match {
      case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
      case desc => ListDescriptor(id, tpe, iter, desc)
    }

    private def analyzeClassHierarchy(id: Int, tpe: Type): UDTDescriptor = {

      val tagField = {
        val (intTpe, intDefault, intWrapper) = PrimitiveType.intPrimitive
        FieldAccessor(NoSymbol, NoSymbol, NullaryMethodType(intTpe), true, PrimitiveDescriptor(cache.newId, intTpe, intDefault, intWrapper))
      }
      
//      c.info(c.enclosingPosition, "KNOWN SUBCLASSES: " + tpe.typeSymbol.asClass.knownDirectSubclasses.toList, true)

      val subTypes = tpe.typeSymbol.asClass.knownDirectSubclasses.toList flatMap { d =>

        val dTpe = // verbosely[Type] { dTpe => d.tpe + " <: " + tpe + " instantiated as " + dTpe + " (" + (if (dTpe <:< tpe) "Valid" else "Invalid") + " subtype)" } 
          {
            val tArgs = (tpe.typeSymbol.asClass.typeParams, typeArgs(tpe)).zipped.toMap
            val dArgs = d.asClass.typeParams map { dp =>
              val tArg = tArgs.keySet.find { tp => dp == tp.typeSignature.asSeenFrom(d.typeSignature, tpe.typeSymbol).typeSymbol }
              tArg map { tArgs(_) } getOrElse dp.typeSignature
            }

            normTpe(appliedType(d.asType.toType, dArgs))
          }
//      c.info(c.enclosingPosition, "dTpe: " + dTpe, true)

        if (dTpe <:< tpe)
          Some(analyze(dTpe))
        else
          None
      }

//      c.info(c.enclosingPosition, c.enclosingRun.units.size +  " SUBTYPES: " + subTypes, true)

      val errors = subTypes flatMap { _.findByType[UnsupportedDescriptor] }

//      c.info(c.enclosingPosition, "ERROS: " + errors, true)

      errors match {
        case _ :: _ => UnsupportedDescriptor(id, tpe, errors flatMap { case UnsupportedDescriptor(_, subType, errs) => errs map { err => "Subtype " + subType + " - " + err } })
        case Nil if subTypes.isEmpty => UnsupportedDescriptor(id, tpe, Seq("No instantiable subtypes found for base class"))
        case Nil => {

          val (tParams, tArgs) = tpe.typeSymbol.asClass.typeParams.zip(typeArgs(tpe)).unzip
          val baseMembers = tpe.members filter { f => f.isMethod } filter { f => f.asMethod.isSetter } map {
            f => (f, f.asMethod.setter, normTpe(f.asMethod.returnType))
          }

          val subMembers = subTypes map {
            case BaseClassDescriptor(_, _, getters, _) => getters
            case CaseClassDescriptor(_, _, _, _, getters) => getters
            case _ => Seq()
          }

          val baseFields = baseMembers flatMap {
            case (bGetter, bSetter, bTpe) => {
              val accessors = subMembers map {
                _ find { sf =>
                  sf.getter.name == bGetter.name && sf.tpe.termSymbol.asMethod.returnType <:< bTpe.termSymbol.asMethod.returnType
                }
              }
              accessors.forall { _.isDefined } match {
                case true => Some(FieldAccessor(bGetter, bSetter, bTpe, true, analyze(bTpe.termSymbol.asMethod.returnType)))
                case false => None
              }
            }
          }

          def wireBaseFields(desc: UDTDescriptor): UDTDescriptor = {

            def updateField(field: FieldAccessor) = {
              baseFields find { bf => bf.getter.name == field.getter.name } match {
                case Some(FieldAccessor(_, _, _, _, desc)) => field.copy(isBaseField = true, desc = desc)
                case None => field
              }
            }

            desc match {
              case desc @ BaseClassDescriptor(_, _, getters, subTypes) => desc.copy(getters = getters map updateField, subTypes = subTypes map wireBaseFields)
              case desc @ CaseClassDescriptor(_, _, _, _, getters) => desc.copy(getters = getters map updateField)
              case _ => desc
            }
          }

          //Debug.report("BaseClass " + tpe + " has shared fields: " + (baseFields.map { m => m.sym.name + ": " + m.tpe }))
          BaseClassDescriptor(id, tpe, tagField +: (baseFields.toSeq), subTypes map wireBaseFields)
        }
      }

    }

    private def analyzeCaseClass(id: Int, tpe: Type): UDTDescriptor = {

      tpe.baseClasses exists { bc => !(bc == tpe.typeSymbol) && bc.asClass.isCaseClass } match {

        case true => UnsupportedDescriptor(id, tpe, Seq("Case-to-case inheritance is not supported."))

        case false => {

          val ctors = tpe.declarations collect {
            case m: MethodSymbol if m.isPrimaryConstructor => m
          }

          ctors match {
            case c1 :: c2 :: _ => UnsupportedDescriptor(id, tpe, Seq("Multiple constructors found, this is not supported."))
            case c :: Nil => {
              val caseFields = c.paramss.flatten.map {
                sym =>
                  {
                    val methodSym = tpe.member(sym.name).asMethod
                    (methodSym.getter, methodSym.setter, methodSym.returnType.asSeenFrom(tpe, tpe.typeSymbol))
                  }
              }
              val fields = caseFields map {
                case (fgetter, fsetter, fTpe) => FieldAccessor(fgetter, fsetter, fTpe, false, analyze(fTpe))
              }
              val mutable = maybeVerbosely[Boolean](m => m && mutableTypes.add(tpe))(_ => "Detected recyclable type: " + tpe) {
                enableMutableUDTs && (fields forall { f => f.setter != NoSymbol })
              }
              fields filter { _.desc.isInstanceOf[UnsupportedDescriptor] } match {
                case errs @ _ :: _ => {
                  val msgs = errs flatMap { f =>
                    (f: @unchecked) match {
                      case FieldAccessor(fgetter, _, _, _, UnsupportedDescriptor(_, fTpe, errors)) => errors map { err => "Field " + fgetter.name + ": " + fTpe + " - " + err }
                    }
                  }
                  UnsupportedDescriptor(id, tpe, msgs)
                }
                case Nil => CaseClassDescriptor(id, tpe, mutable, c, fields.toSeq)
              }
            }
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

      def unapply(tpe: Type): Option[(Literal, Type, Tree => Tree, Tree => Tree)] = boxedPrimitives.get(tpe.typeSymbol)
    }

    private object ListType {

      def unapply(tpe: Type): Option[(Type, Tree => Tree)] = tpe match {

        case ArrayType(elemTpe) => {
          val iter = { source: Tree => 
            Select(source, "iterator": TermName)
          }
          Some(elemTpe, iter)
        }

        case TraversableType(elemTpe) => {
          val iter = { source: Tree => Select(source, "toIterator": TermName) }
          Some(elemTpe, iter)
        }

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
          case _ if tpe <:< typeOf[GenTraversableOnce[_]] => {
            //              val abstrElemTpe = genTraversableOnceClass.typeConstructor.typeParams.head.tpe
            //              val elemTpe = abstrElemTpe.asSeenFrom(tpe, genTraversableOnceClass)
            //              Some(elemTpe)
            // TODO make sure this shit works as it should
            tpe match {
              case TypeRef(_, _, elemTpe :: Nil) => Some(elemTpe.asSeenFrom(tpe, tpe.typeSymbol))
            }
          }
          case _ => None
        }
      }
    }

    private object CaseClassType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.isCaseClass
    }

    private object BaseClassType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.isAbstractClass && tpe.typeSymbol.asClass.isSealed
    }
    
    private object PactValueType {
      def unapply(tpe: Type): Boolean = tpe.typeSymbol.asClass.baseClasses exists { s => s.fullName == "eu.stratosphere.types.Value" }
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
    definitions.BooleanClass -> (Literal(Constant(false)), typeOf[PactBoolean]),
    definitions.ByteClass -> (Literal(Constant(0: Byte)), typeOf[PactByte]),
    definitions.CharClass -> (Literal(Constant(0: Char)), typeOf[PactCharacter]),
    definitions.DoubleClass -> (Literal(Constant(0: Double)), typeOf[PactDouble]),
    definitions.FloatClass -> (Literal(Constant(0: Float)), typeOf[PactFloat]),
    definitions.IntClass -> (Literal(Constant(0: Int)), typeOf[PactInteger]),
    definitions.LongClass -> (Literal(Constant(0: Long)), typeOf[PactLong]),
    definitions.ShortClass -> (Literal(Constant(0: Short)), typeOf[PactShort]),
    definitions.StringClass -> (Literal(Constant(null: String)), typeOf[PactString]))

  lazy val boxedPrimitives = {

    def getBoxInfo(prim: Symbol, primName: String, boxName: String) = {
      val (default, wrapper) = primitives(prim)
      val box = { t: Tree => 
        Apply(Select(Select(Ident(newTermName("scala")), newTermName("Predef")), newTermName(primName + "2" + boxName)), List(t))
      }
      val unbox = { t: Tree =>
        Apply(Select(Select(Ident(newTermName("scala")), newTermName("Predef")), newTermName(boxName + "2" + primName)), List(t))
      }
      (default, wrapper, box, unbox)
    }

    Map(
      typeOf[java.lang.Boolean].typeSymbol -> getBoxInfo(definitions.BooleanClass, "boolean", "Boolean"),
      typeOf[java.lang.Byte].typeSymbol -> getBoxInfo(definitions.ByteClass, "byte", "Byte"),
      typeOf[java.lang.Character].typeSymbol -> getBoxInfo(definitions.CharClass, "char", "Character"),
      typeOf[java.lang.Double].typeSymbol -> getBoxInfo(definitions.DoubleClass, "double", "Double"),
      typeOf[java.lang.Float].typeSymbol -> getBoxInfo(definitions.FloatClass, "float", "Float"),
      typeOf[java.lang.Integer].typeSymbol -> getBoxInfo(definitions.IntClass, "int", "Integer"),
      typeOf[java.lang.Long].typeSymbol -> getBoxInfo(definitions.LongClass, "long", "Long"),
      typeOf[java.lang.Short].typeSymbol -> getBoxInfo(definitions.ShortClass, "short", "Short"))
  }

}

