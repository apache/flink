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

import scala.reflect.macros.Context

trait DeserializeMethodGen[C <: Context] { this: MacroContextHolder[C] with UDTDescriptors[C] with TreeGen[C] with SerializerGen[C] with Loggers[C] =>
  import c.universe._

  protected def mkDeserialize(desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

//    val rootRecyclingOn = mkMethod("deserializeRecyclingOn", Flag.OVERRIDE | Flag.FINAL, List(("record", typeOf[eu.stratosphere.pact.common.`type`.PactRecord])), desc.tpe, {
    val rootRecyclingOn = mkMethod("deserializeRecyclingOn", Flag.FINAL, List(("record", typeOf[eu.stratosphere.pact.common.`type`.PactRecord])), desc.tpe, {
      val env = GenEnvironment(listImpls, "flat" + desc.id, false, true, true, true)
      mkSingle(genDeserialize(desc, Ident("record"), env, Map()))
    })

//    val rootRecyclingOff = mkMethod("deserializeRecyclingOff", Flag.OVERRIDE | Flag.FINAL, List(("record", typeOf[eu.stratosphere.pact.common.`type`.PactRecord])), desc.tpe, {
    val rootRecyclingOff = mkMethod("deserializeRecyclingOff", Flag.FINAL, List(("record", typeOf[eu.stratosphere.pact.common.`type`.PactRecord])), desc.tpe, {
      val env = GenEnvironment(listImpls, "flat" + desc.id, false, false, true, true)
      mkSingle(genDeserialize(desc, Ident("record"), env, Map()))
    })

    val aux = desc.getRecursiveRefs map { desc =>
      mkMethod("deserialize" + desc.id, Flag.PRIVATE | Flag.FINAL, List(("record", typeOf[eu.stratosphere.pact.common.`type`.PactRecord])), desc.tpe, {
        val env = GenEnvironment(listImpls, "boxed" + desc.id, true, false, false, true)
        mkSingle(genDeserialize(desc, Ident("record"), env, Map()))
      })
    }

    rootRecyclingOn +: rootRecyclingOff +: aux.toList
  }

  private def genDeserialize(desc: UDTDescriptor, source: Tree, env: GenEnvironment, scope: Map[Int, (String, Type)]): Seq[Tree] = desc match {

    case PrimitiveDescriptor(id, _, default, _) => {
      val chk = env.mkChkIdx(id)
      val des = env.mkGetFieldInto(id, source)
      val get = env.mkGetValue(id)

      Seq(mkIf(chk, Block(List(des), get), default))
    }

    case BoxedPrimitiveDescriptor(id, tpe, _, _, box, _) => {
      val des = env.mkGetFieldInto(id, source)
      val chk = mkAnd(env.mkChkIdx(id), des)
      val get = box(env.mkGetValue(id))

      Seq(mkIf(chk, get, mkNull))
    }

    case list @ ListDescriptor(id, tpe, _, elem) => {
      val chk = mkAnd(env.mkChkIdx(id), env.mkNotIsNull(id, source))

      val (init, pactList) = env.reentrant match {

        // This is a bit conservative, but avoids runtime checks
        // and/or even more specialized deserialize() methods to
        // track whether it's safe to reuse the list variable.
        case true => {
          val listTpe = env.listImpls(id)
          val list = mkVal("list" + id, NoFlags, false, listTpe, New(TypeTree(listTpe), List(List())))
          (list, Ident("list" + id: TermName))
        }

        case false => {
          val clear = Apply(Select(env.mkSelectWrapper(id), "clear"), List())
          (clear, env.mkSelectWrapper(id))
        }
      }

      //        val buildTpe = appliedType(builderClass.tpe, List(elem.tpe, tpe))
      //        val build = mkVal(env.methodSym, "b" + id, 0, false, buildTpe) { _ => Apply(Select(cbf(), "apply"), List()) }
//      val userList = mkVal("b" + id, NoFlags, false, tpe, New(TypeTree(tpe), List(List())))
      val buildTpe = mkBuilderOf(elem.tpe, tpe)
      val cbf = c.inferImplicitValue(mkCanBuildFromOf(tpe, elem.tpe, tpe))
      val build = mkVal("b" + id, NoFlags, false, buildTpe, Apply(Select(cbf, "apply": TermName), List()))
      val des = env.mkGetFieldInto(id, source, pactList)
      val body = genDeserializeList(elem, pactList, Ident("b" + id: TermName), env.copy(allowRecycling = false, chkNull = true), scope)
      val stats = init +: des +: build +: body

      Seq(mkIf(chk, Block(stats.init.toList, stats.last), mkNull))
    }

    // we have a mutable UDT and the context allows recycling
    case CaseClassDescriptor(_, tpe, true, _, getters) if env.allowRecycling => {

      val fields = getters filterNot { _.isBaseField } map {
        case FieldAccessor(_, _, _, _, desc) => (desc.id, mkVal("v" + desc.id, NoFlags, false, desc.tpe, {
          mkSingle(genDeserialize(desc, source, env, scope))
        }), desc.tpe, "v" + desc.id)
      }

      val newScope = scope ++ (fields map { case (id, tree, tpe, name) => id -> (name, tpe) })

      val stats = fields map { _._2 }

      val setterStats = getters map {
        case FieldAccessor(_, setter, fTpe, _, fDesc) => {
          val (name, tpe) = newScope(fDesc.id)
          val castVal = maybeMkAsInstanceOf(Ident(name: TermName))(c.WeakTypeTag(tpe), c.WeakTypeTag(fTpe))
          env.mkCallSetMutableField(desc.id, setter, castVal)
        }
      }

      val ret = env.mkSelectMutableUdtInst(desc.id)

      (stats ++ setterStats) :+ ret
    }

    case CaseClassDescriptor(_, tpe, _, _, getters) => {

      val fields = getters filterNot { _.isBaseField } map {
        case FieldAccessor(_, _, _, _, desc) => (desc.id, mkVal("v" + desc.id, NoFlags, false, desc.tpe, {
          mkSingle(genDeserialize(desc, source, env, scope))
        }), desc.tpe, "v" + desc.id)
      }

      val newScope = scope ++ (fields map { case (id, tree, tpe, name) => id -> (name, tpe) })

      val stats = fields map { _._2 }

      val args = getters map {
        case FieldAccessor(_, _, fTpe, _, fDesc) => {
          val (name, tpe) = newScope(fDesc.id)
          maybeMkAsInstanceOf(Ident(name: TermName))(c.WeakTypeTag(tpe), c.WeakTypeTag(fTpe))
        }
      }

      val ret = New(TypeTree(tpe), List(args.toList))

      stats :+ ret
    }

    case BaseClassDescriptor(_, tpe, Seq(tagField, baseFields @ _*), subTypes) => {

      val fields = baseFields map {
        case FieldAccessor(_, _, _, _, desc) => (desc.id, mkVal("v" + desc.id, NoFlags, false, desc.tpe, {
          val special = desc match {
            case d @ PrimitiveDescriptor(id, _, _, _) if id == tagField.desc.id => d.copy(default = Literal(Constant(-1)))
            case _ => desc
          }
          mkSingle(genDeserialize(desc, source, env, scope))
        }), desc.tpe, "v" + desc.id)
      }

      val newScope = scope ++ (fields map { case (id, tree, tpe, name) => id -> (name, tpe) })

      val stats = fields map { _._2 }

      val cases = subTypes.zipWithIndex.toList map {
        case (dSubType, i) => {
          val code = mkSingle(genDeserialize(dSubType, source, env, newScope))
          val pat = Bind("tag": TermName, Literal(Constant(i)))
          CaseDef(pat, EmptyTree, code)
        }
      }

      val chk = env.mkChkIdx(tagField.desc.id)
      val des = env.mkGetFieldInto(tagField.desc.id, source)
      val get = env.mkGetValue(tagField.desc.id)
      Seq(mkIf(chk, Block(stats.toList :+ des, Match(get, cases)), mkNull))
    }

    case RecursiveDescriptor(id, tpe, refId) => {
      val chk = mkAnd(env.mkChkIdx(id), env.mkNotIsNull(id, source))
      val rec = mkVal("record" + id, NoFlags, false, typeOf[eu.stratosphere.pact.common.`type`.PactRecord], New(TypeTree(typeOf[eu.stratosphere.pact.common.`type`.PactRecord]), List(List())))
      val get = env.mkGetFieldInto(id, source, Ident("record" + id: TermName))
      val des = env.mkCallDeserialize(refId, Ident("record" + id: TermName))

      Seq(mkIf(chk, Block(List(rec, get), des), mkNull))
    }

    case _ => Seq(mkNull)
  }

  private def genDeserializeList(elem: UDTDescriptor, source: Tree, target: Tree, env: GenEnvironment, scope: Map[Int, (String, Type)]): Seq[Tree] = {

    val size = mkVal("size", NoFlags, false, definitions.IntTpe, Apply(Select(source, "size"), List()))
    val sizeHint = Apply(Select(target, "sizeHint"), List(Ident("size": TermName)))
    val i = mkVar("i", NoFlags, false, definitions.IntTpe, mkZero)

    val loop = mkWhile(Apply(Select(Ident("i": TermName), "$less"), List(Ident("size": TermName)))) {

      val item = mkVal("item", NoFlags, false, getListElemWrapperType(elem, env), Apply(Select(source, "get"), List(Ident("i": TermName))))

      val (stats, value) = elem match {

        case PrimitiveDescriptor(_, _, _, wrapper) => (Seq(), env.mkGetValue(Ident("item": TermName)))

        case BoxedPrimitiveDescriptor(_, _, _, wrapper, box, _) => (Seq(), box(env.mkGetValue(Ident("item": TermName))))

        case ListDescriptor(id, tpe, _, innerElem) => {

          //            val buildTpe = appliedType(builderClass.tpe, List(innerElem.tpe, tpe))
          //            val build = mkVal(env.methodSym, "b" + id, 0, false, buildTpe) { _ => Apply(Select(cbf(), "apply"), List()) }
          val buildTpe = mkBuilderOf(innerElem.tpe, tpe)
          val cbf = c.inferImplicitValue(mkCanBuildFromOf(tpe, innerElem.tpe, tpe))
          val build = mkVal("b" + id, NoFlags, false, buildTpe, Apply(Select(cbf, "apply": TermName), List()))
          val body = mkVal("v" + id, NoFlags, false, elem.tpe,
            mkSingle(genDeserializeList(innerElem, Ident("item": TermName), Ident("b" + id: TermName), env, scope)))
          (Seq(build, body), Ident("v" + id: TermName))
        }

        case RecursiveDescriptor(id, tpe, refId) => (Seq(), env.mkCallDeserialize(refId, Ident("item": TermName)))

        case _ => {
          val body = genDeserialize(elem, Ident("item": TermName), env.copy(idxPrefix = "boxed" + elem.id, chkIndex = false, chkNull = false), scope)
          val v = mkVal("v" + elem.id, NoFlags, false, elem.tpe, mkSingle(body))
          (Seq(v), Ident("v" + elem.id: TermName))
        }
      }

      val chk = env.mkChkNotNull(Ident("item": TermName), elem.tpe)
      val add = Apply(Select(target, "$plus$eq"), List(value))
      val addNull = Apply(Select(target, "$plus$eq"), List(mkNull))
      val inc = Assign(Ident("i": TermName), Apply(Select(Ident("i": TermName), "$plus"), List(mkOne)))

      Block(List(item, mkIf(chk, mkSingle(stats :+ add), addNull)), inc)
    }
    
    val get = Apply(Select(target, "result"), List())

    Seq(size, sizeHint, i, loop, get)
  }
  

  private def getListElemWrapperType(desc: UDTDescriptor, env: GenEnvironment): Type = desc match {
    case PrimitiveDescriptor(_, _, _, wrapper) => wrapper
    case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, _) => wrapper
    case ListDescriptor(id, _, _, _) => env.listImpls(id)
    case _ => typeOf[eu.stratosphere.pact.common.`type`.PactRecord]
  }
}