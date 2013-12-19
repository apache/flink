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

trait SerializeMethodGen[C <: Context] { this: MacroContextHolder[C] with UDTDescriptors[C] with TreeGen[C] with SerializerGen[C] with Loggers[C] =>
  import c.universe._

  protected def mkSerialize(desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

//    val root = mkMethod("serialize", Flag.OVERRIDE | Flag.FINAL, List(("item", desc.tpe), ("record", typeOf[eu.stratosphere.pact.common.`type`.Record])), definitions.UnitTpe, {
    val root = mkMethod("serialize", Flag.FINAL, List(("item", desc.tpe), ("record", typeOf[eu.stratosphere.types.Record])), definitions.UnitTpe, {
      val env = GenEnvironment(listImpls, "flat" + desc.id, false, true, true, true)
      val stats = genSerialize(desc, Ident("item": TermName), Ident("record": TermName), env)
      Block(stats.toList, mkUnit)
    })

    val aux = desc.getRecursiveRefs map { desc =>
      mkMethod("serialize" + desc.id, Flag.PRIVATE | Flag.FINAL, List(("item", desc.tpe), ("record", typeOf[eu.stratosphere.types.Record])), definitions.UnitTpe, {
        val env = GenEnvironment(listImpls, "boxed" + desc.id, true, false, false, true)
        val stats = genSerialize(desc, Ident("item": TermName), Ident("record": TermName), env)
        Block(stats.toList, mkUnit)
      })
    }

    root +: aux.toList
  }

  private def genSerialize(desc: UDTDescriptor, source: Tree, target: Tree, env: GenEnvironment): Seq[Tree] = desc match {

    case PactValueDescriptor(id, _) => {
      val chk = env.mkChkIdx(id)
      val set = env.mkSetField(id, target, source)

      Seq(mkIf(chk, set))
    }
    
    case PrimitiveDescriptor(id, _, _, _) => {
      val chk = env.mkChkIdx(id)
      val ser = env.mkSetValue(id, source)
      val set = env.mkSetField(id, target)

      Seq(mkIf(chk, Block(List(ser), set)))
    }

    case BoxedPrimitiveDescriptor(id, tpe, _, _, _, unbox) => {
      val chk = mkAnd(env.mkChkIdx(id), env.mkChkNotNull(source, tpe))
      val ser = env.mkSetValue(id, unbox(source))
      val set = env.mkSetField(id, target)

      Seq(mkIf(chk, Block(List(ser), set)))
    }

    case desc @ ListDescriptor(id, tpe, iter, elem) => {
      val chk = mkAnd(env.mkChkIdx(id), env.mkChkNotNull(source, tpe))

      val upd = desc.getInnermostElem match {
        case _: RecursiveDescriptor => Some(Apply(Select(target, "updateBinaryRepresenation"), List()))
        case _ => None
      }

      val (init, list) = env.reentrant match {

        // This is a bit conservative, but avoids runtime checks
        // and/or even more specialized serialize() methods to
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

      val body = genSerializeList(elem, iter(source), list, env.copy(chkNull = true))
      val set = env.mkSetField(id, target, list)
      val stats = (init +: body) :+ set

      val updStats = upd ++ stats
      Seq(mkIf(chk, Block(updStats.init.toList, updStats.last)))
    }

    case CaseClassDescriptor(_, tpe, _, _, getters) => {
      val chk = env.mkChkNotNull(source, tpe)
      val stats = getters filterNot { _.isBaseField } flatMap { case FieldAccessor(sym, _, _, _, desc) => genSerialize(desc, Select(source, sym), target, env.copy(chkNull = true)) }

      stats match {
        case Nil => Seq()
        case _ => Seq(mkIf(chk, mkSingle(stats)))
      }
    }

    case BaseClassDescriptor(id, tpe, Seq(tagField, baseFields @ _*), subTypes) => {
      val chk = env.mkChkNotNull(source, tpe)
      val fields = baseFields flatMap { (f => genSerialize(f.desc, Select(source, f.getter), target, env.copy(chkNull = true))) }
      val cases = subTypes.zipWithIndex.toList map {
        case (dSubType, i) => {

          val pat = Bind("inst": TermName, Typed(Ident("_"), TypeTree(dSubType.tpe)))
          val cast = None
          val inst = Ident("inst": TermName)
          //            val (pat, cast, inst) = {
          //              val erasedTpe = mkErasedType(env.methodSym, dSubType.tpe)
          //
          //              if (erasedTpe =:= dSubType.tpe) {
          //
          //                val pat = Bind(newTermName("inst"), Typed(Ident("_"), TypeTree(dSubType.tpe)))
          //                (pat, None, Ident(newTermName("inst")))
          //
          //              } else {
          //
          //                // This avoids type erasure warnings in the generated pattern match
          //                val pat = Bind(newTermName("erasedInst"), Typed(Ident("_"), TypeTree(erasedTpe)))
          //                val cast = mkVal("inst", NoFlags, false, dSubType.tpe, mkAsInstanceOf(Ident("erasedInst"))(c.WeakTypeTag(dSubType.tpe)))
          //                val inst = Ident(cast.symbol)
          //                (pat, Some(cast), inst)
          //              }
          //            }

          val tag = genSerialize(tagField.desc, c.literal(i).tree, target, env.copy(chkNull = false))
          val code = genSerialize(dSubType, inst, target, env.copy(chkNull = false))
          val body = (cast.toSeq ++ tag ++ code) :+ mkUnit

          CaseDef(pat, EmptyTree, Block(body.init.toList, body.last))
        }
      }

      Seq(mkIf(chk, Block(fields.toList,Match(source, cases))))
    }

    case RecursiveDescriptor(id, tpe, refId) => {
      // Important: recursive types introduce re-entrant calls to serialize()

      val chk = mkAnd(env.mkChkIdx(id), env.mkChkNotNull(source, tpe))

      // Persist the outer record prior to recursing, since the call
      // is going to reuse all the PactPrimitive wrappers that were 
      // needed *before* the recursion.
      val updTgt = Apply(Select(target, "updateBinaryRepresenation"), List())

      val rec = mkVal("record" + id, NoFlags, false, typeOf[eu.stratosphere.types.Record], New(TypeTree(typeOf[eu.stratosphere.types.Record]), List(List())))
      val ser = env.mkCallSerialize(refId, source, Ident("record" + id: TermName))

      // Persist the new inner record after recursing, since the
      // current call is going to reuse all the PactPrimitive
      // wrappers that are needed *after* the recursion.
      val updRec = Apply(Select(Ident("record" + id: TermName), "updateBinaryRepresenation"), List())

      val set = env.mkSetField(id, target, Ident("record" + id: TermName))

      Seq(mkIf(chk, Block(List(updTgt, rec, ser, updRec), set)))
    }
  }

  private def genSerializeList(elem: UDTDescriptor, iter: Tree, target: Tree, env: GenEnvironment): Seq[Tree] = {

    val it = mkVal("it", NoFlags, false, mkIteratorOf(elem.tpe), iter)

    val loop = mkWhile(Select(Ident("it": TermName), "hasNext")) {

      val item = mkVal("item", NoFlags, false, elem.tpe, Select(Ident("it": TermName), "next"))

      val (stats, value) = elem match {

        case PrimitiveDescriptor(_, _, _, wrapper) => (Seq(), New(TypeTree(wrapper), List(List(Ident("item": TermName)))))

        case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, unbox) => (Seq(), New(TypeTree(wrapper), List(List(unbox(Ident("item": TermName))))))
        
        case PactValueDescriptor(_, tpe) => (Seq(), Ident("item": TermName))

        case ListDescriptor(id, _, iter, innerElem) => {
          val listTpe = env.listImpls(id)
          val list = mkVal("list" + id, NoFlags, false, listTpe, New(TypeTree(listTpe), List(List())))
          val body = genSerializeList(innerElem, iter(Ident("item": TermName)), Ident("list" + id: TermName), env)
          (list +: body, Ident("list" + id: TermName))
        }

        case RecursiveDescriptor(id, tpe, refId) => {
          val rec = mkVal("record" + id, NoFlags, false, typeOf[eu.stratosphere.types.Record], New(TypeTree(typeOf[eu.stratosphere.types.Record]), List(List())))
          val ser = env.mkCallSerialize(refId, Ident("item": TermName), Ident("record" + id: TermName))
          val updRec = Apply(Select(Ident("record" + id: TermName), "updateBinaryRepresenation"), List())

          (Seq(rec, ser, updRec), Ident("record" + id: TermName))
        }

        case _ => {
          val rec = mkVal("record", NoFlags, false, typeOf[eu.stratosphere.types.Record], New(TypeTree(typeOf[eu.stratosphere.types.Record]), List(List())))
          val ser = genSerialize(elem, Ident("item": TermName), Ident("record": TermName), env.copy(idxPrefix = "boxed" + elem.id, chkIndex = false, chkNull = false))
          val upd = Apply(Select(Ident("record": TermName), "updateBinaryRepresenation"), List())
          ((rec +: ser) :+ upd, Ident("record": TermName))
        }
      }

      val chk = env.mkChkNotNull(Ident("item": TermName), elem.tpe)
      val add = Apply(Select(target, "add"), List(value))
      val addNull = Apply(Select(target, "add"), List(mkNull))

      Block(List(item), mkIf(chk, mkSingle(stats :+ add), addNull))
    }

    Seq(it, loop)
  }
}

