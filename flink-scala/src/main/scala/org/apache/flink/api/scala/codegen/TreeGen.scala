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

import scala.language.implicitConversions
import scala.reflect.macros.whitebox.Context

@Internal
private[flink] trait TreeGen[C <: Context] { this: MacroContextHolder[C] with TypeDescriptors[C] =>
  import c.universe._

  def mkDefault(tpe: Type): Tree = {
    tpe match {
      case definitions.BooleanTpe => Literal(Constant(false))
      case definitions.ByteTpe => Literal(Constant(0: Byte))
      case definitions.CharTpe => Literal(Constant(0: Char))
      case definitions.DoubleTpe => Literal(Constant(0: Double))
      case definitions.FloatTpe => Literal(Constant(0: Float))
      case definitions.IntTpe => Literal(Constant(0: Int))
      case definitions.LongTpe => Literal(Constant(0: Long))
      case definitions.ShortTpe => Literal(Constant(0: Short))
      case definitions.UnitTpe => Literal(Constant(()))
      case _ => Literal(Constant(null: Any))
    }
  }

  def mkUnit = reify(()).tree
  def mkNull = reify(null).tree
  def mkZero = reify(0).tree
  def mkOne = reify(1).tree

  def mkAsInstanceOf[T: c.WeakTypeTag](source: Tree): Tree =
    reify(c.Expr(source).splice.asInstanceOf[T]).tree

  def maybeMkAsInstanceOf[S: c.WeakTypeTag, T: c.WeakTypeTag](source: Tree): Tree = {
    if (weakTypeOf[S] <:< weakTypeOf[T]) {
      source
    } else {
      mkAsInstanceOf[T](source)
    }
  }

//    def mkIdent(target: Symbol): Tree = Ident(target) setType target.tpe
  def mkSelect(rootModule: String, path: String*): Tree =
    mkSelect(Ident(TermName(rootModule)), path: _*)

  def mkSelect(source: Tree, path: String*): Tree =
    path.foldLeft(source)((ret, item) => Select(ret, TermName(item)))

  def mkSelectSyms(source: Tree, path: Symbol*): Tree =
    path.foldLeft(source)((ret, item) => Select(ret, item))

  def mkCall(root: Tree, path: String*)(args: List[Tree]) = Apply(mkSelect(root, path: _*), args)

  def mkSeq(items: List[Tree]): Tree =
    Apply(mkSelect("_root_", "scala", "collection", "Seq", "apply"), items)

  def mkList(items: List[Tree]): Tree =
    Apply(mkSelect("_root_", "scala", "collection", "immutable", "List", "apply"), items)

  def mkMap(items: List[Tree]): Tree =
    Apply(mkSelect("_root_", "scala", "collection", "immutable", "Map", "apply"), items)

  def mkVal(name: String, flags: FlagSet, transient: Boolean, valTpe: Type, value: Tree): Tree = {
    ValDef(Modifiers(flags), TermName(name), TypeTree(valTpe), value)
  }

  def mkVar(name: String, flags: FlagSet, transient: Boolean, valTpe: Type, value: Tree): Tree = {
    mkVal(name, flags | Flag.MUTABLE, transient, valTpe, value)
  }

  def mkValAndGetter(name: String, flags: FlagSet, valTpe: Type, value: Tree): List[Tree] = {
    val fieldName = name + " "
    val valDef = mkVal(fieldName, Flag.PRIVATE, false, valTpe, value)
    val defDef = mkMethod(name, flags, Nil, valTpe, Ident(TermName(fieldName)))
    List(valDef, defDef)
  }

  def mkVarAndLazyGetter(name: String, flags: FlagSet, valTpe: Type, value: Tree): (Tree, Tree) = {
    val fieldName = name + " "
    val field = mkVar(fieldName, NoFlags, false, valTpe, mkNull)
    val fieldSel = Ident(TermName(fieldName))

    val getter = mkMethod(
      name,
      flags,
      Nil,
      valTpe, {
        val eqeq = Select(fieldSel, TermName("$eq$eq"))
        val chk = Apply(eqeq, List(mkNull))
        val init = Assign(fieldSel, value)
        Block(List(If(chk, init, EmptyTree)), fieldSel)
      }
    )

    (field, getter)
  }

  def mkIf(cond: Tree, bodyT: Tree): Tree = mkIf(cond, bodyT, EmptyTree)

  def mkIf(cond: Tree, bodyT: Tree, bodyF: Tree): Tree = cond match {
    case EmptyTree => bodyT
    case _ => If(cond, bodyT, bodyF)
  }

  def mkSingle(stats: Seq[Tree]): Tree = stats match {
    case Seq() => EmptyTree
    case Seq(stat) => stat
    case _ => Block(stats.init.toList, stats.last)
  }

  def mkAnd(cond1: Tree, cond2: Tree): Tree = cond1 match {
    case EmptyTree => cond2
    case _ =>
      cond2 match {
        case EmptyTree => cond1
        case _ => reify(c.Expr[Boolean](cond1).splice && c.Expr[Boolean](cond2).splice).tree
      }
  }

  def mkMethod(
      name: String,
      flags: FlagSet,
      args: List[(String, Type)],
      ret: Type,
      impl: Tree): Tree = {
    val valParams = args.map {
      case (`name`, tpe) =>
        ValDef(Modifiers(Flag.PARAM), TermName(name), TypeTree(tpe), EmptyTree)
    }
    DefDef(Modifiers(flags), TermName(name), Nil, List(valParams), TypeTree(ret), impl)
  }

  def mkClass(
      name: TypeName,
      flags: FlagSet,
      parents: List[Type],
      members: List[Tree]): ClassDef = {
    val parentTypeTrees = parents.map(TypeTree(_))
    val selfType = ValDef(Modifiers(), termNames.WILDCARD, TypeTree(NoType), EmptyTree)
    ClassDef(Modifiers(flags), name, Nil, Template(parentTypeTrees, selfType, members))
  }

  def mkThrow(tpe: Type, msg: Tree): Tree =
    Throw(Apply(Select(New(TypeTree(tpe)), termNames.CONSTRUCTOR), List(msg)))

//    def mkThrow(tpe: Type, msg: Tree): Tree = Throw(New(TypeTree(tpe)), List(List(msg))))

  def mkThrow(tpe: Type, msg: String): Tree = mkThrow(tpe, c.literal(msg).tree)

  def mkThrow(msg: String): Tree = mkThrow(typeOf[java.lang.RuntimeException], msg)

  implicit def tree2Ops[T <: Tree](tree: T) = new {
    // copy of Tree.copyAttrs, since that method is private
//      def copyAttrs(from: Tree): T = {
//        tree.pos = from.pos
//        tree.tpe = from.tpe
//        if (tree.hasSymbol) tree.symbol = from.symbol
//        tree
//      }

    def getSimpleClassName: String = {
      val name = tree.getClass.getName
      val idx = math.max(name.lastIndexOf('$'), name.lastIndexOf('.')) + 1
      name.substring(idx)
    }
  }

  def mkIteratorOf(tpe: Type) = {
    def makeIt[T: c.WeakTypeTag] = weakTypeOf[Iterator[T]]
    makeIt(c.WeakTypeTag(tpe))
  }

  def mkSeqOf(tpe: Type) = {
    def makeIt[T: c.WeakTypeTag] = weakTypeOf[Seq[T]]
    makeIt(c.WeakTypeTag(tpe))
  }

  def mkListOf(tpe: Type) = {
    def makeIt[T: c.WeakTypeTag] = weakTypeOf[List[T]]
    makeIt(c.WeakTypeTag(tpe))
  }

  def mkBuilderOf(elemTpe: Type, listTpe: Type) = {
    def makeIt[ElemTpe: c.WeakTypeTag, ListTpe: c.WeakTypeTag] =
      weakTypeOf[scala.collection.mutable.Builder[ElemTpe, ListTpe]]

    makeIt(c.WeakTypeTag(elemTpe), c.WeakTypeTag(listTpe))
  }

  def mkCanBuildFromOf(fromTpe: Type, elemTpe: Type, toTpe: Type) = {
    def makeIt[From: c.WeakTypeTag, Elem: c.WeakTypeTag, To: c.WeakTypeTag] =
      weakTypeOf[scala.collection.generic.CanBuildFrom[From, Elem, To]]

    makeIt(c.WeakTypeTag(fromTpe), c.WeakTypeTag(elemTpe), c.WeakTypeTag(toTpe))
  }

  def mkCtorCall(tpe: Type, args: List[Tree]) =
    Apply(Select(New(TypeTree(tpe)), termNames.CONSTRUCTOR), args)

  def mkSuperCall(args: List[Tree] = List()) =
    Apply(Select(Super(This(typeNames.EMPTY), typeNames.EMPTY), termNames.CONSTRUCTOR), args)

  def mkWhile(cond: Tree)(body: Tree): Tree = {
    val lblName = c.freshName("while")
    Ident(TermName(lblName))
    val jump = Apply(Ident(TermName(lblName)), Nil)
    val block = body match {
      case Block(stats, expr) => Block(stats :+ expr, jump)
      case _ => Block(List(body), jump)
    }
    LabelDef(lblName, Nil, If(cond, block, EmptyTree))
  }

  def typeCheck(classDef: ClassDef): (ClassDef, Type) = {
    val block = Block(List(classDef), EmptyTree)
    val checkedBlock = c.typecheck(block)

    // extract the class def from the block again
    val checkedDef = checkedBlock match {
      case Block((cls: ClassDef) :: Nil, _) => cls
    }
    val tpe = checkedDef.symbol.asClass.toType
    (checkedDef, tpe)
  }

  def extractOneInputUdf(fun: Tree) = {
    val (paramName, udfBody) = fun match {
      case Function(List(param), body) => (param.name.toString, body)
      case _ =>
        c.abort(c.enclosingPosition, "Could not extract user defined function, got: " + show(fun))
    }
    val uncheckedUdfBody = c.untypecheck(udfBody)
    (paramName, uncheckedUdfBody)
  }

  def extractTwoInputUdf(fun: Tree) = {
    val (param1Name, param2Name, udfBody) = fun match {
      case Function(List(param1, param2), body) =>
        (param1.name.toString, param2.name.toString, body)
      case _ =>
        c.abort(c.enclosingPosition, "Could not extract user defined function, got: " + show(fun))
    }
    val uncheckedUdfBody = c.untypecheck(udfBody)
    (param1Name, param2Name, uncheckedUdfBody)
  }

  def extractClass(block: Tree) = block match {
    case Block((cls: ClassDef) :: Nil, _) => cls
    case e => throw new RuntimeException("No class def at first position in block.")
  }
}
