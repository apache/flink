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
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.scala.analysis.UDTSerializer

trait SerializerGen[C <: Context] { this: MacroContextHolder[C] with UDTDescriptors[C] with UDTAnalyzer[C] with TreeGen[C] with SerializeMethodGen[C] with DeserializeMethodGen[C] with Loggers[C] =>
  import c.universe._

  def mkUdtSerializerClass[T: c.WeakTypeTag](name: String = "", creatorName: String = "createSerializer"): (ClassDef, Tree) = {
    val desc = getUDTDescriptor(weakTypeOf[T])

    desc match {
      case UnsupportedDescriptor(_, _, errs) => {
        val errorString = errs.mkString("\n")
        c.abort(c.enclosingPosition, s"Error analyzing UDT ${weakTypeOf[T]}: $errorString")
      }
      case _ =>
    }

    val serName = newTypeName("UDTSerializerImpl" + name)
    val ser = mkClass(serName, Flag.FINAL, List(weakTypeOf[UDTSerializer[T]]), {

      val (listImpls, listImplTypes) = mkListImplClasses(desc)

      val indexMapIter = Select(Ident("indexMap": TermName), "iterator": TermName)
      val (fields1, inits1) = mkIndexes(desc.id, getIndexFields(desc).toList, false, indexMapIter)
      val (fields2, inits2) = mkBoxedIndexes(desc)

      val fields = fields1 ++ fields2
      val init = inits1 ++ inits2 match {
        case Nil => Nil
        case inits => List(mkMethod("init", Flag.OVERRIDE | Flag.FINAL, List(), definitions.UnitTpe, Block(inits, mkUnit)))
      }

      val (wrapperFields, wrappers) = mkPactWrappers(desc, listImplTypes)

      val mutableUdts = desc.flatten.toList flatMap {
        case cc @ CaseClassDescriptor(_, _, true, _, _) => Some(cc)
        case _ => None
      } distinct

      val mutableUdtInsts = mutableUdts map { u => mkMutableUdtInst(u) }

      val helpers = listImpls ++ fields ++ wrapperFields ++ mutableUdtInsts ++ init
      val ctor = mkMethod(nme.CONSTRUCTOR.toString(), NoFlags, List(("indexMap", typeOf[Array[Int]])), NoType, {
        Block(List(mkSuperCall(List(Ident(newTermName("indexMap"))))), mkUnit)
      })
      
//      val methods = List(ctor)// ++ List(mkGetFieldIndex(desc)) //++ mkSerialize(desc, listImplTypes) ++ mkDeserialize(desc, listImplTypes)
      val methods = List(ctor) ++ mkSerialize(desc, listImplTypes) ++ mkDeserialize(desc, listImplTypes)

      helpers ++ methods
    })
    

    val (_, serTpe) = typeCheck(ser)
    
    val createSerializer = mkMethod(creatorName, Flag.OVERRIDE, List(("indexMap", typeOf[Array[Int]])), NoType, {
      Block(List(), mkCtorCall(serTpe, List(Ident(newTermName("indexMap")))))
    })
    (ser, createSerializer)
  }

  private def mkListImplClass[T <: eu.stratosphere.pact.common.`type`.Value: c.WeakTypeTag]: (Tree, Type) = {
    val listImplName = c.fresh[TypeName]("PactListImpl")
    val tpe = weakTypeOf[eu.stratosphere.pact.common.`type`.base.PactList[T]]

    val listDef = mkClass(listImplName, Flag.FINAL, List(tpe), {
      List(mkMethod(nme.CONSTRUCTOR.toString(), NoFlags, List(), NoType, Block(List(mkSuperCall()), mkUnit)))
    })

    typeCheck(listDef)
  }

  def mkListImplClasses(desc: UDTDescriptor): (List[Tree], Map[Int, Type]) = {
    desc match {
      case ListDescriptor(id, _, _, elem: ListDescriptor) => {
        val (defs, tpes) = mkListImplClasses(elem)
        val (listDef, listTpe) = mkListImplClass(c.WeakTypeTag(tpes(elem.id)))
        (defs :+ listDef, tpes + (id -> listTpe))
      }
      case ListDescriptor(id, _, _, elem: PrimitiveDescriptor) => {
        val (classDef, tpe) = mkListImplClass(c.WeakTypeTag(elem.wrapper))
        (List(classDef), Map(id -> tpe))
      }
      case ListDescriptor(id, _, _, elem: BoxedPrimitiveDescriptor) => {
        val (classDef, tpe) = mkListImplClass(c.WeakTypeTag(elem.wrapper))
        (List(classDef), Map(id -> tpe))
      }
      case ListDescriptor(id, _, _, elem) => {
        val (classDefs, tpes) = mkListImplClasses(elem)
        val (classDef, tpe) = mkListImplClass(c.WeakTypeTag(typeOf[eu.stratosphere.pact.common.`type`.PactRecord]))
        (classDefs :+ classDef, tpes + (id -> tpe))
      }
      case BaseClassDescriptor(_, _, getters, subTypes) => {
        val (defs, tpes) = getters.foldLeft((List[Tree](), Map[Int, Type]())) { (result, f) =>
          val (defs, tpes) = result
          val (newDefs, newTpes) = mkListImplClasses(f.desc)
          (defs ++ newDefs, tpes ++ newTpes)
        }
        val (subDefs, subTpes) = subTypes.foldLeft((List[Tree](), Map[Int, Type]())) { (result, s) =>
          val (defs, tpes) = result
          val (innerDefs, innerTpes) = mkListImplClasses(s)
          (defs ++ innerDefs, tpes ++ innerTpes)
        }
        (defs ++ subDefs, tpes ++ subTpes)
      }
      case CaseClassDescriptor(_, _, _, _, getters) => {
        getters.foldLeft((List[Tree](), Map[Int, Type]())) { (result, f) =>
          val (defs, tpes) = result
          val (newDefs, newTpes) = mkListImplClasses(f.desc)
          (defs ++ newDefs, tpes ++ newTpes)
        }
      }
      case _ => {
        (List[Tree](), Map[Int, Type]())
      }
    }
  }

  private def mkIndexes(descId: Int, descFields: List[UDTDescriptor], boxed: Boolean, indexMapIter: Tree): (List[Tree], List[Tree]) = {

    val prefix = (if (boxed) "boxed" else "flat") + descId
    val iterName = prefix + "Iter"
    val iter = mkVal(iterName, Flag.PRIVATE, true, mkIteratorOf(definitions.IntTpe), indexMapIter)

    val fieldsAndInits = descFields map {
      case d => {
        val next = Apply(Select(Ident(iterName: TermName), "next": TermName), Nil)
        val idxField = mkVal(prefix + "Idx" + d.id, Flag.PRIVATE, false, definitions.IntTpe, next)

        (List(idxField), Nil)
      }
    }

    val (fields, inits) = fieldsAndInits.unzip
    (iter +: fields.flatten, inits.flatten)
  }

  protected def getIndexFields(desc: UDTDescriptor): Seq[UDTDescriptor] = desc match {
    // Flatten product types
    case CaseClassDescriptor(_, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getIndexFields(f.desc) }
    // TODO: Rather than laying out subclass fields sequentially, just reserve enough fields for the largest subclass.
    // This is tricky because subclasses can contain opaque descriptors, so we don't know how many fields we need until runtime.
    case BaseClassDescriptor(id, _, getters, subTypes) => (getters flatMap { f => getIndexFields(f.desc) }) ++ (subTypes flatMap getIndexFields)
    case _ => Seq(desc)
  }

  private def mkBoxedIndexes(desc: UDTDescriptor): (List[Tree], List[Tree]) = {

    def getBoxedDescriptors(d: UDTDescriptor): Seq[UDTDescriptor] = d match {
      case ListDescriptor(_, _, _, elem: BaseClassDescriptor) => elem +: getBoxedDescriptors(elem)
      case ListDescriptor(_, _, _, elem: CaseClassDescriptor) => elem +: getBoxedDescriptors(elem)
      case ListDescriptor(_, _, _, elem) => getBoxedDescriptors(elem)
      case CaseClassDescriptor(_, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getBoxedDescriptors(f.desc) }
      case BaseClassDescriptor(id, _, getters, subTypes) => (getters flatMap { f => getBoxedDescriptors(f.desc) }) ++ (subTypes flatMap getBoxedDescriptors)
      case RecursiveDescriptor(_, _, refId) => desc.findById(refId).map(_.mkRoot).toSeq
      case _ => Seq()
    }

    val fieldsAndInits = getBoxedDescriptors(desc).distinct.toList flatMap { d =>
      // the way this is done here is a relic from the support of OpaqueDescriptors
      // there it was not just mkOne but actual differing numbers of fields
      // retrieved from the opaque UDT descriptors
      getIndexFields(d).toList match {
        case Nil => None
        case fields => {
          val widths = fields map {
            case _ => mkOne
          }
          val sum = widths.reduce { (s, i) => Apply(Select(s, "$plus": TermName), List(i)) }
          val range = Apply(Select(Ident("scala": TermName), "Range": TermName), List(mkZero, sum))
          Some(mkIndexes(d.id, fields, true, Select(range, "iterator": TermName)))
        }
      }
    }

    val (fields, inits) = fieldsAndInits.unzip
    (fields.flatten, inits.flatten)
  }

  private def mkPactWrappers(desc: UDTDescriptor, listImpls: Map[Int, Type]): (List[Tree], List[(Int, Type)]) = {

    def getFieldTypes(desc: UDTDescriptor): Seq[(Int, Type)] = desc match {
      case PrimitiveDescriptor(id, _, _, wrapper) => Seq((id, wrapper))
      case BoxedPrimitiveDescriptor(id, _, _, wrapper, _, _) => Seq((id, wrapper))
      case d @ ListDescriptor(id, _, _, elem) => {
        val listField = (id, listImpls(id))
        val elemFields = d.getInnermostElem match {
          case elem: CaseClassDescriptor => getFieldTypes(elem)
          case elem: BaseClassDescriptor => getFieldTypes(elem)
          case _ => Seq()
        }
        listField +: elemFields
      }
      case CaseClassDescriptor(_, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getFieldTypes(f.desc) }
      case BaseClassDescriptor(_, _, getters, subTypes) => (getters flatMap { f => getFieldTypes(f.desc) }) ++ (subTypes flatMap getFieldTypes)
      case _ => Seq()
    }

    getFieldTypes(desc) toList match {
      case Nil => (Nil, Nil)
      case types =>
        val fields = types map { case (id, tpe) => mkVar("w" + id, Flag.PRIVATE, true, tpe, mkCtorCall(tpe, List())) }
        (fields, types)
    }
  }

  private def mkMutableUdtInst(desc: CaseClassDescriptor): Tree = {
    val args = desc.getters map {
      case FieldAccessor(_, _, fTpe, _, _) => {
        mkDefault(fTpe)
      }
    }

    val ctor = mkCtorCall(desc.tpe, args.toList)
    mkVar("mutableUdtInst" + desc.id, Flag.PRIVATE, true, desc.tpe, ctor)
  }

  private def mkGetFieldIndex(desc: UDTDescriptor): Tree = {

    val env = GenEnvironment(Map(), "flat" + desc.id, false, true, true, true)

    def mkCases(desc: UDTDescriptor, path: Seq[String]): Seq[(Seq[String], Tree)] = desc match {

      case PrimitiveDescriptor(id, _, _, _) => Seq((path, mkList(List(env.mkSelectIdx(id)))))
      case BoxedPrimitiveDescriptor(id, _, _, _, _, _) => Seq((path, mkList(List(env.mkSelectIdx(id)))))

      case BaseClassDescriptor(_, _, Seq(tag, getters @ _*), _) => {
        val tagCase = Seq((path :+ "getClass", mkList(List(env.mkSelectIdx(tag.desc.id)))))
        val fieldCases = getters flatMap { f => mkCases(f.desc, path :+ f.getter.name.toString) }
        tagCase ++ fieldCases
      }

      case CaseClassDescriptor(_, _, _, _, getters) => {
        def fieldCases = getters flatMap { f => mkCases(f.desc, path :+ f.getter.name.toString) }
        val allFieldsCase = desc match {
          case _ if desc.isPrimitiveProduct => {
            val nonRest = fieldCases filter { case (p, _) => p.size == path.size + 1 } map { _._2 }
            Seq((path, nonRest.reduceLeft((z, f) => Apply(Select(z, "$plus$plus"), List(f)))))
          }
          case _ => Seq()
        }
        allFieldsCase ++ fieldCases
      }
      case _ => Seq()
    }

    def mkPat(path: Seq[String]): Tree = {

      val seqUnapply = TypeApply(mkSelect("scala", "collection", "Seq", "unapplySeq"), List(TypeTree(typeOf[String])))
      val fun = Apply(seqUnapply, List(Ident(nme.WILDCARD)))
      
      val args = path map {
        case null => Bind(newTermName("rest"), Star(Ident(newTermName("_"))))
        case s => Literal(Constant(s))
      }

      UnApply(fun, args.toList)
    }

    mkMethod("getFieldIndex", Flag.FINAL, List(("selection", mkSeqOf(typeOf[String]))), mkListOf(typeOf[Int]), {
//    mkMethod("getFieldIndex", Flag.OVERRIDE | Flag.FINAL, List(("selection", mkSeqOf(typeOf[String]))), mkListOf(typeOf[Int]), {
      val cases = mkCases(desc, Seq()) map { case (path, idxs) => CaseDef(mkPat(path), EmptyTree, idxs) }
//      val errCase = CaseDef(Ident("_"), EmptyTree, Apply(Ident(newTermName("println")), List(Ident("selection"))))
      val errCase = CaseDef(Ident("_"), EmptyTree, (reify {throw new RuntimeException("Invalid selection")}).tree )
//      Match(Ident("selection"), (cases :+ errCase).toList)
      Match(Ident("selection"), List(errCase))
    })
  }

  protected case class GenEnvironment(listImpls: Map[Int, Type], idxPrefix: String, reentrant: Boolean, allowRecycling: Boolean, chkIndex: Boolean, chkNull: Boolean) {
    private def isNullable(tpe: Type) = typeOf[Null] <:< tpe && tpe <:< typeOf[AnyRef]

    def mkChkNotNull(source: Tree, tpe: Type): Tree = if (isNullable(tpe) && chkNull) Apply(Select(source, "$bang$eq": TermName), List(mkNull)) else EmptyTree
    def mkChkIdx(fieldId: Int): Tree = if (chkIndex) Apply(Select(mkSelectIdx(fieldId), "$greater$eq": TermName), List(mkZero)) else EmptyTree

    def mkSelectIdx(fieldId: Int): Tree = Ident(newTermName(idxPrefix + "Idx" + fieldId))
    def mkSelectSerializer(fieldId: Int): Tree = Ident(newTermName(idxPrefix + "Ser" + fieldId))
    def mkSelectWrapper(fieldId: Int): Tree = Ident(newTermName("w" + fieldId))
    def mkSelectMutableUdtInst(udtId: Int): Tree = Ident(newTermName("mutableUdtInst" + udtId))

    def mkCallSetMutableField(udtId: Int, setter: Symbol, source: Tree): Tree = Apply(Select(mkSelectMutableUdtInst(udtId), setter), List(source))
    def mkCallSerialize(refId: Int, source: Tree, target: Tree): Tree = Apply(Ident(newTermName("serialize" + refId)), List(source, target))
    def mkCallDeserialize(refId: Int, source: Tree): Tree = Apply(Ident(newTermName("deserialize" + refId)), List(source))

    def mkSetField(fieldId: Int, record: Tree): Tree = mkSetField(fieldId, record, mkSelectWrapper(fieldId))
    def mkSetField(fieldId: Int, record: Tree, wrapper: Tree): Tree = Apply(Select(record, "setField": TermName), List(mkSelectIdx(fieldId), wrapper))
    def mkGetFieldInto(fieldId: Int, record: Tree): Tree = mkGetFieldInto(fieldId, record, mkSelectWrapper(fieldId))
    def mkGetFieldInto(fieldId: Int, record: Tree, wrapper: Tree): Tree = Apply(Select(record, "getFieldInto": TermName), List(mkSelectIdx(fieldId), wrapper))

    def mkSetValue(fieldId: Int, value: Tree): Tree = mkSetValue(mkSelectWrapper(fieldId), value)
    def mkSetValue(wrapper: Tree, value: Tree): Tree = Apply(Select(wrapper, "setValue": TermName), List(value))
    def mkGetValue(fieldId: Int): Tree = mkGetValue(mkSelectWrapper(fieldId))
    def mkGetValue(wrapper: Tree): Tree = Apply(Select(wrapper, "getValue": TermName), List())

    def mkNotIsNull(fieldId: Int, record: Tree): Tree = Select(Apply(Select(record, "isNull": TermName), List(mkSelectIdx(fieldId))), "unary_$bang": TermName)
  }
}