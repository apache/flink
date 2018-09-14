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

import java.lang.reflect.{Field, Modifier}

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.api.scala.typeutils._
import org.apache.flink.types.Value

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps
import scala.reflect.macros.Context

@Internal
private[flink] trait TypeInformationGen[C <: Context] {
  this: MacroContextHolder[C]
  with TypeDescriptors[C]
  with TypeAnalyzer[C]
  with TreeGen[C] =>

  import c.universe._

  // This is for external calling by TypeUtils.createTypeInfo
  def mkTypeInfo[T: c.WeakTypeTag]: c.Expr[TypeInformation[T]] = {
    val desc = getUDTDescriptor(weakTypeTag[T].tpe)
    val result: c.Expr[TypeInformation[T]] = mkTypeInfo(desc)(c.WeakTypeTag(desc.tpe))
    result
  }

  // We have this for internal use so that we can use it to recursively generate a tree of
  // TypeInformation from a tree of UDTDescriptor
  def mkTypeInfo[T: c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = desc match {

    case f: FactoryTypeDescriptor => mkTypeInfoFromFactory(f)

    case cc@CaseClassDescriptor(_, tpe, _, _, _) =>
      mkCaseClassTypeInfo(cc)(c.WeakTypeTag(tpe).asInstanceOf[c.WeakTypeTag[Product]])
        .asInstanceOf[c.Expr[TypeInformation[T]]]

    case tp: TypeParameterDescriptor => mkTypeParameter(tp)

    case p : PrimitiveDescriptor => mkPrimitiveTypeInfo(p.tpe)
    case p : BoxedPrimitiveDescriptor => mkPrimitiveTypeInfo(p.tpe)

    case n: NothingDescriptor =>
      reify { new ScalaNothingTypeInfo().asInstanceOf[TypeInformation[T]] }

    case u: UnitDescriptor => reify { new UnitTypeInfo().asInstanceOf[TypeInformation[T]] }

    case e: EitherDescriptor => mkEitherTypeInfo(e)

    case e: EnumValueDescriptor => mkEnumValueTypeInfo(e)

    case tr: TryDescriptor => mkTryTypeInfo(tr)

    case o: OptionDescriptor => mkOptionTypeInfo(o)

    case a : ArrayDescriptor => mkArrayTypeInfo(a)

    case l : TraversableDescriptor => mkTraversableTypeInfo(l)

    case v : ValueDescriptor =>
      mkValueTypeInfo(v)(c.WeakTypeTag(v.tpe).asInstanceOf[c.WeakTypeTag[Value]])
        .asInstanceOf[c.Expr[TypeInformation[T]]]

    case pojo: PojoDescriptor => mkPojo(pojo)

    case javaTuple: JavaTupleDescriptor => mkJavaTuple(javaTuple)

    case d => mkGenericTypeInfo(d)
  }

  def mkTypeInfoFromFactory[T: c.WeakTypeTag](desc: FactoryTypeDescriptor)
    : c.Expr[TypeInformation[T]] = {

    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val baseClazz = c.Expr[Class[T]](Literal(Constant(desc.baseType)))

    val typeInfos = desc.params map { p => mkTypeInfo(p)(c.WeakTypeTag(p.tpe)).tree }
    val typeInfosList = c.Expr[List[TypeInformation[_]]](mkList(typeInfos.toList))

    reify {
      val factory = TypeExtractor.getTypeInfoFactory[T](baseClazz.splice)
      val genericParameters = typeInfosList.splice
        .zip(baseClazz.splice.getTypeParameters).map { case (typeInfo, typeParam) =>
          typeParam.getName -> typeInfo
        }.toMap[String, TypeInformation[_]]
      factory.createTypeInfo(tpeClazz.splice, genericParameters.asJava)
    }
  }

  def mkCaseClassTypeInfo[T <: Product : c.WeakTypeTag](
      desc: CaseClassDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))

    val genericTypeInfos = desc.tpe match {
      case TypeRef(_, _, typeParams) =>
        val typeInfos = typeParams map { tpe => mkTypeInfo(c.WeakTypeTag[T](tpe)).tree }
        c.Expr[List[TypeInformation[_]]](mkList(typeInfos))
      case _ =>
        reify { List[TypeInformation[_]]() }
    }

    val fields = desc.getters.toList map { field =>
      mkTypeInfo(field.desc)(c.WeakTypeTag(field.tpe)).tree
    }
    val fieldsExpr = c.Expr[Seq[TypeInformation[_]]](mkList(fields))
    val instance = mkCreateTupleInstance[T](desc)(c.WeakTypeTag(desc.tpe))

    val fieldNames = desc.getters map { f => Literal(Constant(f.getter.name.toString)) } toList
    val fieldNamesExpr = c.Expr[Seq[String]](mkSeq(fieldNames))
    reify {
      new CaseClassTypeInfo[T](
        tpeClazz.splice,
        genericTypeInfos.splice.toArray,
        fieldsExpr.splice,
        fieldNamesExpr.splice) {

        override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] = {
          val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
          for (i <- 0 until getArity) {
            fieldSerializers(i) = types(i).createSerializer(executionConfig)
          }

          new CaseClassSerializer[T](getTypeClass(), fieldSerializers) {
            override def createInstance(fields: Array[AnyRef]): T = {
              instance.splice
            }

            override def createSerializerInstance(
                tupleClass: Class[T],
                fieldSerializers: Array[TypeSerializer[_]]) = {
              this.getClass
                .getConstructors()(0)
                .newInstance(tupleClass, fieldSerializers)
                .asInstanceOf[CaseClassSerializer[T]]
            }
          }
        }
      }
    }
  }

  def mkEitherTypeInfo[T: c.WeakTypeTag](desc: EitherDescriptor): c.Expr[TypeInformation[T]] = {

    val eitherClass = c.Expr[Class[T]](Literal(Constant(weakTypeOf[T])))
    val leftTypeInfo = mkTypeInfo(desc.left)(c.WeakTypeTag(desc.left.tpe))
    val rightTypeInfo = mkTypeInfo(desc.right)(c.WeakTypeTag(desc.right.tpe))

    val result = q"""
      import org.apache.flink.api.scala.typeutils.EitherTypeInfo

      new EitherTypeInfo[${desc.left.tpe}, ${desc.right.tpe}, ${desc.tpe}](
        $eitherClass,
        $leftTypeInfo,
        $rightTypeInfo)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkEnumValueTypeInfo[T: c.WeakTypeTag](d: EnumValueDescriptor): c.Expr[TypeInformation[T]] = {

    val enumValueClass = c.Expr[Class[T]](Literal(Constant(weakTypeOf[T])))

    val result = q"""
      import org.apache.flink.api.scala.typeutils.EnumValueTypeInfo

      new EnumValueTypeInfo[${d.enum.typeSignature}](${d.enum}, $enumValueClass)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkTryTypeInfo[T: c.WeakTypeTag](desc: TryDescriptor): c.Expr[TypeInformation[T]] = {

    val elemTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))

    val result = q"""
      import org.apache.flink.api.scala.typeutils.TryTypeInfo

      new TryTypeInfo[${desc.elem.tpe}, ${desc.tpe}]($elemTypeInfo)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkOptionTypeInfo[T: c.WeakTypeTag](desc: OptionDescriptor): c.Expr[TypeInformation[T]] = {

    val elemTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))

    val result = q"""
      import org.apache.flink.api.scala.typeutils.OptionTypeInfo

      new OptionTypeInfo[${desc.elem.tpe}, ${desc.tpe}]($elemTypeInfo)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkTraversableTypeInfo[T: c.WeakTypeTag](
      desc: TraversableDescriptor): c.Expr[TypeInformation[T]] = {
    val collectionClass = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val elementClazz = c.Expr[Class[T]](Literal(Constant(desc.elem.tpe)))
    val elementTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))

    val cbf = q"implicitly[CanBuildFrom[${desc.tpe}, ${desc.elem.tpe}, ${desc.tpe}]]"

    val result = q"""
      import scala.collection.generic.CanBuildFrom
      import org.apache.flink.api.scala.typeutils.TraversableTypeInfo
      import org.apache.flink.api.scala.typeutils.TraversableSerializer
      import org.apache.flink.api.common.ExecutionConfig

      val elementTpe = $elementTypeInfo
      new TraversableTypeInfo($collectionClass, elementTpe) {
        def createSerializer(executionConfig: ExecutionConfig) = {
          new TraversableSerializer[${desc.tpe}, ${desc.elem.tpe}](
              elementTpe.createSerializer(executionConfig)) {
            def getCbf = implicitly[CanBuildFrom[${desc.tpe}, ${desc.elem.tpe}, ${desc.tpe}]]
          }
        }
      }
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkArrayTypeInfo[T: c.WeakTypeTag](desc: ArrayDescriptor): c.Expr[TypeInformation[T]] = {
    val arrayClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val elementClazz = c.Expr[Class[T]](Literal(Constant(desc.elem.tpe)))
    val elementTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))

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
          val elementType = elementTypeInfo.splice.asInstanceOf[TypeInformation[_]]
          val result = elementType match {
            case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
              PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.BYTE_TYPE_INFO =>
              PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.CHAR_TYPE_INFO =>
              PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.DOUBLE_TYPE_INFO =>
              PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.FLOAT_TYPE_INFO =>
              PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.INT_TYPE_INFO =>
              PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.LONG_TYPE_INFO =>
              PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.SHORT_TYPE_INFO =>
              PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO

            case BasicTypeInfo.STRING_TYPE_INFO =>
              BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO

            case _ =>
              ObjectArrayTypeInfo.getInfoFor(elementType)
          }
          result.asInstanceOf[TypeInformation[T]]
        }
    }
  }

  def mkValueTypeInfo[T <: Value : c.WeakTypeTag](
      desc: UDTDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    reify {
      new ValueTypeInfo[T](tpeClazz.splice)
    }
  }

  def mkJavaTuple[T: c.WeakTypeTag](desc: JavaTupleDescriptor): c.Expr[TypeInformation[T]] = {

    val fieldsTrees = desc.fields map { f => mkTypeInfo(f)(c.WeakTypeTag(f.tpe)).tree }

    val fieldsList = c.Expr[List[TypeInformation[_]]](mkList(fieldsTrees.toList))

    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))

    reify {
      val fields =  fieldsList.splice
      val clazz = tpeClazz.splice.asInstanceOf[Class[org.apache.flink.api.java.tuple.Tuple]]
      new TupleTypeInfo[org.apache.flink.api.java.tuple.Tuple](clazz, fields: _*)
        .asInstanceOf[TypeInformation[T]]
    }
  }


  def mkPojo[T: c.WeakTypeTag](desc: PojoDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val fieldsTrees = desc.getters map {
      f =>
        val name = c.Expr(Literal(Constant(f.name)))
        val fieldType = mkTypeInfo(f.desc)(c.WeakTypeTag(f.tpe))
        reify { (name.splice, fieldType.splice) }.tree
    }

    val fieldsList = c.Expr[List[(String, TypeInformation[_])]](mkList(fieldsTrees.toList))

    reify {
      val fields =  fieldsList.splice
      val clazz: Class[T] = tpeClazz.splice

      var traversalClazz: Class[_] = clazz
      val clazzFields = mutable.Map[String, Field]()

      var error = false
      while (traversalClazz != null) {
        for (field <- traversalClazz.getDeclaredFields) {
          if (clazzFields.contains(field.getName) && !Modifier.isStatic(field.getModifiers)) {
            println(s"The field $field is already contained in the " +
              s"hierarchy of the class $clazz. Please use unique field names throughout " +
              "your class hierarchy")
            error = true
          }
          clazzFields += (field.getName -> field)
        }
        traversalClazz = traversalClazz.getSuperclass
      }

      if (error) {
        new GenericTypeInfo(clazz)
      } else {
        val pojoFields = fields flatMap {
          case (fName, fTpe) =>
            val field = clazzFields(fName)
            if (Modifier.isTransient(field.getModifiers) || Modifier.isStatic(field.getModifiers)) {
              // ignore transient and static fields
              // the TypeAnalyzer for some reason does not always detect transient fields
              None
            } else {
              Some(new PojoField(clazzFields(fName), fTpe))
            }
        }

        new PojoTypeInfo(clazz, pojoFields.asJava)
      }
    }
  }

  def mkGenericTypeInfo[T: c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    reify {
      TypeExtractor.createTypeInfo(tpeClazz.splice).asInstanceOf[TypeInformation[T]]
    }
  }

  def mkTypeParameter[T: c.WeakTypeTag](
      typeParameter: TypeParameterDescriptor): c.Expr[TypeInformation[T]] = {

    val result = c.inferImplicitValue(
      c.weakTypeOf[TypeInformation[T]],
      silent = true,
      withMacrosDisabled =  false,
      pos = c.enclosingPosition)

    if (result.isEmpty) {
      c.error(
        c.enclosingPosition,
        s"could not find implicit value of type TypeInformation[${typeParameter.tpe}].")
    }

    c.Expr[TypeInformation[T]](result)
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
}
