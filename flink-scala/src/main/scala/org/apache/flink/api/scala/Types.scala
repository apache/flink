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

package org.apache.flink.api.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types => JTypes}
import org.apache.flink.api.scala.typeutils._

import _root_.scala.collection.JavaConverters._
import _root_.scala.util.Either
import _root_.scala.util.Try

/**
  * This class gives access to the type information of the most common Scala types for which Flink
  * has built-in serializers and comparators.
  *
  * This class contains types of [[org.apache.flink.api.common.typeinfo.Types]] and adds
  * types for Scala specific classes (such as [[Unit]] or case classes).
  *
  * In many cases, Flink tries to analyze generic signatures of functions to determine return
  * types automatically. This class is intended for cases where the extraction is not possible
  * (or inefficient) as well as cases where type information has to be supplied manually.
  *
  * Depending on the API you are using (e.g. Java API or Table API), there might be a more
  * specialized `Types` class.
  *
  * Scala macros allow to determine type information of classes and type parameters. You can
  * use [[Types.of]] to let type information this feature.
  */
@PublicEvolving
object Types {

  /**
    * Generates type information based on the given class and/or its type parameters.
    *
    * The definition is similar to a [[org.apache.flink.api.common.typeinfo.TypeHint]] but does
    * not require to implement anonymous classes.
    *
    * If the class could not be analyzed by the Scala type analyzer, the Java analyzer
    * will be used.
    *
    * Example use:
    *
    * `Types.of[(Int, String, String)]` for Scala tuples
    * `Types.of[Unit]` for Scala specific types
    *
    * @tparam T class to be analyzed
    */
  def of[T: TypeInformation]: TypeInformation[_] = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    typeInfo
  }

  /**
    * Returns type information for Scala [[Nothing]]. Does not support a null value.
    */
  val NOTHING: TypeInformation[_] = new ScalaNothingTypeInfo

  /**
    * Returns type information for Scala [[Unit]]. Does not support a null value.
    */
  val UNIT: TypeInformation[_] = new UnitTypeInfo

  /**
    * Returns type information for [[String]] and [[java.lang.String]]. Supports a null value.
    */
  val STRING: TypeInformation[_] = JTypes.STRING

  /**
    * Returns type information for primitive [[Byte]] and [[java.lang.Byte]]. Does not
    * support a null value.
    */
  val BYTE: TypeInformation[_] = JTypes.BYTE

  /**
    * Returns type information for primitive [[Boolean]] and [[java.lang.Boolean]]. Does not
    * support a null value.
    */
  val BOOLEAN: TypeInformation[_] = JTypes.BOOLEAN

  /**
    * Returns type information for primitive [[Short]] and [[java.lang.Short]]. Does not
    * support a null value.
    */
  val SHORT: TypeInformation[_] = JTypes.SHORT

  /**
    * Returns type information for primitive [[Int]] and [[java.lang.Integer]]. Does not
    * support a null value.
    */
  val INT: TypeInformation[_] = JTypes.INT

  /**
    * Returns type information for primitive [[Long]] and [[java.lang.Long]]. Does not
    * support a null value.
    */
  val LONG: TypeInformation[_] = JTypes.LONG

  /**
    * Returns type information for primitive [[Float]] and [[java.lang.Float]]. Does not
    * support a null value.
    */
  val FLOAT: TypeInformation[_] = JTypes.FLOAT

  /**
    * Returns type information for primitive [[Double]] and [[java.lang.Double]]. Does not
    * support a null value.
    */
  val DOUBLE: TypeInformation[_] = JTypes.DOUBLE

  /**
    * Returns type information for primitive [[Char]] and [[java.lang.Character]]. Does not
    * support a null value.
    */
  val CHAR: TypeInformation[_] = JTypes.CHAR

  /**
    * Returns type information for Java [[java.math.BigDecimal]]. Supports a null value.
    *
    * Note that Scala [[BigDecimal]] is not supported yet.
    */
  val JAVA_BIG_DEC: TypeInformation[_] = JTypes.BIG_DEC

  /**
    * Returns type information for Java [[java.math.BigInteger]]. Supports a null value.
    *
    * Note that Scala [[BigInt]] is not supported yet.
    */
  val JAVA_BIG_INT: TypeInformation[_] = JTypes.BIG_INT

  /**
    * Returns type information for [[java.sql.Date]]. Supports a null value.
    */
  val SQL_DATE: TypeInformation[_] = JTypes.SQL_DATE

  /**
    * Returns type information for [[java.sql.Time]]. Supports a null value.
    */
  val SQL_TIME: TypeInformation[_] = JTypes.SQL_TIME

  /**
    * Returns type information for [[java.sql.Timestamp]]. Supports a null value.
    */
  val SQL_TIMESTAMP: TypeInformation[_] = JTypes.SQL_TIMESTAMP

  /**
    * Returns type information for [[org.apache.flink.types.Row]] with fields of the given types.
    * A row itself must not be null.
    *
    * A row is a variable-length, null-aware composite type for storing multiple values in a
    * deterministic field order. Every field can be null independent of the field's type.
    * Fields of a row are untyped; therefore, it is required to pass type information whenever
    * a row is used.
    *
    * This method generates type information with fields of the given types; the fields have
    * the default names (f0, f1, f2 ..).
    *
    * @param types The types of the row fields, e.g., Types.STRING, Types.INT
    */
  def ROW(types: TypeInformation[_]*): TypeInformation[_] = JTypes.ROW(types: _*)

  /**
    * Returns type information for [[org.apache.flink.types.Row]] with fields of the given types
    * and with given names. A row must not be null.
    *
    * A row is a variable-length, null-aware composite type for storing multiple values in a
    * deterministic field order. Every field can be null independent of the field's type.
    * Fields of a row are untyped; therefore, it is required to pass type information whenever
    * a row is used.
    *
    * Example use: `Types.ROW(Array("name", "number"), Array(Types.STRING, Types.INT))`.
    *
    * @param fieldNames array of field names
    * @param types      array of field types
    */
  def ROW(fieldNames: Array[String], types: Array[TypeInformation[_]]): TypeInformation[_] =
    JTypes.ROW_NAMED(fieldNames, types: _*)

  /**
    * Returns type information for a POJO (Plain Old Java Object).
    *
    * A POJO class is public and standalone (no non-static inner class). It has a public
    * no-argument constructor. All non-static, non-transient fields in the class (and all
    * superclasses) are either public (and non-final) or have a public getter and a setter
    * method that follows the Java beans naming conventions for getters and setters.
    *
    * A POJO is a fixed-length, null-aware composite type with non-deterministic field order.
    * Every field can be null independent of the field's type.
    *
    * Types for all fields of the POJO can be defined in a hierarchy of subclasses.
    *
    * If Flink's type analyzer is unable to extract a valid POJO type information with
    * type information for all fields, an
    * [[org.apache.flink.api.common.functions.InvalidTypesException}]] is thrown.
    *
    * @param pojoClass POJO class to be analyzed by Flink
    */
  def POJO(pojoClass: Class[_]): TypeInformation[_] = {
    JTypes.POJO(pojoClass)
  }

  /**
    * Returns type information for a POJO (Plain Old Java Object) and allows to specify all
    * fields manually.
    *
    * A POJO class is public and standalone (no non-static inner class). It has a public no-argument
    * constructor. All non-static, non-transient fields in the class (and all superclasses) are
    * either public (and non-final) or have a public getter and a setter method that follows the
    * Java beans naming conventions for getters and setters.
    *
    * A POJO is a fixed-length, null-aware composite type with non-deterministic field order.
    * Every field can be null independent of the field's type.
    *
    * Types for all fields of the POJO can be defined in a hierarchy of subclasses.
    *
    * If Flink's type analyzer is unable to extract a POJO field, an
    * [[org.apache.flink.api.common.functions.InvalidTypesException]] is thrown.
    *
    * '''Note:''' In most cases the type information of fields can be determined automatically,
    * we recommend to use [[Types.POJO(Class)]].
    *
    * @param pojoClass POJO class
    * @param fields    map of fields that map a name to type information
    */
  def POJO(pojoClass: Class[_], fields: Map[String, TypeInformation[_]]): TypeInformation[_] = {
    JTypes.POJO(pojoClass, fields.asJava)
  }

  /**
    * Returns generic type information for any Scala/Java object. The serialization logic will
    * use the general purpose serializer Kryo.
    *
    * Generic types are black-boxes for Flink, but allow any object and null values in fields.
    *
    * By default, serialization of this type is not very efficient. Please read the documentation
    * about how to improve efficiency (namely by pre-registering classes).
    *
    * @param genericClass any Scala/Java class
    */
  def GENERIC(genericClass: Class[_]): TypeInformation[_] = JTypes.GENERIC(genericClass)

  /**
    * Returns type information for a Scala case class and Scala tuples.
    *
    * A Scala case class is a fixed-length composite type for storing multiple values in a
    * deterministic field order. Fields of a case class are typed. Case classes and tuples are
    * the most efficient composite type; therefore, they do not support null values unless
    * its field type supports nullability.
    *
    * Example use: `Types.CASE_CLASS[MyCaseClass]`
    *
    * @tparam T case class to be analyzed
    */
  def CASE_CLASS[T: TypeInformation]: TypeInformation[_] = {
    val t = Types.of[T]
    if (t.isInstanceOf[CaseClassTypeInfo[_]]) {
      t
    } else {
      throw new InvalidTypesException("Case class type expected but was: " + t)
    }
  }

  /**
    * Returns type information for Scala/Java arrays of primitive type (such as `Array[Byte]`).
    * The array and its elements do not support null values.
    *
    * @param elementType element type of the array (e.g. Types.BOOLEAN, Types.INT, Types.DOUBLE)
    */
  def PRIMITIVE_ARRAY(elementType: TypeInformation[_]): TypeInformation[_] = {
    JTypes.PRIMITIVE_ARRAY(elementType)
  }

  /**
    * Returns type information for Scala/Java arrays of object types (such as `Array[String]`,
    * `Array[java.lang.Integer]`). The array itself must not be null. Null values for elements
    * are supported.
    *
    * @param elementType element type of the array
    */
  def OBJECT_ARRAY(elementType: TypeInformation[_]): TypeInformation[_] = {
    JTypes.OBJECT_ARRAY(elementType)
  }

  /**
    * Returns type information for Scala [[Either]] type. Null values are not supported.
    *
    * The either type can be used for a value of two possible types.
    *
    * Example use: `Types.EITHER(Types.INT, Types.NOTHING]`
    *
    * @param leftType type information of left side / [[Left]]
    * @param rightType type information of right side / [[Right]]
    */
  def EITHER(leftType: TypeInformation[_], rightType: TypeInformation[_]): TypeInformation[_] = {
    new EitherTypeInfo(classOf[Either[_, _]], leftType, rightType)
  }

  /**
    * Returns type information for Scala [[Option]] type. Null values are not supported.
    *
    * The option type can be used for distinguishing between a value or no value.
    *
    * Example use: `Types.OPTION(Types.INT)`
    *
    * @param valueType type information of the option's value
    */
  def OPTION(valueType: TypeInformation[_]): TypeInformation[_] = {
    new OptionTypeInfo(valueType)
  }

  /**
    * Returns type information for Scala [[Try]] type. Null values are not supported.
    *
    * The try type can be used for distinguishing between a value or throwable.
    *
    * Example use: `Types.TRY(Types.INT)`
    *
    * @param valueType type information of the try's value
    */
  def TRY(valueType: TypeInformation[_]): TypeInformation[_] = {
    new TryTypeInfo(valueType)
  }

  /**
    * Returns type information for Scala enumerations. Null values are not supported.
    *
    * @param enum enumeration object
    * @param valueClass value class
    */
  def ENUMERATION[E <: Enumeration](enum: E, valueClass: Class[E#Value]): TypeInformation[_] = {
    new EnumValueTypeInfo(enum, valueClass)
  }

  /**
    * Returns type information for Scala collections that implement [[Traversable]].
    */
  def TRAVERSABLE[T: TypeInformation]: TypeInformation[_] = {
    val t = Types.of[T]
    if (t.isInstanceOf[TraversableTypeInfo[_, _]]) {
      t
    } else {
      throw new InvalidTypesException("Traversable type expected but was: " + t)
    }
  }
}
