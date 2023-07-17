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
package org.apache.flink.api.scala.typeutils

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.operators.Keys
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.common.typeutils.CompositeType.{FlatFieldDescriptor, InvalidFieldReferenceException, TypeComparatorBuilder}
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase

import Keys.ExpressionKeys

import java.util
import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * TypeInformation for Case Classes. Creation and access is different from our Java Tuples so we
 * have to treat them differently.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@Public
abstract class CaseClassTypeInfo[T <: Product](
    clazz: Class[T],
    val typeParamTypeInfos: Array[TypeInformation[_]],
    fieldTypes: Seq[TypeInformation[_]],
    val fieldNames: Seq[String])
  extends TupleTypeInfoBase[T](clazz, fieldTypes: _*) {

  @PublicEvolving
  override def getGenericParameters: java.util.Map[String, TypeInformation[_]] = {
    typeParamTypeInfos.zipWithIndex
      .map {
        case (info, index) =>
          "T" + (index + 1) -> info
      }
      .toMap[String, TypeInformation[_]]
      .asJava
  }

  private val REGEX_INT_FIELD: String = "[0-9]+"
  private val REGEX_STR_FIELD: String = "[\\p{L}_\\$][\\p{L}\\p{Digit}_\\$]*"
  private val REGEX_FIELD: String = REGEX_STR_FIELD + "|" + REGEX_INT_FIELD
  private val REGEX_NESTED_FIELDS: String = "(" + REGEX_FIELD + ")(\\.(.+))?"
  private val REGEX_NESTED_FIELDS_WILDCARD: String = REGEX_NESTED_FIELDS + "|\\" +
    ExpressionKeys.SELECT_ALL_CHAR + "|\\" + ExpressionKeys.SELECT_ALL_CHAR_SCALA

  private val PATTERN_NESTED_FIELDS: Pattern = Pattern.compile(REGEX_NESTED_FIELDS)
  private val PATTERN_NESTED_FIELDS_WILDCARD: Pattern =
    Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD)
  private val PATTERN_INT_FIELD: Pattern = Pattern.compile(REGEX_INT_FIELD)

  @PublicEvolving
  def getFieldIndices(fields: Array[String]): Array[Int] = {
    fields.map(x => fieldNames.indexOf(x))
  }

  @PublicEvolving
  override def getFlatFields(
      fieldExpression: String,
      offset: Int,
      result: java.util.List[FlatFieldDescriptor]): Unit = {
    val matcher: Matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression)

    if (!matcher.matches) {
      throw new InvalidFieldReferenceException(
        "Invalid tuple field reference \"" +
          fieldExpression + "\".")
    }

    var field: String = matcher.group(0)

    if (
      (field == ExpressionKeys.SELECT_ALL_CHAR) ||
      (field == ExpressionKeys.SELECT_ALL_CHAR_SCALA)
    ) {
      var keyPosition: Int = 0
      for (fType <- fieldTypes) {
        fType match {
          case ct: CompositeType[_] =>
            ct.getFlatFields(ExpressionKeys.SELECT_ALL_CHAR, offset + keyPosition, result)
            keyPosition += ct.getTotalFields - 1
          case _ =>
            result.add(new FlatFieldDescriptor(offset + keyPosition, fType))
        }
        keyPosition += 1
      }
    } else {
      field = matcher.group(1)

      val intFieldMatcher = PATTERN_INT_FIELD.matcher(field)
      if (intFieldMatcher.matches()) {
        // convert 0-indexed integer field into 1-indexed name field
        field = "_" + (Integer.valueOf(field) + 1)
      }

      val tail = matcher.group(3)

      if (tail == null) {
        def extractFlatFields(index: Int, pos: Int): Unit = {
          if (index >= fieldNames.size) {
            throw new InvalidFieldReferenceException(
              "Unable to find field \"" + field +
                "\" in type " + this + ".")
          } else if (field == fieldNames(index)) {
            // found field
            fieldTypes(index) match {
              case ct: CompositeType[_] =>
                ct.getFlatFields("*", pos, result)
              case _ =>
                result.add(new FlatFieldDescriptor(pos, fieldTypes(index)))
            }
          } else {
            // skipping over non-matching fields
            extractFlatFields(index + 1, pos + fieldTypes(index).getTotalFields())
          }
        }

        extractFlatFields(0, offset)
      } else {
        def extractFlatFields(index: Int, pos: Int): Unit = {
          if (index >= fieldNames.size) {
            throw new InvalidFieldReferenceException(
              "Unable to find field \"" + field +
                "\" in type " + this + ".")
          } else if (field == fieldNames(index)) {
            // found field
            fieldTypes(index) match {
              case ct: CompositeType[_] =>
                ct.getFlatFields(tail, pos, result)
              case _ =>
                throw new InvalidFieldReferenceException(
                  "Nested field expression \"" + tail +
                    "\" not possible on atomic type " + fieldTypes(index) + ".")
            }
          } else {
            extractFlatFields(index + 1, pos + fieldTypes(index).getTotalFields())
          }
        }

        extractFlatFields(0, offset)
      }
    }
  }

  @PublicEvolving
  override def getTypeAt[X](fieldExpression: String): TypeInformation[X] = {

    val matcher: Matcher = PATTERN_NESTED_FIELDS.matcher(fieldExpression)
    if (!matcher.matches) {
      if (
        fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR) ||
        fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR_SCALA)
      ) {
        throw new InvalidFieldReferenceException("Wildcard expressions are not allowed here.")
      } else {
        throw new InvalidFieldReferenceException(
          "Invalid format of case class field expression \""
            + fieldExpression + "\".")
      }
    }

    var field = matcher.group(1)
    val tail = matcher.group(3)

    val intFieldMatcher = PATTERN_INT_FIELD.matcher(field)
    if (intFieldMatcher.matches()) {
      // convert 0-indexed integer field into 1-indexed name field
      field = "_" + (Integer.valueOf(field) + 1)
    }

    for (i <- fieldNames.indices) {
      if (fieldNames(i) == field) {
        if (tail == null) {
          return getTypeAt(i)
        } else {
          fieldTypes(i) match {
            case co: CompositeType[_] =>
              return co.getTypeAt(tail)
            case _ =>
              throw new InvalidFieldReferenceException(
                "Nested field expression \"" + tail +
                  "\" not possible on atomic type " + fieldTypes(i) + ".")
          }
        }
      }
    }
    throw new InvalidFieldReferenceException(
      "Unable to find field \"" + field +
        "\" in type " + this + ".")
  }

  @PublicEvolving
  override def getFieldNames: Array[String] = fieldNames.toArray

  @PublicEvolving
  override def getFieldIndex(fieldName: String): Int = {
    val result = fieldNames.indexOf(fieldName)
    if (result != fieldNames.lastIndexOf(fieldName)) {
      -1
    } else {
      result
    }
  }

  @PublicEvolving
  override def createTypeComparatorBuilder(): TypeComparatorBuilder[T] = {
    new CaseClassTypeComparatorBuilder
  }

  private class CaseClassTypeComparatorBuilder extends TypeComparatorBuilder[T] {
    val fieldComparators: ArrayBuffer[TypeComparator[_]] = new ArrayBuffer[TypeComparator[_]]()
    val logicalKeyFields: ArrayBuffer[Int] = new ArrayBuffer[Int]()

    override def initializeTypeComparatorBuilder(size: Int): Unit = {
      fieldComparators.sizeHint(size)
      logicalKeyFields.sizeHint(size)
    }

    override def addComparatorField(fieldId: Int, comparator: TypeComparator[_]): Unit = {
      fieldComparators += comparator
      logicalKeyFields += fieldId
    }

    override def createTypeComparator(config: ExecutionConfig): TypeComparator[T] = {
      val maxIndex = logicalKeyFields.max

      new CaseClassComparator[T](
        logicalKeyFields.toArray,
        fieldComparators.toArray,
        types.take(maxIndex + 1).map(_.createSerializer(config))
      )
    }
  }

  override def toString: String = {
    clazz.getName + "(" + fieldNames
      .zip(types)
      .map { case (n, t) => n + ": " + t }
      .mkString(", ") + ")"
  }

  override def isCaseClass = true

  override def equals(obj: Any): Boolean = {
    obj match {
      case caseClass: CaseClassTypeInfo[_] =>
        caseClass.canEqual(this) &&
        super.equals(caseClass) &&
        typeParamTypeInfos.sameElements(caseClass.typeParamTypeInfos) &&
        fieldNames.equals(caseClass.fieldNames)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    31 * (31 * super.hashCode() + fieldNames.hashCode()) +
      util.Arrays.hashCode(typeParamTypeInfos.asInstanceOf[Array[AnyRef]])
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[CaseClassTypeInfo[_]]
  }
}
