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

import java.util.regex.{Pattern, Matcher}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.AtomicType
import org.apache.flink.api.common.typeutils.CompositeType.InvalidFieldReferenceException
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.operators.Keys.ExpressionKeys
import org.apache.flink.api.java.typeutils.{TupleTypeInfoBase, PojoTypeInfo}
import PojoTypeInfo.NamedFlatFieldDescriptor

import scala.collection.JavaConverters._

/**
 * TypeInformation for Case Classes. Creation and access is different from
 * our Java Tuples so we have to treat them differently.
 */
abstract class CaseClassTypeInfo[T <: Product](
    clazz: Class[T],
    typeParamTypeInfos: Array[TypeInformation[_]],
    fieldTypes: Seq[TypeInformation[_]],
    val fieldNames: Seq[String])
  extends TupleTypeInfoBase[T](clazz, fieldTypes: _*) {

  override def getGenericParameters: java.util.List[TypeInformation[_]] = {
    typeParamTypeInfos.toList.asJava
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

  def getFieldIndices(fields: Array[String]): Array[Int] = {
    fields map { x => fieldNames.indexOf(x) }
  }

  /*
   * Comparator construction
   */
  var fieldComparators: Array[TypeComparator[_]] = null
  var logicalKeyFields : Array[Int] = null
  var comparatorHelperIndex = 0

  override protected def initializeNewComparator(localKeyCount: Int): Unit = {
    fieldComparators = new Array(localKeyCount)
    logicalKeyFields = new Array(localKeyCount)
    comparatorHelperIndex = 0
  }

  override protected def addCompareField(fieldId: Int, comparator: TypeComparator[_]): Unit = {
    fieldComparators(comparatorHelperIndex) = comparator
    logicalKeyFields(comparatorHelperIndex) = fieldId
    comparatorHelperIndex += 1
  }

  override protected def getNewComparator(executionConfig: ExecutionConfig): TypeComparator[T] = {
    val finalLogicalKeyFields = logicalKeyFields.take(comparatorHelperIndex)
    val finalComparators = fieldComparators.take(comparatorHelperIndex)
    val maxKey = finalLogicalKeyFields.max

    // create serializers only up to the last key, fields after that are not needed
    val fieldSerializers = types.take(maxKey + 1).map(_.createSerializer(executionConfig))
    new CaseClassComparator[T](finalLogicalKeyFields, finalComparators, fieldSerializers.toArray)
  }

  override def getFlatFields(
      fieldExpression: String,
      offset: Int,
      result: java.util.List[FlatFieldDescriptor]): Unit = {
    val matcher: Matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression)
    if (!matcher.matches) {
      throw new InvalidFieldReferenceException("Invalid tuple field reference \"" +
        fieldExpression + "\".")
    }

    var field: String = matcher.group(0)
    if ((field == ExpressionKeys.SELECT_ALL_CHAR) ||
      (field == ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
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
      return
    } else {
      field = matcher.group(1)
    }

    val intFieldMatcher = PATTERN_INT_FIELD.matcher(field)
    if(intFieldMatcher.matches()) {
      // convert 0-indexed integer field into 1-indexed name field
      field = "_" + (Integer.valueOf(field) + 1)
    }

    var pos = offset
    val tail = matcher.group(3)
    if (tail == null) {

      for (i <- 0 until fieldNames.length) {
        if (field == fieldNames(i)) {
          // found field
          fieldTypes(i) match {
            case ct: CompositeType[_] =>
              ct.getFlatFields("*", pos, result)
              return
            case _ =>
              result.add(new FlatFieldDescriptor(pos, fieldTypes(i)))
              return
          }
        } else {
          // skipping over non-matching fields
          pos += fieldTypes(i).getTotalFields
        }
      }
      throw new InvalidFieldReferenceException("Unable to find field \"" + field +
        "\" in type " + this + ".")
    } else {
      var pos = offset
      for (i <- 0 until fieldNames.length) {
        if (field == fieldNames(i)) {
          // found field
          fieldTypes(i) match {
            case ct: CompositeType[_] =>
              ct.getFlatFields(tail, pos, result)
              return
            case _ =>
              throw new InvalidFieldReferenceException("Nested field expression \"" + tail +
                "\" not possible on atomic type " + fieldTypes(i) + ".")
          }
        } else {
          // skipping over non-matching fields
          pos += fieldTypes(i).getTotalFields
        }
      }
      throw new InvalidFieldReferenceException("Unable to find field \"" + field +
        "\" in type " + this + ".")
    }
  }

  override def getTypeAt[X](fieldExpression: String) : TypeInformation[X] = {

    val matcher: Matcher = PATTERN_NESTED_FIELDS.matcher(fieldExpression)
    if (!matcher.matches) {
      if (fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR) ||
        fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
        throw new InvalidFieldReferenceException("Wildcard expressions are not allowed here.")
      }
      else {
        throw new InvalidFieldReferenceException("Invalid format of case class field expression \""
          + fieldExpression + "\".")
      }
    }

    var field = matcher.group(1)
    val tail = matcher.group(3)

    val intFieldMatcher = PATTERN_INT_FIELD.matcher(field)
    if(intFieldMatcher.matches()) {
      // convert 0-indexed integer field into 1-indexed name field
      field = "_" + (Integer.valueOf(field) + 1)
    }

    for (i <- 0 until fieldNames.length) {
      if (fieldNames(i) == field) {
        if (tail == null) {
          return getTypeAt(i)
        } else {
          fieldTypes(i) match {
            case co: CompositeType[_] =>
              return co.getTypeAt(tail)
            case _ =>
              throw new InvalidFieldReferenceException("Nested field expression \"" + tail +
                "\" not possible on atomic type " + fieldTypes(i) + ".")
          }
        }
      }
    }
    throw new InvalidFieldReferenceException("Unable to find field \"" + field +
      "\" in type " + this + ".")
  }

  override def getFieldNames: Array[String] = fieldNames.toArray

  override def getFieldIndex(fieldName: String): Int = {
    val result = fieldNames.indexOf(fieldName)
    if (result != fieldNames.lastIndexOf(fieldName)) {
      -2
    } else {
      result
    }
  }

  override def toString = clazz.getName + "(" + fieldNames.zip(types).map {
    case (n, t) => n + ": " + t}
    .mkString(", ") + ")"
}
