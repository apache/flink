/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.AtomicType
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor
import org.apache.flink.api.java.operators.Keys.ExpressionKeys
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.common.typeutils.{CompositeType, TypeComparator}

/**
 * TypeInformation for Case Classes. Creation and access is different from
 * our Java Tuples so we have to treat them differently.
 */
abstract class CaseClassTypeInfo[T <: Product](
    clazz: Class[T],
    fieldTypes: Seq[TypeInformation[_]],
    val fieldNames: Seq[String])
  extends TupleTypeInfoBase[T](clazz, fieldTypes: _*) {

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

  override protected def getNewComparator: TypeComparator[T] = {
    val finalLogicalKeyFields = logicalKeyFields.take(comparatorHelperIndex)
    val finalComparators = fieldComparators.take(comparatorHelperIndex)
    val maxKey = finalLogicalKeyFields.max

    // create serializers only up to the last key, fields after that are not needed
    val fieldSerializers = types.take(maxKey + 1).map(_.createSerializer)
    new CaseClassComparator[T](finalLogicalKeyFields, finalComparators, fieldSerializers.toArray)
  }

  override def getKey(
      fieldExpression: String,
      offset: Int,
      result: java.util.List[FlatFieldDescriptor]): Unit = {

    if (fieldExpression == ExpressionKeys.SELECT_ALL_CHAR) {
      var keyPosition = 0
      for (tpe <- types) {
        tpe match {
          case a: AtomicType[_] =>
            result.add(new CompositeType.FlatFieldDescriptor(offset + keyPosition, tpe))

          case co: CompositeType[_] =>
            co.getKey(ExpressionKeys.SELECT_ALL_CHAR, offset + keyPosition, result)
            keyPosition += co.getTotalFields - 1

          case _ => throw new RuntimeException(s"Unexpected key type: $tpe")

        }
        keyPosition += 1
      }
      return
    }

    if (fieldExpression == null || fieldExpression.length <= 0) {
      throw new IllegalArgumentException("Field expression must not be empty.")
    }

    fieldExpression.split('.').toList match {
      case headField :: Nil =>
        var fieldId = 0
        for (i <- 0 until fieldNames.length) {
          fieldId += types(i).getTotalFields - 1

          if (fieldNames(i) == headField) {
            if (fieldTypes(i).isInstanceOf[CompositeType[_]]) {
              throw new IllegalArgumentException(
                s"The specified field '$fieldExpression' is refering to a composite type.\n"
                + s"Either select all elements in this type with the " +
                  s"'${ExpressionKeys.SELECT_ALL_CHAR}' operator or specify a field in" +
                  s" the sub-type")
            }
            result.add(new CompositeType.FlatFieldDescriptor(offset + fieldId, fieldTypes(i)))
            return
          }

          fieldId += 1
        }
      case firstField :: rest =>
        var fieldId = 0
        for (i <- 0 until fieldNames.length) {

          if (fieldNames(i) == firstField) {
            fieldTypes(i) match {
              case co: CompositeType[_] =>
                co.getKey(rest.mkString("."), offset + fieldId, result)
                return

              case _ =>
                throw new RuntimeException(s"Field ${fieldTypes(i)} is not a composite type.")

            }
          }

          fieldId += types(i).getTotalFields
        }
    }

    throw new RuntimeException(s"Unable to find field $fieldExpression in type $this.")
  }

  override def toString = clazz.getSimpleName + "(" + fieldNames.zip(types).map {
    case (n, t) => n + ": " + t}
    .mkString(", ") + ")"
}
