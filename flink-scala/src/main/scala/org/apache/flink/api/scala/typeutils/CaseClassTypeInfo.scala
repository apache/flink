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
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}

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
    var maxKey: Int = 0
    for (key <- finalLogicalKeyFields) {
      maxKey = Math.max(maxKey, key)
    }
    val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](maxKey + 1)

    for (i <- 0 to maxKey) {
      fieldSerializers(i) = types(i).createSerializer
    }
    new CaseClassComparator[T](finalLogicalKeyFields, finalComparators, fieldSerializers)
  }

  override def toString = clazz.getSimpleName + "(" + fieldNames.zip(types).map {
    case (n, t) => n + ": " + t}
    .mkString(", ") + ")"
}
