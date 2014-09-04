/**
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

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, AtomicType, TupleTypeInfoBase}
import org.apache.flink.types.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}

/**
 * TypeInformation for Scala Tuples. Creation and access is different from
 * our Java Tuples so we have to treat them differently.
 */
abstract class ScalaTupleTypeInfo[T <: Product](
    tupleClass: Class[T],
    fieldTypes: Seq[TypeInformation[_]])
  extends TupleTypeInfoBase[T](tupleClass, fieldTypes: _*) {

  def createComparator(logicalKeyFields: Array[Int], orders: Array[Boolean]): TypeComparator[T] = {
    // sanity checks
    if (logicalKeyFields == null || orders == null
      || logicalKeyFields.length != orders.length || logicalKeyFields.length > types.length) {
      throw new IllegalArgumentException
    }

    // No special handling of leading Key field as in JavaTupleComparator for now

    // --- general case ---
    var maxKey: Int = -1

    for (key <- logicalKeyFields) {
      maxKey = Math.max(key, maxKey)
    }

    if (maxKey >= types.length) {
      throw new IllegalArgumentException("The key position " + maxKey + " is out of range for " +
        "Tuple" + types.length)
    }

    // create the comparators for the individual fields
    val fieldComparators: Array[TypeComparator[_]] = new Array(logicalKeyFields.length)

    for (i <- 0 until logicalKeyFields.length) {
      val keyPos = logicalKeyFields(i)
      if (types(keyPos).isKeyType && types(keyPos).isInstanceOf[AtomicType[_]]) {
        fieldComparators(i) = types(keyPos).asInstanceOf[AtomicType[_]].createComparator(orders(i))
      } else {
        throw new IllegalArgumentException(
          "The field at position " + i + " (" + types(keyPos) + ") is no atomic key type.")
      }
    }

    // create the serializers for the prefix up to highest key position
    val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](maxKey + 1)

    for (i <- 0 to maxKey) {
      fieldSerializers(i) = types(i).createSerializer
    }

    new ScalaTupleComparator[T](logicalKeyFields, fieldComparators, fieldSerializers)
  }

  override def toString = "Scala " + super.toString
}
