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

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.java.typeutils.runtime.TupleComparatorBase
import org.apache.flink.core.memory.MemorySegment
import org.apache.flink.types.{KeyFieldOutOfBoundsException, NullKeyFieldException}

/**
 * Comparator for Case Classes. Access is different from
 * our Java Tuples so we have to treat them differently.
 */
@Internal
class CaseClassComparator[T <: Product](
    keys: Array[Int],
    scalaComparators: Array[TypeComparator[_]],
    scalaSerializers: Array[TypeSerializer[_]] )
  extends TupleComparatorBase[T](keys, scalaComparators, scalaSerializers) {

  private val extractedKeys = new Array[AnyRef](keys.length)

  // We cannot use the Clone Constructor from Scala so we have to do it manually
  def duplicate: TypeComparator[T] = {
    // ensure that the serializers are available
    instantiateDeserializationUtils()
    val result = new CaseClassComparator[T](keyPositions, comparators, serializers)
    result.privateDuplicate(this)
    result
  }

  // --------------------------------------------------------------------------------------------
  //  Comparator Methods
  // --------------------------------------------------------------------------------------------

  def hash(value: T): Int = {
    val comparator = comparators(0).asInstanceOf[TypeComparator[Any]]
    var code: Int = comparator.hash(value.productElement(keyPositions(0)))
    var i = 1
    try {
      while(i < keyPositions.length) {
          code *= TupleComparatorBase.HASH_SALT(i & 0x1F)
          val comparator = comparators(i).asInstanceOf[TypeComparator[Any]]
          code += comparator.hash(value.productElement(keyPositions(i)))
        i += 1
      }
    } catch {
      case npex: NullPointerException =>
        throw new NullKeyFieldException(keyPositions(i))
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
    code
  }

  def setReference(toCompare: T) {
    var i = 0
    try {
      while(i < keyPositions.length) {
        val comparator = comparators(i).asInstanceOf[TypeComparator[Any]]
        comparator.setReference(toCompare.productElement(keyPositions(i)))
        i += 1
      }
    } catch {
      case npex: NullPointerException =>
        throw new NullKeyFieldException(keyPositions(i))
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
  }

  def equalToReference(candidate: T): Boolean = {
    var i = 0
    try {
      while(i < keyPositions.length) {
        val comparator = comparators(i).asInstanceOf[TypeComparator[Any]]
        if (!comparator.equalToReference(candidate.productElement(keyPositions(i)))) {
          return false
        }
        i += 1
      }
    } catch {
      case npex: NullPointerException =>
        throw new NullKeyFieldException(keyPositions(i))
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
    true
  }

  def compare(first: T, second: T): Int = {
    var i = 0
    try {
      while(i < keyPositions.length) {
        val keyPos: Int = keyPositions(i)
        val comparator = comparators(i).asInstanceOf[TypeComparator[Any]]
        val cmp: Int = comparator.compare(
          first.productElement(keyPos),
          second.productElement(keyPos))
        if (cmp != 0) {
          return cmp
        }
        i += 1
      }
    } catch {
      case npex: NullPointerException =>
        throw new NullKeyFieldException(keyPositions(i))
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
    0
  }

  def putNormalizedKey(value: T, target: MemorySegment, offsetParam: Int, numBytesParam: Int) {
    var numBytes = numBytesParam
    var offset = offsetParam
    var i: Int = 0
    try {
      while (i < numLeadingNormalizableKeys && numBytes > 0) {
        {
          var len: Int = normalizedKeyLengths(i)
          len = if (numBytes >= len) len else numBytes
          val comparator = comparators(i).asInstanceOf[TypeComparator[Any]]
          comparator.putNormalizedKey(value.productElement(keyPositions(i)), target, offset, len)
          numBytes -= len
          offset += len
        }
        i += 1
      }
    } catch {
      case npex: NullPointerException => throw new NullKeyFieldException(keyPositions(i))
    }
  }

  def extractKeys(value: AnyRef, target: Array[AnyRef], index: Int) = {
    val in = value.asInstanceOf[T]

    var localIndex: Int = index
    var i = 0
    while (i < comparators.length) {
      localIndex += comparators(i).extractKeys(
        in.productElement(keyPositions(i)),
        target,
        localIndex)

      i += 1
    }

    localIndex - index
  }
}
