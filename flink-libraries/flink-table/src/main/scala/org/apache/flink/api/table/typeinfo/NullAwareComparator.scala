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
package org.apache.flink.api.table.typeinfo

import org.apache.flink.api.common.typeutils.{CompositeTypeComparator, TypeComparator}
import org.apache.flink.core.memory.{DataInputView, DataOutputView, MemorySegment}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Null-aware comparator that wraps a comparator which does not support null references.
 *
 * NOTE: This class assumes to be used within a composite type comparator (such
 * as [[RowComparator]]) that handles serialized comparison.
 */
class NullAwareComparator[T](
    val wrappedComparator: TypeComparator[T],
    val order: Boolean)
  extends TypeComparator[T] {

  // number of flat fields
  private val flatFields = wrappedComparator.getFlatComparators.length

  // stores the null for reference comparison
  private var nullReference = false

  override def hash(record: T): Int = {
    if (record != null) {
      wrappedComparator.hash(record)
    }
    else {
      0
    }
  }

 override def getNormalizeKeyLen: Int = {
    val len = wrappedComparator.getNormalizeKeyLen
    if (len == Integer.MAX_VALUE) {
      Integer.MAX_VALUE
    }
    else {
      len + 1 // add one for a null byte
    }
  }

  override def putNormalizedKey(
      record: T,
      target: MemorySegment,
      offset: Int,
      numBytes: Int)
    : Unit = {
    if (numBytes > 0) {
      // write a null byte with padding
      if (record == null) {
        target.putBoolean(offset, false)
        // write padding
        var j = 0
        while (j < numBytes - 1) {
          target.put(offset + 1 + j, 0.toByte)
          j += 1
        }
      }
      // write a non-null byte with key
      else {
        target.putBoolean(offset, true)
        // write key
        wrappedComparator.putNormalizedKey(record, target, offset + 1, numBytes - 1)
      }
    }
  }

  override def invertNormalizedKey(): Boolean = wrappedComparator.invertNormalizedKey()

  override def supportsSerializationWithKeyNormalization(): Boolean = false

  override def writeWithKeyNormalization(record: T, target: DataOutputView): Unit =
    throw new UnsupportedOperationException("Record serialization with leading normalized keys" +
      " not supported.")

  override def readWithKeyDenormalization(reuse: T, source: DataInputView): T =
    throw new UnsupportedOperationException("Record deserialization with leading normalized keys" +
      " not supported.")

  override def isNormalizedKeyPrefixOnly(keyBytes: Int): Boolean =
    wrappedComparator.isNormalizedKeyPrefixOnly(keyBytes - 1)

  override def setReference(toCompare: T): Unit = {
    if (toCompare == null) {
      nullReference = true
    }
    else {
      nullReference = false
      wrappedComparator.setReference(toCompare)
    }
  }

  override def compare(first: T, second: T): Int = {
    // both values are null -> equality
    if (first == null && second == null) {
      0
    }
    // first value is null -> inequality
    // but order is considered
    else if (first == null) {
      if (order) -1 else 1
    }
    // second value is null -> inequality
    // but order is considered
    else if (second == null) {
      if (order) 1 else -1
    }
    // no null values
    else {
      wrappedComparator.compare(first, second)
    }
  }

  override def compareToReference(referencedComparator: TypeComparator[T]): Int = {
    val otherComparator = referencedComparator.asInstanceOf[NullAwareComparator[T]]
    val otherNullReference = otherComparator.nullReference
    // both values are null -> equality
    if (nullReference && otherNullReference) {
      0
    }
    // first value is null -> inequality
    // but order is considered
    else if (nullReference) {
      if (order) 1 else -1
    }
    // second value is null -> inequality
    // but order is considered
    else if (otherNullReference) {
      if (order) -1 else 1
    }
    // no null values
    else {
      wrappedComparator.compareToReference(otherComparator.wrappedComparator)
    }
  }

  override def supportsNormalizedKey(): Boolean = wrappedComparator.supportsNormalizedKey()

  override def equalToReference(candidate: T): Boolean = {
    // both values are null
    if (candidate == null && nullReference) {
      true
    }
    // one value is null
    else if (candidate == null || nullReference) {
      false
    }
    // no null value
    else {
      wrappedComparator.equalToReference(candidate)
    }
  }

  override def duplicate(): TypeComparator[T] = {
    new NullAwareComparator[T](wrappedComparator.duplicate(), order)
  }

  override def extractKeys(record: Any, target: Array[AnyRef], index: Int): Int = {
    if (record == null) {
      var i = 0
      while (i < flatFields) {
        target(index + i) = null
        i += 1
      }
      flatFields
    }
    else {
      wrappedComparator.extractKeys(record, target, index)
    }
  }


  override def getFlatComparators: Array[TypeComparator[_]] = {
    // determine the flat comparators and wrap them again in null-aware comparators
    val flatComparators = new ArrayBuffer[TypeComparator[_]]()
    wrappedComparator match {
      case ctc: CompositeTypeComparator[_] => ctc.getFlatComparator(flatComparators)
      case c: TypeComparator[_] => flatComparators += c
    }
    val wrappedComparators = flatComparators.map { c =>
      new NullAwareComparator[Any](c.asInstanceOf[TypeComparator[Any]], order)
    }
    wrappedComparators.toArray[TypeComparator[_]]
  }

  /**
   * This method is not implemented here. It must be implemented by the comparator this class
   * is contained in (e.g. RowComparator).
   *
   * @param firstSource The input view containing the first record.
   * @param secondSource The input view containing the second record.
   * @return An integer defining the oder among the objects in the same way as
   *         { @link java.util.Comparator#compare(Object, Object)}.
   */
  override def compareSerialized(firstSource: DataInputView, secondSource: DataInputView): Int =
    throw new UnsupportedOperationException("Comparator does not support null-aware serialized " +
      "comparision.")
}
