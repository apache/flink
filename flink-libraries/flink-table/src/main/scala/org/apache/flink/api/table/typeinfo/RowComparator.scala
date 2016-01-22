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

import java.util

import org.apache.flink.api.common.typeutils.{CompositeTypeComparator, TypeComparator, TypeSerializer}
import org.apache.flink.api.java.typeutils.runtime.TupleComparatorBase
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeinfo.NullMaskUtils.readIntoNullMask
import org.apache.flink.api.table.typeinfo.RowComparator.{createAuxiliaryFields, makeNullAware}
import org.apache.flink.core.memory.{DataInputView, DataOutputView, MemorySegment}
import org.apache.flink.types.KeyFieldOutOfBoundsException

/**
 * Comparator for [[Row]].
 */
class RowComparator private (
    /** key positions describe which fields are keys in what order */
    val keyPositions: Array[Int],
    /** null-aware comparators for the key fields, in the same order as the key fields */
    val comparators: Array[NullAwareComparator[Any]],
    /** serializers to deserialize the first n fields for comparison */
    val serializers: Array[TypeSerializer[Any]],
    /** auxiliary fields for normalized key support */
    private val auxiliaryFields: (Array[Int], Int, Int, Boolean))
  extends CompositeTypeComparator[Row] with Serializable {

  // null masks for serialized comparison
  private val nullMask1 = new Array[Boolean](serializers.length)
  private val nullMask2 = new Array[Boolean](serializers.length)

  // cache for the deserialized key field objects
  @transient
  private lazy val deserializedKeyFields1: Array[Any] = instantiateDeserializationFields()

  @transient
  private lazy val deserializedKeyFields2: Array[Any] = instantiateDeserializationFields()

  // create auxiliary fields
  private val normalizedKeyLengths: Array[Int] = auxiliaryFields._1
  private val numLeadingNormalizableKeys: Int = auxiliaryFields._2
  private val normalizableKeyPrefixLen: Int = auxiliaryFields._3
  private val invertNormKey: Boolean = auxiliaryFields._4

  /**
   * Intermediate constructor for creating auxiliary fields.
   */
  def this(
      keyPositions: Array[Int],
      comparators: Array[NullAwareComparator[Any]],
      serializers: Array[TypeSerializer[Any]]) = {
    this(
      keyPositions,
      comparators,
      serializers,
      createAuxiliaryFields(keyPositions, comparators))
  }

  /**
   * General constructor for RowComparator.
   *
   * @param keyPositions key positions describe which fields are keys in what order
   * @param comparators non-null-aware comparators for the key fields, in the same order as
   *   the key fields
   * @param serializers serializers to deserialize the first n fields for comparison
   * @param orders sorting orders for the fields
   */
  def this(
      keyPositions: Array[Int],
      comparators: Array[TypeComparator[Any]],
      serializers: Array[TypeSerializer[Any]],
      orders: Array[Boolean]) = {
    this(
      keyPositions,
      makeNullAware(comparators, orders),
      serializers)
  }

  private def instantiateDeserializationFields(): Array[Any] = {
    val newFields = new Array[Any](serializers.length)
    var i = 0
    while (i < serializers.length) {
      newFields(i) = serializers(i).createInstance()
      i += 1
    }
    newFields
  }

  // --------------------------------------------------------------------------------------------
  //  Comparator Methods
  // --------------------------------------------------------------------------------------------

  override def compareToReference(referencedComparator: TypeComparator[Row]): Int = {
    val other: RowComparator = referencedComparator.asInstanceOf[RowComparator]
    var i = 0
    try {
      while (i < keyPositions.length) {
        val comparator = comparators(i)
        val otherComparator = other.comparators(i)

        val cmp = comparator.compareToReference(otherComparator)
        if (cmp != 0) {
          return cmp
        }
        i = i + 1
      }
      0
    }
    catch {
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
  }

  override def compareSerialized(firstSource: DataInputView, secondSource: DataInputView): Int = {
    val len = serializers.length
    val keyLen = keyPositions.length

    readIntoNullMask(len, firstSource, nullMask1)
    readIntoNullMask(len, secondSource, nullMask2)

    // deserialize
    var i = 0
    while (i < len) {
      val serializer = serializers(i)

      // deserialize field 1
      if (!nullMask1(i)) {
        deserializedKeyFields1(i) = serializer.deserialize(deserializedKeyFields1(i), firstSource)
      }

      // deserialize field 2
      if (!nullMask2(i)) {
        deserializedKeyFields2(i) = serializer.deserialize(deserializedKeyFields2(i), secondSource)
      }

      i += 1
    }

    // compare
    i = 0
    while (i < keyLen) {
      val keyPos = keyPositions(i)
      val comparator = comparators(i)

      val isNull1 = nullMask1(keyPos)
      val isNull2 = nullMask2(keyPos)

      var cmp = 0
      // both values are null -> equality
      if (isNull1 && isNull2) {
        cmp = 0
      }
      // first value is null -> inequality
      else if (isNull1) {
        cmp = comparator.compare(null, deserializedKeyFields2(keyPos))
      }
      // second value is null -> inequality
      else if (isNull2) {
        cmp = comparator.compare(deserializedKeyFields1(keyPos), null)
      }
      // no null values
      else {
        cmp = comparator.compare(deserializedKeyFields1(keyPos), deserializedKeyFields2(keyPos))
      }

      if (cmp != 0) {
        return cmp
      }

      i += 1
    }
    0
  }

  override def supportsNormalizedKey(): Boolean = numLeadingNormalizableKeys > 0

  override def getNormalizeKeyLen: Int = normalizableKeyPrefixLen

  override def isNormalizedKeyPrefixOnly(keyBytes: Int): Boolean =
    numLeadingNormalizableKeys < keyPositions.length ||
      normalizableKeyPrefixLen == Integer.MAX_VALUE ||
      normalizableKeyPrefixLen > keyBytes

  override def invertNormalizedKey(): Boolean = invertNormKey

  override def supportsSerializationWithKeyNormalization(): Boolean = false

  override def writeWithKeyNormalization(record: Row, target: DataOutputView): Unit =
    throw new UnsupportedOperationException("Record serialization with leading normalized keys " +
      "not supported.")

  override def readWithKeyDenormalization(reuse: Row, source: DataInputView): Row =
    throw new UnsupportedOperationException("Record deserialization with leading normalized keys " +
      "not supported.")

  override def duplicate(): TypeComparator[Row] = {
    // copy comparator and serializer factories
    val comparatorsCopy = comparators.map(_.duplicate().asInstanceOf[NullAwareComparator[Any]])
    val serializersCopy = serializers.map(_.duplicate())

    new RowComparator(
      keyPositions,
      comparatorsCopy,
      serializersCopy,
      auxiliaryFields)
  }

  override def hash(value: Row): Int = {
    var code: Int = 0
    var i = 0
    try {
      while(i < keyPositions.length) {
        code *= TupleComparatorBase.HASH_SALT(i & 0x1F)
        val element = value.productElement(keyPositions(i)) // element can be null
        code += comparators(i).hash(element)
        i += 1
      }
    } catch {
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
    code
  }

  override def setReference(toCompare: Row) {
    var i = 0
    try {
      while(i < keyPositions.length) {
        val comparator = comparators(i)
        val element = toCompare.productElement(keyPositions(i))
        comparator.setReference(element) // element can be null
        i += 1
      }
    } catch {
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
  }

  override def equalToReference(candidate: Row): Boolean = {
    var i = 0
    try {
      while(i < keyPositions.length) {
        val comparator = comparators(i)
        val element = candidate.productElement(keyPositions(i)) // element can be null
        // check if reference is not equal
        if (!comparator.equalToReference(element)) {
          return false
        }
        i += 1
      }
    } catch {
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
    true
  }

  override def compare(first: Row, second: Row): Int = {
    var i = 0
    try {
      while(i < keyPositions.length) {
        val keyPos: Int = keyPositions(i)
        val comparator = comparators(i)
        val firstElement = first.productElement(keyPos) // element can be null
        val secondElement = second.productElement(keyPos) // element can be null

        val cmp = comparator.compare(firstElement, secondElement)
        if (cmp != 0) {
          return cmp
        }
        i += 1
      }
    } catch {
      case iobex: IndexOutOfBoundsException =>
        throw new KeyFieldOutOfBoundsException(keyPositions(i))
    }
    0
  }

  override def putNormalizedKey(
      record: Row,
      target: MemorySegment,
      offset: Int,
      numBytes: Int)
    : Unit = {
    var bytesLeft = numBytes
    var currentOffset = offset

    var i = 0
    while (i < numLeadingNormalizableKeys && bytesLeft > 0) {
      var len = normalizedKeyLengths(i)
      len = if (bytesLeft >= len) len else bytesLeft

      val comparator = comparators(i)
      val element = record.productElement(keyPositions(i)) // element can be null
      // write key
      comparator.putNormalizedKey(element, target, currentOffset, len)

      bytesLeft -= len
      currentOffset += len
      i += 1
    }
  }

  override def getFlatComparator(flatComparators: util.List[TypeComparator[_]]): Unit =
    comparators.foreach { c =>
      c.getFlatComparators.foreach { fc =>
        flatComparators.add(fc)
      }
    }

  override def extractKeys(record: Any, target: Array[AnyRef], index: Int): Int = {
    val len = comparators.length
    var localIndex = index
    var i = 0
    while (i < len) {
      val element = record.asInstanceOf[Row].productElement(keyPositions(i)) // element can be null
      localIndex += comparators(i).extractKeys(element, target, localIndex)
      i += 1
    }
    localIndex - index
  }
}

object RowComparator {
  private def makeNullAware(
      comparators: Array[TypeComparator[Any]],
      orders: Array[Boolean])
    : Array[NullAwareComparator[Any]] =
    comparators
      .zip(orders)
      .map { case (comp, order) =>
        new NullAwareComparator[Any](
          comp,
          order)
      }

  /**
   * @return creates auxiliary fields for normalized key support
   */
  private def createAuxiliaryFields(
      keyPositions: Array[Int],
      comparators: Array[NullAwareComparator[Any]])
    : (Array[Int], Int, Int, Boolean) = {

    val normalizedKeyLengths = new Array[Int](keyPositions.length)
    var numLeadingNormalizableKeys = 0
    var normalizableKeyPrefixLen = 0
    var inverted = false

    var i = 0
    while (i < keyPositions.length) {
      val k = comparators(i)
      // as long as the leading keys support normalized keys, we can build up the composite key
      if (k.supportsNormalizedKey()) {
        if (i == 0) {
          // the first comparator decides whether we need to invert the key direction
          inverted = k.invertNormalizedKey()
        }
        else if (k.invertNormalizedKey() != inverted) {
          // if a successor does not agree on the inversion direction, it cannot be part of the
          // normalized key
          return (normalizedKeyLengths,
            numLeadingNormalizableKeys,
            normalizableKeyPrefixLen,
            inverted)
        }
        numLeadingNormalizableKeys += 1
        val len = k.getNormalizeKeyLen
        if (len < 0) {
          throw new RuntimeException("Comparator " + k.getClass.getName +
            " specifies an invalid length for the normalized key: " + len)
        }
        normalizedKeyLengths(i) = len
        normalizableKeyPrefixLen += len
        if (normalizableKeyPrefixLen < 0) {
          // overflow, which means we are out of budget for normalized key space anyways
          return (normalizedKeyLengths,
            numLeadingNormalizableKeys,
            Integer.MAX_VALUE,
            inverted)
        }
      }
      else {
        return (normalizedKeyLengths,
          numLeadingNormalizableKeys,
          normalizableKeyPrefixLen,
          inverted)
      }
      i += 1
    }
    (normalizedKeyLengths,
      numLeadingNormalizableKeys,
      normalizableKeyPrefixLen,
      inverted)
  }
}

