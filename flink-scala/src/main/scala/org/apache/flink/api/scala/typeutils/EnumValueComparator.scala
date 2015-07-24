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

import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.core.memory.{DataOutputView, DataInputView, MemorySegment}

/**
 * Comparator for [[Enumeration]] values.
 */
@SerialVersionUID(1000L)
class EnumValueComparator[E <: Enumeration](ascComp: Boolean) extends TypeComparator[E#Value] {

  type T = E#Value

  @transient
  private var reference: T = null

  // We cannot use the Clone Constructor from Scala so we have to do it manually
  def duplicate: TypeComparator[T] = {
    new EnumValueComparator[E](ascComp)
  }

  // --------------------------------------------------------------------------------------------
  //  Comparator Methods
  // --------------------------------------------------------------------------------------------

  override def compareSerialized(firstSource: DataInputView, secondSource: DataInputView): Int = {
    val i1: Int = firstSource.readInt
    val i2: Int = secondSource.readInt
    val comp: Int = if (i1 < i2) -1 else if (i1 == i2) 0 else 1
    if (ascComp) comp else -comp
  }

  def supportsNormalizedKey: Boolean = {
    true
  }

  def getNormalizeKeyLen: Int = {
    4
  }

  def isNormalizedKeyPrefixOnly(keyBytes: Int): Boolean = {
    keyBytes < 4
  }

  override def putNormalizedKey(v: T, tgt: MemorySegment, offset: Int, numBytes: Int): Unit = {
    val value: Int = v.id - Integer.MIN_VALUE

    // see IntValue for an explanation of the logic
    if (numBytes == 4) {
      // default case, full normalized key
      tgt.putIntBigEndian(offset, value)
    }
    else if (numBytes <= 0) {
    }
    else if (numBytes < 4) {
      var i: Int = 0
      while (numBytes - i > 0) {
        tgt.put(offset + i, (value >>> ((3 - i) << 3)).toByte)
        i += 1
      }
    }
    else {
      tgt.putLongBigEndian(offset, value)
      var i: Int = 4
      while (i < numBytes) {
        tgt.put(offset + i, 0.toByte)
        i += 1
      }
    }
  }

  override def hash(record: T): Int = record.##

  override def setReference(toCompare: T): Unit = {
    this.reference = toCompare
  }

  override def equalToReference(candidate: T): Boolean = {
    candidate == reference
  }

  override def compareToReference(refComparator: TypeComparator[T]): Int = {
    val comp = refComparator.asInstanceOf[this.type].reference.id.compareTo(this.reference.id)
    if (ascComp) comp else -comp
  }

  override def compare(first: E#Value, second: E#Value): Int = {
    val cmp = first.id.compareTo(second.id)
    if (ascComp) cmp else -cmp
  }

  override def invertNormalizedKey(): Boolean = {
    !ascComp
  }

  override def writeWithKeyNormalization(record: T, target: DataOutputView): Unit = {
    throw new UnsupportedOperationException
  }

  override def supportsSerializationWithKeyNormalization(): Boolean = {
    false
  }

  override def extractKeys(record: AnyRef, target: Array[AnyRef], index: Int): Int = {
    target(index) = record
    1
  }

  override lazy val getFlatComparators: Array[TypeComparator[_]] = {
    Array(this)
  }

  override def readWithKeyDenormalization(reuse: T, source: DataInputView): T = {
    throw new UnsupportedOperationException
  }
}
