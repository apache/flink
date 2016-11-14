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
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.api.common.typeutils.base.IntComparator
import org.apache.flink.core.memory.{DataOutputView, DataInputView, MemorySegment}

/**
 * Comparator for [[Enumeration]] values.
 */
@Internal
@SerialVersionUID(1000L)
class EnumValueComparator[E <: Enumeration](ascComp: Boolean) extends TypeComparator[E#Value] {

  type T = E#Value

  final val intComparator = new IntComparator(ascComp)

  // We cannot use the Clone Constructor from Scala so we have to do it manually
  def duplicate: TypeComparator[T] = {
    new EnumValueComparator[E](ascComp)
  }

  // --------------------------------------------------------------------------------------------
  //  Comparator Methods
  // --------------------------------------------------------------------------------------------

  override def compareSerialized(firstSource: DataInputView, secondSource: DataInputView): Int = {
    intComparator.compareSerialized(firstSource, secondSource)
  }

  def supportsNormalizedKey: Boolean = {
    intComparator.supportsNormalizedKey
  }

  def getNormalizeKeyLen: Int = {
    intComparator.getNormalizeKeyLen
  }

  def isNormalizedKeyPrefixOnly(keyBytes: Int): Boolean = {
    intComparator.isNormalizedKeyPrefixOnly(keyBytes)
  }

  override def putNormalizedKey(v: T, target: MemorySegment, offset: Int, numBytes: Int): Unit = {
    intComparator.putNormalizedKey(v.id, target, offset, numBytes)
  }

  override def hash(record: T): Int = intComparator.hash(record.id)

  override def setReference(toCompare: T): Unit = {
    intComparator.setReference(toCompare.id)
  }

  override def equalToReference(candidate: T): Boolean = {
    intComparator.equalToReference(candidate.id)
  }

  override def compareToReference(referencedComparator: TypeComparator[T]): Int = {
    intComparator.compareToReference(referencedComparator.asInstanceOf[this.type].intComparator)
  }

  override def compare(first: E#Value, second: E#Value): Int = {
    intComparator.compare(first.id, second.id)
  }

  override def invertNormalizedKey(): Boolean = {
    intComparator.invertNormalizedKey()
  }

  override def writeWithKeyNormalization(record: T, target: DataOutputView): Unit = {
    intComparator.writeWithKeyNormalization(record.id, target)
  }

  override def supportsSerializationWithKeyNormalization(): Boolean = {
    intComparator.supportsSerializationWithKeyNormalization()
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
