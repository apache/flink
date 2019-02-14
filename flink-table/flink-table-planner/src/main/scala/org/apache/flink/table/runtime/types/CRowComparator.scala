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

package org.apache.flink.table.runtime.types

import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.core.memory.{DataInputView, DataOutputView, MemorySegment}
import org.apache.flink.types.Row

class CRowComparator(val rowComp: TypeComparator[Row]) extends TypeComparator[CRow] {

  override def hash(record: CRow): Int = rowComp.hash(record.row)

  override def setReference(toCompare: CRow): Unit = rowComp.setReference(toCompare.row)

  override def equalToReference(candidate: CRow): Boolean = rowComp.equalToReference(candidate.row)

  override def compareToReference(otherComp: TypeComparator[CRow]): Int = {
    val otherCRowComp = otherComp.asInstanceOf[CRowComparator]
    rowComp.compareToReference(otherCRowComp.rowComp)
  }

  override def compare(first: CRow, second: CRow): Int = {
    rowComp.compare(first.row, second.row)
  }

  override def compareSerialized(firstSource: DataInputView, secondSource: DataInputView): Int = {
    rowComp.compareSerialized(firstSource, secondSource)
  }

  override def supportsNormalizedKey(): Boolean = rowComp.supportsNormalizedKey()

  override def supportsSerializationWithKeyNormalization(): Boolean =
    rowComp.supportsSerializationWithKeyNormalization()

  override def getNormalizeKeyLen: Int = rowComp.getNormalizeKeyLen

  override def isNormalizedKeyPrefixOnly(keyBytes: Int): Boolean =
    rowComp.isNormalizedKeyPrefixOnly(keyBytes)

  override def putNormalizedKey(
      record: CRow,
      target: MemorySegment,
      offset: Int,
      numBytes: Int): Unit = rowComp.putNormalizedKey(record.row, target, offset, numBytes)

  override def writeWithKeyNormalization(record: CRow, target: DataOutputView): Unit = {
    rowComp.writeWithKeyNormalization(record.row, target)
    target.writeBoolean(record.change)
  }

  override def readWithKeyDenormalization(reuse: CRow, source: DataInputView): CRow = {
    val row = rowComp.readWithKeyDenormalization(reuse.row, source)
    reuse.row = row
    reuse.change = source.readBoolean()
    reuse
  }

  override def invertNormalizedKey(): Boolean = rowComp.invertNormalizedKey()

  override def duplicate(): TypeComparator[CRow] = new CRowComparator(rowComp.duplicate())

  override def extractKeys(record: scala.Any, target: Array[AnyRef], index: Int): Int =
    rowComp.extractKeys(record.asInstanceOf[CRow].row, target, index)

  override def getFlatComparators: Array[TypeComparator[_]] =
    rowComp.getFlatComparators
}
