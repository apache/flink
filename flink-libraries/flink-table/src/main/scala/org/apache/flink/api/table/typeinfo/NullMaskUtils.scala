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

import org.apache.flink.api.table.Row
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

object NullMaskUtils {

  def writeNullMask(len: Int, value: Row, target: DataOutputView): Unit = {
    var b = 0x00
    var bytePos = 0

    var fieldPos = 0
    var numPos = 0
    while (fieldPos < len) {
      b = 0x00
      // set bits in byte
      bytePos = 0
      numPos = Math.min(8, len - fieldPos)
      while (bytePos < numPos) {
        b = b << 1
        // set bit if field is null
        if(value.productElement(fieldPos + bytePos) == null) {
          b |= 0x01
        }
        bytePos += 1
      }
      fieldPos += numPos
      // shift bits if last byte is not completely filled
      b <<= (8 - bytePos)
      // write byte
      target.writeByte(b)
    }
  }

  def readIntoNullMask(len: Int, source: DataInputView, nullMask: Array[Boolean]): Unit = {
    var b = 0x00
    var bytePos = 0

    var fieldPos = 0
    var numPos = 0
    while (fieldPos < len) {
      // read byte
      b = source.readUnsignedByte()
      bytePos = 0
      numPos = Math.min(8, len - fieldPos)
      while (bytePos < numPos) {
        nullMask(fieldPos + bytePos) = (b & 0x80) > 0
        b = b << 1
        bytePos += 1
      }
      fieldPos += numPos
    }
  }

  def readIntoAndCopyNullMask(
      len: Int,
      source: DataInputView,
      target: DataOutputView,
      nullMask: Array[Boolean]): Unit = {
    var b = 0x00
    var bytePos = 0

    var fieldPos = 0
    var numPos = 0
    while (fieldPos < len) {
      // read byte
      b = source.readUnsignedByte()
      // copy byte
      target.writeByte(b)
      bytePos = 0
      numPos = Math.min(8, len - fieldPos)
      while (bytePos < numPos) {
        nullMask(fieldPos + bytePos) = (b & 0x80) > 0
        b = b << 1
        bytePos += 1
      }
      fieldPos += numPos
    }
  }

}
