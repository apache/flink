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
package org.apache.flink.api.table.typeutils

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeutils.NullMaskUtils.{writeNullMask, readIntoNullMask, readIntoAndCopyNullMask}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
 * Serializer for [[Row]].
 */
class RowSerializer(val fieldSerializers: Array[TypeSerializer[Any]])
  extends TypeSerializer[Row] {

  private val nullMask = new Array[Boolean](fieldSerializers.length)

  override def isImmutableType: Boolean = false

  override def getLength: Int = -1

  override def duplicate: RowSerializer = {
    val duplicateFieldSerializers = fieldSerializers.map(_.duplicate())
    new RowSerializer(duplicateFieldSerializers)
  }

  override def createInstance: Row = {
    new Row(fieldSerializers.length)
  }

  override def copy(from: Row, reuse: Row): Row = {
    val len = fieldSerializers.length

    // cannot reuse, do a non-reuse copy
    if (reuse == null) {
      return copy(from)
    }

    if (from.productArity != len || reuse.productArity != len) {
      throw new RuntimeException("Row arity of reuse or from is incompatible with this " +
        "RowSerializer.")
    }

    var i = 0
    while (i < len) {
      val fromField = from.productElement(i)
      if (fromField != null) {
        val reuseField = reuse.productElement(i)
        if (reuseField != null) {
          val copy = fieldSerializers(i).copy(fromField, reuseField)
          reuse.setField(i, copy)
        }
        else {
          val copy = fieldSerializers(i).copy(fromField)
          reuse.setField(i, copy)
        }
      }
      else {
        reuse.setField(i, null)
      }
      i += 1
    }
    reuse
  }

  override def copy(from: Row): Row = {
    val len = fieldSerializers.length

    if (from.productArity != len) {
      throw new RuntimeException("Row arity of from does not match serializers.")
    }
    val result = new Row(len)
    var i = 0
    while (i < len) {
      val fromField = from.productElement(i).asInstanceOf[AnyRef]
      if (fromField != null) {
        val copy = fieldSerializers(i).copy(fromField)
        result.setField(i, copy)
      }
      else {
        result.setField(i, null)
      }
      i += 1
    }
    result
  }

  override def serialize(value: Row, target: DataOutputView) {
    val len = fieldSerializers.length

    if (value.productArity != len) {
      throw new RuntimeException("Row arity of value does not match serializers.")
    }

    // write a null mask
    writeNullMask(len, value, target)

    // serialize non-null fields
    var i = 0
    while (i < len) {
      val o = value.productElement(i).asInstanceOf[AnyRef]
      if (o != null) {
        val serializer = fieldSerializers(i)
        serializer.serialize(value.productElement(i), target)
      }
      i += 1
    }
  }

  override def deserialize(reuse: Row, source: DataInputView): Row = {
    val len = fieldSerializers.length

    if (reuse.productArity != len) {
      throw new RuntimeException("Row arity of reuse does not match serializers.")
    }

    // read null mask
    readIntoNullMask(len, source, nullMask)

    // read non-null fields
    var i = 0
    while (i < len) {
      if (nullMask(i)) {
        reuse.setField(i, null)
      }
      else {
        val reuseField = reuse.productElement(i).asInstanceOf[AnyRef]
        if (reuseField != null) {
          reuse.setField(i, fieldSerializers(i).deserialize(reuseField, source))
        }
        else {
          reuse.setField(i, fieldSerializers(i).deserialize(source))
        }
      }
      i += 1
    }
    reuse
  }

  override def deserialize(source: DataInputView): Row = {
    val len = fieldSerializers.length

    val result = new Row(len)

    // read null mask
    readIntoNullMask(len, source, nullMask)

    // read non-null fields
    var i = 0
    while (i < len) {
      if (nullMask(i)) {
        result.setField(i, null)
      }
      else {
        result.setField(i, fieldSerializers(i).deserialize(source))
      }
      i += 1
    }
    result
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val len = fieldSerializers.length

    // copy null mask
    readIntoAndCopyNullMask(len, source, target, nullMask)

    // read non-null fields
    var i = 0
    while (i < len) {
      if (!nullMask(i)) {
        fieldSerializers(i).copy(source, target)
      }
      i += 1
    }
  }

  override def equals(any: Any): Boolean = {
    any match {
      case otherRS: RowSerializer =>
        otherRS.canEqual(this) &&
          fieldSerializers.sameElements(otherRS.fieldSerializers)
      case _ => false
    }
  }

  override def canEqual(obj: AnyRef): Boolean = {
    obj.isInstanceOf[RowSerializer]
  }

  override def hashCode(): Int = {
    java.util.Arrays.hashCode(fieldSerializers.asInstanceOf[Array[AnyRef]])
  }
}
