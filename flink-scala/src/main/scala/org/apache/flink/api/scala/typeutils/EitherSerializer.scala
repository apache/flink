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

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataOutputView, DataInputView}

/**
 * Serializer for [[Either]].
 */
class EitherSerializer[A, B, T <: Either[A, B]](
    val leftSerializer: TypeSerializer[A],
    val rightSerializer: TypeSerializer[B])
  extends TypeSerializer[T] {

  override def isStateful: Boolean = false

  override def createInstance: T = {
    Left(null).asInstanceOf[T]
  }

  override def isImmutableType: Boolean = {
    (leftSerializer == null || leftSerializer.isImmutableType) &&
      (rightSerializer == null || rightSerializer.isImmutableType)
  }

  override def getLength: Int = -1

  override def copy(from: T): T = from match {
    case Left(a: A) => Left(leftSerializer.copy(a)).asInstanceOf[T]
    case Right(b: B) => Right(rightSerializer.copy(b)).asInstanceOf[T]
  }

  override def copy(from: T, reuse: T): T = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val isLeft = source.readBoolean()
    target.writeBoolean(isLeft)
    if (isLeft) {
      leftSerializer.copy(source, target)
    } else {
      rightSerializer.copy(source, target)
    }
  }

  override def serialize(either: T, target: DataOutputView): Unit = either match {
    case Left(a: A) =>
      target.writeBoolean(true)
      leftSerializer.serialize(a, target)
    case Right(b: B) =>
      target.writeBoolean(false)
      rightSerializer.serialize(b, target)
  }

  override def deserialize(source: DataInputView): T = {
    val isLeft = source.readBoolean()
    if (isLeft) {
      Left(leftSerializer.deserialize(source)).asInstanceOf[T]
    } else {
      Right(rightSerializer.deserialize(source)).asInstanceOf[T]
    }
  }

  override def deserialize(reuse: T, source: DataInputView): T = {
    val isLeft = source.readBoolean()
    if (isLeft) {
      Left(leftSerializer.deserialize(source)).asInstanceOf[T]
    } else {
      Right(rightSerializer.deserialize(source)).asInstanceOf[T]
    }
  }

  override def equals(obj: Any): Boolean = {
    if (obj != null && obj.isInstanceOf[EitherSerializer[_, _, _]]) {
      val other = obj.asInstanceOf[EitherSerializer[_, _, _]]
      other.leftSerializer.equals(leftSerializer) && other.rightSerializer.equals(rightSerializer)
    } else {
      false
    }
  }
}
