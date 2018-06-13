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
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.typeutils.runtime.EitherSerializerConfigSnapshot
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
 * Serializer for [[Either]].
 */
@Internal
@SerialVersionUID(9219995873023657525L)
class EitherSerializer[A, B, T <: Either[A, B]](
    val leftSerializer: TypeSerializer[A],
    val rightSerializer: TypeSerializer[B])
  extends TypeSerializer[T] {

  override def duplicate: EitherSerializer[A,B,T] = this

  override def createInstance: T = {
    Left(null).asInstanceOf[T]
  }

  override def isImmutableType: Boolean = {
    (leftSerializer == null || leftSerializer.isImmutableType) &&
      (rightSerializer == null || rightSerializer.isImmutableType)
  }

  override def getLength: Int = -1

  override def copy(from: T): T = from match {
    case Left(a) => Left(leftSerializer.copy(a)).asInstanceOf[T]
    case Right(b) => Right(rightSerializer.copy(b)).asInstanceOf[T]
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
    case Left(a) =>
      target.writeBoolean(true)
      leftSerializer.serialize(a, target)
    case Right(b) =>
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
    obj match {
      case eitherSerializer: EitherSerializer[_, _, _] =>
        eitherSerializer.canEqual(this) &&
        leftSerializer.equals(eitherSerializer.leftSerializer) &&
        rightSerializer.equals(eitherSerializer.rightSerializer)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[EitherSerializer[_, _, _]]
  }

  override def hashCode(): Int = {
    31 * leftSerializer.hashCode() + rightSerializer.hashCode()
  }

  // --------------------------------------------------------------------------------------------
  // Serializer configuration snapshotting & compatibility
  // --------------------------------------------------------------------------------------------

  override def snapshotConfiguration(): ScalaEitherSerializerConfigSnapshot[T, A, B] = {
    new ScalaEitherSerializerConfigSnapshot[T, A, B](leftSerializer, rightSerializer)
  }

  override def ensureCompatibility(
      configSnapshot: TypeSerializerConfigSnapshot[_]): CompatibilityResult[T] = {

    configSnapshot match {
      case eitherSerializerConfig: ScalaEitherSerializerConfigSnapshot[T, A, B] =>
        checkCompatibility(eitherSerializerConfig)

      // backwards compatibility path;
      // Flink versions older or equal to 1.5.x uses a
      // EitherSerializerConfigSnapshot as the snapshot
      case legacyConfig: EitherSerializerConfigSnapshot[A, B] =>
        checkCompatibility(legacyConfig)

      case _ => CompatibilityResult.requiresMigration()
    }
  }

  private def checkCompatibility(
      configSnapshot: CompositeTypeSerializerConfigSnapshot[_]
    ): CompatibilityResult[T] = {

    val previousLeftRightSerWithConfigs =
      configSnapshot.getNestedSerializersAndConfigs

    val leftCompatResult = CompatibilityUtil.resolveCompatibilityResult(
      previousLeftRightSerWithConfigs.get(0).f0,
      classOf[UnloadableDummyTypeSerializer[_]],
      previousLeftRightSerWithConfigs.get(0).f1,
      leftSerializer)

    val rightCompatResult = CompatibilityUtil.resolveCompatibilityResult(
      previousLeftRightSerWithConfigs.get(1).f0,
      classOf[UnloadableDummyTypeSerializer[_]],
      previousLeftRightSerWithConfigs.get(1).f1,
      rightSerializer)

    if (leftCompatResult.isRequiresMigration
      || rightCompatResult.isRequiresMigration) {
      CompatibilityResult.requiresMigration()
    } else {
      CompatibilityResult.compatible()
    }
  }
}
