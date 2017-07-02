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
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.util.{Failure, Success, Try}

/**
 * Serializer for [[scala.util.Try]].
 */
@Internal
@SerialVersionUID(-3052182891252564491L)
class TrySerializer[A](
    private val elemSerializer: TypeSerializer[A],
    private val executionConfig: ExecutionConfig)
  extends TypeSerializer[Try[A]] {

  override def duplicate: TrySerializer[A] = this

  val throwableSerializer = new KryoSerializer[Throwable](classOf[Throwable], executionConfig)

  override def createInstance: Try[A] = {
    Failure(new RuntimeException("Empty Failure"))
  }

  override def isImmutableType: Boolean = elemSerializer == null || elemSerializer.isImmutableType

  override def getLength: Int = -1

  override def copy(from: Try[A]): Try[A] = from match {
    case Success(a) => Success(elemSerializer.copy(a))
    case Failure(t) => Failure(throwableSerializer.copy(t))
  }

  override def copy(from: Try[A], reuse: Try[A]): Try[A] = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val isSuccess = source.readBoolean()
    target.writeBoolean(isSuccess)
    if (isSuccess) {
      elemSerializer.copy(source, target)
    } else {
      throwableSerializer.copy(source, target)
    }
  }

  override def serialize(either: Try[A], target: DataOutputView): Unit = either match {
    case Success(a) =>
      target.writeBoolean(true)
      elemSerializer.serialize(a, target)
    case Failure(t) =>
      target.writeBoolean(false)
      throwableSerializer.serialize(t, target)
  }

  override def deserialize(source: DataInputView): Try[A] = {
    val isSuccess = source.readBoolean()
    if (isSuccess) {
      Success(elemSerializer.deserialize(source))
    } else {
      Failure(throwableSerializer.deserialize(source))
    }
  }

  override def deserialize(reuse: Try[A], source: DataInputView): Try[A] = deserialize(source)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: TrySerializer[_] =>
        other.canEqual(this) && elemSerializer.equals(other.elemSerializer)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[TrySerializer[_]]
  }

  override def hashCode(): Int = {
    31 * elemSerializer.hashCode() + executionConfig.hashCode()
  }

  // --------------------------------------------------------------------------------------------
  // Serializer configuration snapshotting & compatibility
  // --------------------------------------------------------------------------------------------

  override def snapshotConfiguration(): ScalaTrySerializerConfigSnapshot[A] = {
    new ScalaTrySerializerConfigSnapshot[A](elemSerializer, throwableSerializer)
  }

  override def ensureCompatibility(
      configSnapshot: TypeSerializerConfigSnapshot): CompatibilityResult[Try[A]] = {

    configSnapshot match {
      case trySerializerConfigSnapshot
          : ScalaTrySerializerConfigSnapshot[A] =>
        ensureCompatibility(trySerializerConfigSnapshot)
      case legacyTrySerializerConfigSnapshot
          : TrySerializer.TrySerializerConfigSnapshot[A] =>
        ensureCompatibility(legacyTrySerializerConfigSnapshot)
      case _ => CompatibilityResult.requiresMigration()
    }
  }

  private def ensureCompatibility(
      compositeConfigSnapshot: CompositeTypeSerializerConfigSnapshot)
        : CompatibilityResult[Try[A]] = {

    val previousSerializersAndConfigs =
      compositeConfigSnapshot.getNestedSerializersAndConfigs

    val elemCompatRes = CompatibilityUtil.resolveCompatibilityResult(
      previousSerializersAndConfigs.get(0).f0,
      classOf[UnloadableDummyTypeSerializer[_]],
      previousSerializersAndConfigs.get(0).f1,
      elemSerializer)

    val throwableCompatRes = CompatibilityUtil.resolveCompatibilityResult(
      previousSerializersAndConfigs.get(1).f0,
      classOf[UnloadableDummyTypeSerializer[_]],
      previousSerializersAndConfigs.get(1).f1,
      throwableSerializer)

    if (elemCompatRes.isRequiresMigration || throwableCompatRes.isRequiresMigration) {
      CompatibilityResult.requiresMigration()
    } else {
      CompatibilityResult.compatible()
    }
  }
}

object TrySerializer {

  /**
    * We need to keep this to be compatible with snapshots taken in Flink 1.3.0.
    * Once Flink 1.3.x is no longer supported, this can be removed.
    */
  class TrySerializerConfigSnapshot[A]()
      extends CompositeTypeSerializerConfigSnapshot() {

    override def getVersion: Int = TrySerializerConfigSnapshot.VERSION
  }

  object TrySerializerConfigSnapshot {
    val VERSION = 1
  }

}
