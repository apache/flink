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
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.shaded.guava31.com.google.common.cache.{Cache, CacheBuilder}

import java.io.ObjectInputStream
import java.util.concurrent.Callable

import scala.collection.generic.CanBuildFrom
import scala.ref.WeakReference

/** Serializer for Scala Collections. */
@Internal
@SerialVersionUID(7522917416391312410L)
class TraversableSerializer[T <: TraversableOnce[E], E](
    var elementSerializer: TypeSerializer[E],
    var cbfCode: String)
  extends TypeSerializer[T]
  with Cloneable {

  @transient var cbf: CanBuildFrom[T, E, T] = compileCbf(cbfCode)

  // this is needed for compatibility with pre-1.8 versions of this. Serialized instances
  // of this in savepoints don't have the cbfCode field, therefore we override it in the
  // Macro that generates a specific TraversableSerializer and use it in readObject()
  // if needed.
  protected def legacyCbfCode: String = null

  def compileCbf(code: String): CanBuildFrom[T, E, T] = {
    val cl = Thread.currentThread().getContextClassLoader
    TraversableSerializer.compileCbf(cl, code)
  }

  override def duplicate = {
    val duplicateElementSerializer = elementSerializer.duplicate()
    if (duplicateElementSerializer eq elementSerializer) {
      // is not stateful, so return ourselves
      this
    } else {
      val result = this.clone().asInstanceOf[TraversableSerializer[T, E]]
      result.elementSerializer = elementSerializer.duplicate()
      result
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    if (cbfCode == null) {
      cbfCode = legacyCbfCode
    }
    require(cbfCode != null)
    cbf = compileCbf(cbfCode)
  }

  override def createInstance: T = {
    cbf().result()
  }

  override def isImmutableType: Boolean = false

  override def getLength: Int = -1

  override def copy(from: T): T = {
    val builder = cbf()
    builder.sizeHint(from.size)
    from.foreach(e => builder += elementSerializer.copy(e))
    builder.result()
  }

  override def copy(from: T, reuse: T): T = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val len = source.readInt()
    target.writeInt(len)

    var i = 0
    while (i < len) {
      val isNonNull = source.readBoolean()
      target.writeBoolean(isNonNull)
      if (isNonNull) {
        elementSerializer.copy(source, target)
      }
      i += 1
    }
  }

  override def serialize(coll: T, target: DataOutputView): Unit = {
    val len = coll.size
    target.writeInt(len)
    coll.foreach {
      e =>
        if (e == null) {
          target.writeBoolean(false)
        } else {
          target.writeBoolean(true)
          elementSerializer.serialize(e, target)
        }
    }
  }

  override def deserialize(source: DataInputView): T = {
    val len = source.readInt()
    val builder = cbf()

    var i = 0
    while (i < len) {
      val isNonNull = source.readBoolean()
      if (isNonNull) {
        builder += elementSerializer.deserialize(source)
      } else {
        builder += null.asInstanceOf[E]
      }
      i += 1
    }

    builder.result()
  }

  override def deserialize(reuse: T, source: DataInputView): T = {
    val len = source.readInt()
    val builder = cbf()

    var i = 0
    while (i < len) {
      val isNonNull = source.readBoolean()
      if (isNonNull) {
        builder += elementSerializer.deserialize(source)
      } else {
        builder += null.asInstanceOf[E]
      }
      i += 1
    }

    builder.result()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: TraversableSerializer[_, _] =>
        elementSerializer.equals(other.elementSerializer)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    elementSerializer.hashCode()
  }

  override def snapshotConfiguration(): TraversableSerializerSnapshot[T, E] = {
    new TraversableSerializerSnapshot[T, E](this)
  }
}

object TraversableSerializer {

  private val CACHE: Cache[Key, CanBuildFrom[_, _, _]] = CacheBuilder
    .newBuilder()
    .weakValues()
    .maximumSize(128)
    .build()

  def compileCbf[T, E](classLoader: ClassLoader, cbfCode: String): CanBuildFrom[T, E, T] = {
    val key = Key(classLoader, cbfCode)

    CACHE
      .get(key, LazyRuntimeCompiler(classLoader, cbfCode))
      .asInstanceOf[CanBuildFrom[T, E, T]]
  }

  object Key {

    def apply(classLoader: ClassLoader, cbfCode: String): Key = {
      val hashCode = System.identityHashCode(classLoader)
      val weakReference = WeakReference(classLoader)
      Key(hashCode, weakReference, cbfCode)
    }
  }

  case class Key(
      classLoaderHash: Int,
      classLoaderRef: WeakReference[ClassLoader],
      cbfCode: String) {

    override def hashCode(): Int = classLoaderHash * 37 + cbfCode.hashCode

    override def equals(obj: Any): Boolean = {
      obj match {
        case Key(thatHashCode, thatClassLoaderRef, thatCbfCode) =>
          (this.classLoaderHash == thatHashCode) &&
          (this.classLoaderRef.get == thatClassLoaderRef.get) &&
          (this.cbfCode == thatCbfCode)

        case _ =>
          false
      }
    }
  }

  private case class LazyRuntimeCompiler[T, E](classLoader: ClassLoader, code: String)
    extends Callable[CanBuildFrom[T, E, T]] {

    override def call(): CanBuildFrom[T, E, T] = compileCbfInternal(classLoader, code)

    private def compileCbfInternal(
        classLoader: ClassLoader,
        code: String): CanBuildFrom[T, E, T] = {

      import scala.reflect.runtime.universe._
      import scala.tools.reflect.ToolBox

      val tb = runtimeMirror(classLoader).mkToolBox()
      val tree = tb.parse(code)
      val compiled = tb.compile(tree)
      val cbf = compiled()
      cbf.asInstanceOf[CanBuildFrom[T, E, T]]
    }
  }

}
