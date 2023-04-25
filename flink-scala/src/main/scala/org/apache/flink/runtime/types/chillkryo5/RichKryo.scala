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
package org.apache.flink.runtime.types.chillkryo5

import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.Serializer
import com.esotericsoftware.kryo.kryo5.io.ByteBufferInputStream
import com.esotericsoftware.kryo.kryo5.io.Input

import java.io.{InputStream, Serializable}
import java.nio.ByteBuffer

import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag
import scala.util.control.Exception.allCatch

/**
 * Enrichment pattern to add methods to Kryo objects TODO: make this a value-class in scala 2.10
 * This also follows the builder pattern to allow easily chaining this calls
 */
class RichKryo(k: Kryo) {
  def alreadyRegistered(klass: Class[_]): Boolean =
    k.getClassResolver.getRegistration(klass) != null

  def alreadyRegistered[T](implicit cmf: ClassTag[T]): Boolean = alreadyRegistered(cmf.runtimeClass)

  def forSubclass[T](kser: Serializer[T])(implicit cmf: ClassTag[T]): RichKryo = {
    k.addDefaultSerializer(cmf.runtimeClass, kser)
    this
  }

  def forTraversableSubclass[T, C <: Traversable[T]](
      c: C with Traversable[T],
      isImmutable: Boolean = true)(implicit
      mf: ClassTag[C],
      cbf: CanBuildFrom[C, T, C]): RichKryo = {
    k.addDefaultSerializer(mf.runtimeClass, new TraversableSerializer(isImmutable)(cbf))
    this
  }

  def forClass[T](kser: Serializer[T])(implicit cmf: ClassTag[T]): RichKryo = {
    k.register(cmf.runtimeClass, kser)
    this
  }

  def forTraversableClass[T, C <: Traversable[T]](
      c: C with Traversable[T],
      isImmutable: Boolean = true)(implicit mf: ClassTag[C], cbf: CanBuildFrom[C, T, C]): RichKryo =
    forClass(new TraversableSerializer(isImmutable)(cbf))

  def forConcreteTraversableClass[T, C <: Traversable[T]](
      c: C with Traversable[T],
      isImmutable: Boolean = true)(implicit cbf: CanBuildFrom[C, T, C]): RichKryo = {
    // a ClassTag is not used here since its runtimeClass method does not return the concrete internal type
    // that Scala uses for small immutable maps (i.e., scala.collection.immutable.Map$Map1)
    k.register(c.getClass, new TraversableSerializer(isImmutable)(cbf))
    this
  }

//  /**
//   * Use Java serialization, which is very slow.
//   * avoid this if possible, but for very rare classes it is probably fine
//   */
//  def javaForClass[T <: Serializable](implicit cmf: ClassTag[T]): Kryo = {
//    k.register(cmf.runtimeClass, new com.esotericsoftware.kryo.serializers.JavaSerializer)
//    k
//  }
//  /**
//   * Use Java serialization, which is very slow.
//   * avoid this if possible, but for very rare classes it is probably fine
//   */
//  def javaForSubclass[T <: Serializable](implicit cmf: ClassTag[T]): Kryo = {
//    k.addDefaultSerializer(cmf.runtimeClass, new com.esotericsoftware.kryo.serializers.JavaSerializer)
//    k
//  }

  def registerClasses(klasses: TraversableOnce[Class[_]]): RichKryo = {
    klasses.foreach {
      klass: Class[_] =>
        if (!alreadyRegistered(ClassTag(klass)))
          k.register(klass)
    }
    this
  }

  /** Populate the wrapped Kryo instance with this registrar */
  def populateFrom(reg: IKryo5Registrar): RichKryo = {
    reg(k)
    this
  }

  def fromInputStream(s: InputStream): Option[AnyRef] = {
    // Can't reuse Input and call Input#setInputStream everytime
    val streamInput = new Input(s)
    allCatch.opt(k.readClassAndObject(streamInput))
  }

  def fromByteBuffer(b: ByteBuffer): Option[AnyRef] =
    fromInputStream(new ByteBufferInputStream(b))
}
