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
package org.apache.flink.runtime.types

import org.apache.flink.api.java.typeutils.runtime.kryo.FlinkChillPackageRegistrar

import _root_.java.io.Serializable
import com.twitter.chill._

import scala.collection.JavaConverters._
import scala.collection.immutable.{BitSet, HashMap, HashSet, ListMap, ListSet, NumericRange, Queue, Range, SortedMap, SortedSet}
import scala.collection.mutable.{BitSet => MBitSet, Buffer, HashMap => MHashMap, HashSet => MHashSet, ListBuffer, Map => MMap, Queue => MQueue, Set => MSet, WrappedArray}
import scala.reflect.ClassTag
import scala.util.matching.Regex

/*
This code is copied as is from Twitter Chill 0.7.4 because we need to user a newer chill version
but want to ensure that the serializers that are registered by default stay the same.

The only changes to the code are those that are required to make it compile and pass checkstyle
checks in our code base.
 */

/**
 * This class has a no-arg constructor, suitable for use with reflection instantiation It has no
 * registered serializers, just the standard Kryo configured for Kryo.
 */
class EmptyFlinkScalaKryoInstantiator extends KryoInstantiator {
  override def newKryo = {
    val k = new KryoBase
    k.setRegistrationRequired(false)
    k.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy)

    // Handle cases where we may have an odd classloader setup like with libjars
    // for hadoop
    val classLoader = Thread.currentThread.getContextClassLoader
    k.setClassLoader(classLoader)

    k
  }
}

object FlinkScalaKryoInstantiator extends Serializable {
  private val mutex = new AnyRef with Serializable // some serializable object
  @transient private var kpool: KryoPool = null

  /** Return a KryoPool that uses the FlinkScalaKryoInstantiator */
  def defaultPool: KryoPool = mutex.synchronized {
    if (null == kpool) {
      kpool = KryoPool.withByteArrayOutputStream(guessThreads, new FlinkScalaKryoInstantiator)
    }
    kpool
  }

  private def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }
}

/** Makes an empty instantiator then registers everything */
class FlinkScalaKryoInstantiator extends EmptyFlinkScalaKryoInstantiator {
  override def newKryo = {
    val k = super.newKryo
    val reg = new AllScalaRegistrar
    reg(k)
    k
  }
}

class ScalaCollectionsRegistrar extends IKryoRegistrar {
  def apply(newK: Kryo) {
    // for binary compat this is here, but could be moved to RichKryo
    def useField[T](cls: Class[T]) {
      val fs = new com.esotericsoftware.kryo.serializers.FieldSerializer(newK, cls)
      fs.setIgnoreSyntheticFields(false) // scala generates a lot of these attributes
      newK.register(cls, fs)
    }
    // The wrappers are private classes:
    useField(List(1, 2, 3).asJava.getClass)
    useField(List(1, 2, 3).iterator.asJava.getClass)
    useField(Map(1 -> 2, 4 -> 3).asJava.getClass)
    useField(new _root_.java.util.ArrayList().asScala.getClass)
    useField(new _root_.java.util.HashMap().asScala.getClass)

    /*
     * Note that subclass-based use: addDefaultSerializers, else: register
     * You should go from MOST specific, to least to specific when using
     * default serializers. The FIRST one found is the one used
     */
    newK
      // wrapper array is abstract
      .forSubclass[WrappedArray[Any]](new WrappedArraySerializer[Any])
      .forSubclass[BitSet](new BitSetSerializer)
      .forSubclass[SortedSet[Any]](new SortedSetSerializer)
      .forClass[Some[Any]](new SomeSerializer[Any])
      .forClass[Left[Any, Any]](new LeftSerializer[Any, Any])
      .forClass[Right[Any, Any]](new RightSerializer[Any, Any])
      .forTraversableSubclass(Queue.empty[Any])
      // List is a sealed class, so there are only two subclasses:
      .forTraversableSubclass(List.empty[Any])
      // Add ListBuffer subclass before Buffer to prevent the more general case taking precedence
      .forTraversableSubclass(ListBuffer.empty[Any], isImmutable = false)
      // add mutable Buffer before Vector, otherwise Vector is used
      .forTraversableSubclass(Buffer.empty[Any], isImmutable = false)
      // Vector is a final class
      .forTraversableClass(Vector.empty[Any])
      .forTraversableSubclass(ListSet.empty[Any])
      // specifically register small sets since Scala represents them differently
      .forConcreteTraversableClass(Set[Any]('a))
      .forConcreteTraversableClass(Set[Any]('a, 'b))
      .forConcreteTraversableClass(Set[Any]('a, 'b, 'c))
      .forConcreteTraversableClass(Set[Any]('a, 'b, 'c, 'd))
      // default set implementation
      .forConcreteTraversableClass(HashSet[Any]('a, 'b, 'c, 'd, 'e))
      // specifically register small maps since Scala represents them differently
      .forConcreteTraversableClass(Map[Any, Any]('a -> 'a))
      .forConcreteTraversableClass(Map[Any, Any]('a -> 'a, 'b -> 'b))
      .forConcreteTraversableClass(Map[Any, Any]('a -> 'a, 'b -> 'b, 'c -> 'c))
      .forConcreteTraversableClass(Map[Any, Any]('a -> 'a, 'b -> 'b, 'c -> 'c, 'd -> 'd))
      // default map implementation
      .forConcreteTraversableClass(
        HashMap[Any, Any]('a -> 'a, 'b -> 'b, 'c -> 'c, 'd -> 'd, 'e -> 'e))
      // The normal fields serializer works for ranges
      .registerClasses(Seq(
        classOf[Range.Inclusive],
        classOf[NumericRange.Inclusive[_]],
        classOf[NumericRange.Exclusive[_]]))
      // Add some maps
      .forSubclass[SortedMap[Any, Any]](new SortedMapSerializer)
      .forTraversableSubclass(ListMap.empty[Any, Any])
      .forTraversableSubclass(HashMap.empty[Any, Any])
      // The above ListMap/HashMap must appear before this:
      .forTraversableSubclass(Map.empty[Any, Any])
      // here are the mutable ones:
      .forTraversableClass(MBitSet.empty, isImmutable = false)
      .forTraversableClass(MHashMap.empty[Any, Any], isImmutable = false)
      .forTraversableClass(MHashSet.empty[Any], isImmutable = false)
      .forTraversableSubclass(MQueue.empty[Any], isImmutable = false)
      .forTraversableSubclass(MMap.empty[Any, Any], isImmutable = false)
      .forTraversableSubclass(MSet.empty[Any], isImmutable = false)
  }
}

class JavaWrapperCollectionRegistrar extends IKryoRegistrar {
  def apply(newK: Kryo) {
    newK.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)
  }
}

/** Registers all the scala (and java) serializers we have */
class AllScalaRegistrar extends IKryoRegistrar {
  def apply(k: Kryo) {
    val col = new ScalaCollectionsRegistrar
    col(k)

    val jcol = new JavaWrapperCollectionRegistrar
    jcol(k)

    // Register all 22 tuple serializers and specialized serializers
    ScalaTupleSerialization.register(k)
    k.forClass[Symbol](new KSerializer[Symbol] {
      override def isImmutable = true
      def write(k: Kryo, out: Output, obj: Symbol) { out.writeString(obj.name) }
      def read(k: Kryo, in: Input, cls: Class[Symbol]) = Symbol(in.readString)
    }).forSubclass[Regex](new RegexSerializer)
      .forClass[ClassTag[Any]](new ClassManifestSerializer[Any])
      .forSubclass[Manifest[Any]](new ManifestSerializer[Any])
      .forSubclass[scala.Enumeration#Value](new EnumerationSerializer)

    // use the singleton serializer for boxed Unit
    val boxedUnit = scala.Unit.box(())
    k.register(boxedUnit.getClass, new SingletonSerializer(boxedUnit))
    new FlinkChillPackageRegistrar().registerSerializers(k)
  }
}
