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
package org.apache.flink.table.api.runtime.types

import org.apache.flink.annotation.Internal
import org.apache.flink.runtime.checkpoint.{StateObjectCollection, StateObjectCollectionSerializer}
import org.apache.flink.streaming.util.serialize.FlinkChillPackageRegistrar
import org.apache.flink.table.api.runtime.types.FlinkScalaKryoInstantiator.{registerConcreteTraversableClass, useFieldSerializer}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.DefaultSerializers.{BitSetSerializer, VoidSerializer}

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.{HashMap => ImmutableHashMap, HashSet => ImmutableHashSet, NumericRange}
import scala.collection.mutable.{BitSet, HashMap => MutableHashMap, HashSet => MutableHashSet}
import scala.reflect.ClassTag
import scala.runtime.BoxedUnit

/*
This code was copied from Twitter Chill project and modified extensively.
 */

object FlinkScalaKryoInstantiator {
  def useFieldSerializer[T](k: Kryo, cls: Class[T]): Unit = {
    val fs = new com.esotericsoftware.kryo.serializers.FieldSerializer(k, cls)
    // fs.setIgnoreSyntheticFields(false) // scala generates a lot of these attributes
    k.register(cls, fs)
  }

  def registerConcreteTraversableClass[T, C <: Traversable[T]](
      kryo: Kryo,
      c: C with Traversable[T],
      isImmutable: Boolean = true)(implicit cbf: CanBuildFrom[C, T, C]): Unit = {
    // a ClassTag is not used here since its runtimeClass method does not return the concrete internal type
    // that Scala uses for small immutable maps (i.e., scala.collection.immutable.Map$Map1)
    kryo.register(c.getClass, new KryoTraversableSerializer(isImmutable)(cbf))
  }
}

/** Makes an empty instantiator then registers everything. This is called by reflection. */
@Internal
class FlinkScalaKryoInstantiator {
  def newKryo: Kryo = {
    val k = new Kryo
    k.setRegistrationRequired(false)
    k.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy)
    k.register(classOf[Unit], new VoidSerializer)
    // The wrappers are private classes:
    useFieldSerializer(k, List(1, 2, 3).asJava.getClass)
    useFieldSerializer(k, List(1, 2, 3).iterator.asJava.getClass)
    useFieldSerializer(k, Map(1 -> 2, 4 -> 3).asJava.getClass)
    useFieldSerializer(k, new _root_.java.util.ArrayList().asScala.getClass)
    useFieldSerializer(k, new _root_.java.util.HashMap().asScala.getClass)

    k.register(classOf[Some[Any]], new SomeSerializer[Any])
    k.register(classOf[Left[Any, Any]], new LeftSerializer[Any, Any])
    k.register(classOf[Right[Any, Any]], new RightSerializer[Any, Any])

    k.register(classOf[Vector[Any]], new KryoTraversableSerializer[Any, Vector[Any]])
    k.register(Set[Any](1).getClass, new KryoTraversableSerializer[Any, Set[Any]])
    k.register(Set[Any](1, 2).getClass, new KryoTraversableSerializer[Any, Set[Any]])
    k.register(Set[Any](1, 2, 3).getClass, new KryoTraversableSerializer[Any, Set[Any]])
    k.register(Set[Any](1, 2, 3, 4).getClass, new KryoTraversableSerializer[Any, Set[Any]])
    k.register(
      ImmutableHashSet[Any](1, 2, 3, 4, 5).getClass,
      new KryoTraversableSerializer[Any, ImmutableHashSet[Any]])

    registerConcreteTraversableClass(k, Map[Any, Any](1 -> 1))
    registerConcreteTraversableClass(k, Map[Any, Any](1 -> 1, 2 -> 2))
    registerConcreteTraversableClass(k, Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3))
    registerConcreteTraversableClass(k, Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4))
    registerConcreteTraversableClass(
      k,
      ImmutableHashMap[Any, Any](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5))

    k.register(classOf[Range.Inclusive])
    k.register(classOf[NumericRange.Inclusive[_]])
    k.register(classOf[NumericRange.Exclusive[_]])

    k.register(classOf[BitSet], new BitSetSerializer)

    registerConcreteTraversableClass(k, new MutableHashMap[Any, Any])
    registerConcreteTraversableClass(k, new MutableHashSet[Any])

    k.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)

    ScalaTupleSerialization.register(k)

    k.register(classOf[Symbol], new SymbolSerializer)
    k.register(classOf[ClassTag[Any]], new ClassTagSerializer())
    k.register(classOf[BoxedUnit], new SingletonSerializer(()))

    new FlinkChillPackageRegistrar().registerSerializers(k)

    // StateObjectCollection requires a custom serializer for custom new instance instantiation
    // That class is in the flink-runtime project so it makes sense to register it here, rather
    // than in flink-core which doesn't have a dependency on flink-runtime.
    k.addDefaultSerializer(classOf[StateObjectCollection[_]], new StateObjectCollectionSerializer())

    k
  }
}
