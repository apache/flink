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

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}

import java.{lang, util}

/*
This code was copied as is from Twitter Chill 0.7.4 and modified to use Kryo 5.x
 */

/**
 * A Kryo serializer for serializing results returned by asJavaIterable.
 *
 * The underlying object is scala.collection.convert.Wrappers$IterableWrapper. Kryo deserializes
 * this into an AbstractCollection, which unfortunately doesn't work.
 *
 * Ported from Apache Spark's KryoSerializer.scala.
 */

private class JavaIterableWrapperSerializer extends Serializer[lang.Iterable[_]] {
  override def write(kryo: Kryo, output: KryoOutput, t: lang.Iterable[_]): Unit = {
    // If the object is the wrapper, simply serialize the underlying Scala Iterable object.
    // Otherwise, serialize the object itself.
    if (
      t.getClass == JavaIterableWrapperSerializer.wrapperClass && JavaIterableWrapperSerializer.underlyingMethodOpt.isDefined
    ) {
      kryo.writeClassAndObject(
        output,
        JavaIterableWrapperSerializer.underlyingMethodOpt.get.invoke(t))
    } else {
      kryo.writeClassAndObject(output, t)
    }
  }

  override def read(
      kryo: Kryo,
      input: KryoInput,
      aClass: Class[_ <: lang.Iterable[_]]): lang.Iterable[_] = {
    kryo.readClassAndObject(input) match {
      case scalaIterable: Iterable[_] =>
        scala.collection.JavaConversions.asJavaIterable(scalaIterable)
      case javaIterable: lang.Iterable[_] =>
        javaIterable
    }
  }
}

private object JavaIterableWrapperSerializer {
  // The class returned by asJavaIterable (scala.collection.convert.Wrappers$IterableWrapper).
  val wrapperClass = scala.collection.JavaConversions.asJavaIterable(Seq(1)).getClass

  // Get the underlying method so we can use it to get the Scala collection for serialization.
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying"))
    catch {
      case e: Exception =>
        None
    }
  }
}
