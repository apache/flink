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

package org.apache.flink.table.util

import java.util.Optional
import java.util.function.{BiConsumer, Consumer, Function}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.util.{List => JList}
import scala.collection.JavaConverters._

/**
  * Utilities for interoperability between Scala and Java classes.
  */
object JavaScalaConversionUtil {

  // most of these methods are not necessary once we upgraded to Scala 2.12

  def toJava[T](option: Option[T]): Optional[T] = option match {
    case Some(v) => Optional.of(v)
    case None => Optional.empty()
  }

  def toScala[T](option: Optional[T]): Option[T] = Option(option.orElse(null.asInstanceOf[T]))

  def toJava[T](func: (T) => Unit): Consumer[T] = new Consumer[T] {
    override def accept(t: T): Unit = {
      func.apply(t)
    }
  }

  def toJava[K, V](func: (K, V) => Unit): BiConsumer[K, V] = new BiConsumer[K, V] {
    override def accept(k: K, v: V): Unit = {
      func.apply(k ,v)
    }
  }

  def toJava[I, O](func: (I) => O): Function[I, O] = new Function[I, O] {
    override def apply(in: I): O = {
      func.apply(in)
    }
  }

  def toJava[T0, T1](tuple: (T0, T1)): JTuple2[T0, T1] = {
    new JTuple2[T0, T1](tuple._1, tuple._2)
  }

  def toJava[T](seq: Seq[T]): JList[T] = {
    seq.asJava
  }
}
