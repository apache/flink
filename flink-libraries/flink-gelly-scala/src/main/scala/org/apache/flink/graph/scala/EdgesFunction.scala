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

package org.apache.flink.graph.scala

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.graph.Edge
import org.apache.flink.util.Collector

abstract class EdgesFunction[K, EV, T] extends org.apache.flink.graph.EdgesFunction[K, EV, T] {

  def iterateEdges(edges: Iterable[(K, Edge[K, EV])], out: Collector[T])

  override def iterateEdges(edges: java.lang.Iterable[Tuple2[K, Edge[K, EV]]], out:
  Collector[T]): Unit = {
    val scalaIterable = scala.collection.JavaConversions.iterableAsScalaIterable(edges)
      .map(jtuple => (jtuple.f0, jtuple.f1))
    iterateEdges(scalaIterable, out)
  }
}
