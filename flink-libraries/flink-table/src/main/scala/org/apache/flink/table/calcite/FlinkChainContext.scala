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

package org.apache.flink.table.calcite

import java.util

import com.google.common.base.Preconditions
import com.google.common.collect.Lists
import org.apache.calcite.plan.Context
import org.apache.calcite.plan.Contexts.EMPTY_CONTEXT

import scala.collection.JavaConversions._

/** Context that wraps a chain of contexts. */
class FlinkChainContext(val contexts: util.List[Context]) extends Context {
  Preconditions.checkNotNull(contexts)
  contexts.foreach { context =>
    require(!context.isInstanceOf[FlinkChainContext], "must be flat")
  }

  override def unwrap[T](clazz: Class[T]): T = {
    for (context <- contexts) {
      val t: T = context.unwrap(clazz)
      if (t != null) return t
    }
    null.asInstanceOf[T]
  }

  def load(ct: Context): Unit = contexts.add(ct)

  def unload[T](clazz: Class[T]): Unit = {
    val iterator: util.Iterator[Context] = contexts.iterator
    while (iterator.hasNext) {
      val t: T = iterator.next.unwrap(clazz)
      if (t != null) {
        iterator.remove()
        return
      }
    }
  }
}

object FlinkChainContext {
  def chain(contexts: Context*): Context = {
    // Flatten any chain contexts in the list, and remove duplicates
    val list: util.List[Context] = Lists.newArrayList()
    contexts.foreach(build(list, _))
    new FlinkChainContext(list)
  }

  /** Recursively populates a list of contexts. */
  private def build(list: util.List[Context], context: Context): Unit = {
    if ((context eq EMPTY_CONTEXT) || list.contains(context)) return
    context match {
      case chainContext: FlinkChainContext =>
        chainContext.contexts.foreach(build(list, _))
      case _ => list.add(context)
    }
  }

}
