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
package org.apache.flink.table.runtime.functions

import java.util.{LinkedHashMap => JLinkedHashMap}
import java.util.{Map => JMap}

/**
  * Provides a ThreadLocal cache with a maximum cache size per thread.
  * Values must not be null.
  */
abstract class ThreadLocalCache[K, V](val maxSizePerThread: Int) {
  private val cache = new ThreadLocal[BoundedMap[K, V]]

  protected def getNewInstance(key: K): V

  def get(key: K): V = {
    var m = cache.get
    if (m == null) {
      m = new BoundedMap(maxSizePerThread)
      cache.set(m)
    }
    var v = m.get(key)
    if (v == null) {
      v = getNewInstance(key)
      m.put(key, v)
    }
    v
  }
}

private class BoundedMap[K, V](val maxSize: Int) extends JLinkedHashMap[K,V] {
  override protected def removeEldestEntry(eldest: JMap.Entry[K, V]): Boolean = size > maxSize
}
