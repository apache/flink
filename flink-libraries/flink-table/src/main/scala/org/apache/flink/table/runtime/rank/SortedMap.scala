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
package org.apache.flink.table.runtime.rank

import java.util
import java.util.function.Supplier
import java.util.{Comparator, Collection => JCollection, Map => JMap, Set => JSet}

import org.apache.flink.table.dataformat.BaseRow

class SortedMap[T](
    sortKeyComparator: Comparator[T],
    valueSupplier: Supplier[JCollection[BaseRow]]) {

  val treeMap: util.TreeMap[T, JCollection[BaseRow]] =
    new util.TreeMap[T, JCollection[BaseRow]](sortKeyComparator)

  var currentTopNum: Int = 0

  /**
    * Puts a record in to the SortedMap under the sortKey,
    * and return the size of the collection under the sortKey.
    */
  def put(sortKey: T, value: BaseRow): Int = {
    currentTopNum += 1
    // update treeMap
    var collection = treeMap.get(sortKey)
    if (collection == null) {
      collection = valueSupplier.get()
      treeMap.put(sortKey, collection)
    }
    collection.add(value)
    collection.size()
  }

  def putAll(sortKey: T, values: JCollection[BaseRow]): Unit = {
    treeMap.put(sortKey, values)
    currentTopNum += values.size()
  }

  def get(sortKey: T): JCollection[BaseRow] = {
    treeMap.get(sortKey)
  }

  def remove(sortKey: T, value: BaseRow): Unit = {
    val list = treeMap.get(sortKey)
    if (list != null) {
      if (list.remove(value)) {
        currentTopNum -= 1
      }
      if (list.size() == 0) {
        treeMap.remove(sortKey)
      }
    }
  }

  def removeAll(sortKey: T): Unit = {
    val list = treeMap.get(sortKey)
    if (list != null) {
      currentTopNum -= list.size()
      treeMap.remove(sortKey)
    }
  }

  def removeLast(): BaseRow = {
    val last = treeMap.lastEntry()
    var lastElement: BaseRow = null
    if (last != null) {
      val list = last.getValue
      lastElement = getLastElement(list)
      if (lastElement != null) {
        if (list.remove(lastElement)) {
          currentTopNum -= 1
        }
        if (list.size() == 0) {
          treeMap.remove(last.getKey)
        }
      }
    }
    lastElement
  }

  def getElement(rank: Int): BaseRow = {
    var curRank = 0
    val iter = treeMap.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val list = entry.getValue

      val listIter = list.iterator()
      while (listIter.hasNext) {
        val elem = listIter.next()
        curRank += 1
        if (curRank == rank) {
          return elem
        }
      }
    }
    null
  }

  def getLastElement(list: JCollection[BaseRow]): BaseRow = {
    var element: BaseRow = null
    if (list != null && !list.isEmpty) {
      val iter = list.iterator()
      while (iter.hasNext) {
        element = iter.next()
      }
    }
    element
  }

  def entrySet(): JSet[JMap.Entry[T, JCollection[BaseRow]]] = treeMap.entrySet()

  def lastEntry(): JMap.Entry[T, JCollection[BaseRow]] = treeMap.lastEntry()

  def containsKey(key: T): Boolean = treeMap.containsKey(key)

  def getCurrentTopNum: Int = currentTopNum
}
