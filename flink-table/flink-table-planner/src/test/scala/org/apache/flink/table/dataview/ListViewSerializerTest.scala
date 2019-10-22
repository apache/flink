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

package org.apache.flink.table.dataview

import java.lang.Long
import java.util.Random
import org.apache.flink.api.common.typeutils.base.{ListSerializer, LongSerializer}
import org.apache.flink.api.common.typeutils.{SerializerTestBase, TypeSerializer}
import org.apache.flink.table.api.dataview.ListView

/**
  * A test for the [[ListViewSerializer]].
  */
class ListViewSerializerTest extends SerializerTestBase[ListView[Long]] {

  override protected def createSerializer(): TypeSerializer[ListView[Long]] = {
    val listSerializer = new ListSerializer[Long](LongSerializer.INSTANCE)
    new ListViewSerializer[Long](listSerializer)
  }

  override protected def getLength: Int = -1

  override protected def getTypeClass: Class[ListView[Long]] = classOf[ListView[Long]]

  override protected def getTestData: Array[ListView[Long]] = {
    val rnd = new Random(321332)

    // empty
    val listview1 = new ListView[Long]()

    // single element
    val listview2 = new ListView[Long]()
    listview2.add(12345L)

    // multiple elements
    val listview3 = new ListView[Long]()
    var i = 0
    while (i < rnd.nextInt(200)) {
      listview3.add(rnd.nextLong)
      i += 1
    }

    Array[ListView[Long]](listview1, listview2, listview3)
  }
}
