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
package org.apache.flink.table.typeutils

import java.util.{Collections, HashMap => JHashMap, Random, Map => JMap}
import java.lang.{Long => JLong}

import org.apache.flink.api.common.typeutils.base.{LongSerializer, StringSerializer}
import org.apache.flink.api.common.typeutils.{SerializerTestBase, TypeSerializer}

class NullAwareMapSerializerTest extends SerializerTestBase[JMap[String, JLong]]{
  
  override protected def createSerializer(): TypeSerializer[JMap[String, JLong]] = {
    new NullAwareMapSerializer[String, JLong](StringSerializer.INSTANCE, LongSerializer.INSTANCE)
  }

  override protected def getLength: Int = -1

  override protected def getTypeClass: Class[JMap[String, JLong]] = classOf[JMap[String, JLong]]

  override protected def getTestData: Array[JMap[String, JLong]] = {
    val rnd = new Random(123654789)
    // empty maps
    val map1: JMap[String, JLong] = Collections.emptyMap[String, JLong]
    val map2: JMap[String, JLong] = new JHashMap[String, JLong]

    // single element maps
    val map4: JMap[String, JLong] = Collections.singletonMap("hello", 0L)
    val map5: JMap[String, JLong] = new JHashMap[String, JLong]
    map5.put("12345L", 12345L)
    val map6: JMap[String, JLong] = new JHashMap[String, JLong]
    map6.put("777888L", 777888L)

    // longer maps
    val map7: JMap[String, JLong] = new JHashMap[String, JLong]
    for (i <- 0 until rnd.nextInt(200)) {
      map7.put(JLong.toString(rnd.nextLong), rnd.nextLong)
    }

    // null-value maps
    val map9: JMap[String, JLong] = Collections.singletonMap("0", null)
    val map10: JMap[String, JLong] = new JHashMap[String, JLong]
    map10.put("999", null)
    val map11: JMap[String, JLong] = new JHashMap[String, JLong]
    map11.put("666", null)

    // null-key maps
    val map12: JMap[String, JLong] = Collections.singletonMap(null, 0L)
    val map13: JMap[String, JLong] = new JHashMap[String, JLong]
    map13.put(null, 999L)
    val map14: JMap[String, JLong] = new JHashMap[String, JLong]
    map14.put(null, 666L)
    val map15: JMap[String, JLong] = Collections.singletonMap(null, null)

    Array[JMap[_, _]](map1, map2, map4, map5, map6, map7, map9,
                      map10, map11, map12, map13, map14, map15)
      .asInstanceOf[Array[JMap[String, JLong]]]
  }
}
