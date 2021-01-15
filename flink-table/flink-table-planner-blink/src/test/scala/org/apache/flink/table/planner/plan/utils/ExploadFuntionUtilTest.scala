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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.types.Row

import org.junit.Assert.assertTrue
import org.junit.Test

import java.util

/**
 * Test for functions in [[ExploadFuntionUti]].
 */
class ExploadFuntionUtilTest {

  @Test
  def testObjectExplodeTableFunc(): Unit = {
    val func = new ObjectExplodeTableFunc(Types.STRING)
    val list = new util.ArrayList[Object]()
    func.setCollector(new ListCollector[Object](list))
    func.eval(null.asInstanceOf[Array[Object]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Object, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testFloatExplodeTableFunc(): Unit = {
    val func = new FloatExplodeTableFunc()
    val list = new util.ArrayList[Float]()
    func.setCollector(new ListCollector[Float](list))
    func.eval(null.asInstanceOf[Array[Float]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Float, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testShortExplodeTableFunc(): Unit = {
    val func = new ShortExplodeTableFunc()
    val list = new util.ArrayList[Short]()
    func.setCollector(new ListCollector[Short](list))
    func.eval(null.asInstanceOf[Array[Short]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Short, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testIntExplodeTableFunc(): Unit = {
    val func = new IntExplodeTableFunc()
    val list = new util.ArrayList[Int]()
    func.setCollector(new ListCollector[Int](list))
    func.eval(null.asInstanceOf[Array[Int]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Int, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testLongExplodeTableFunc(): Unit = {
    val func = new LongExplodeTableFunc()
    val list = new util.ArrayList[Long]()
    func.setCollector(new ListCollector[Long](list))
    func.eval(null.asInstanceOf[Array[Long]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Long, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testDoubleExplodeTableFunc(): Unit = {
    val func = new DoubleExplodeTableFunc()
    val list = new util.ArrayList[Double]()
    func.setCollector(new ListCollector[Double](list))
    func.eval(null.asInstanceOf[Array[Double]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Double, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testByteExplodeTableFunc(): Unit = {
    val func = new ByteExplodeTableFunc()
    val list = new util.ArrayList[Byte]()
    func.setCollector(new ListCollector[Byte](list))
    func.eval(null.asInstanceOf[Array[Byte]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Byte, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testBooleanExplodeTableFunc(): Unit = {
    val func = new BooleanExplodeTableFunc()
    val list = new util.ArrayList[Boolean]()
    func.setCollector(new ListCollector[Boolean](list))
    func.eval(null.asInstanceOf[Array[Boolean]])
    assertTrue(list.isEmpty)
    func.eval(null.asInstanceOf[util.Map[Boolean, Integer]])
    assertTrue(list.isEmpty)
  }

  @Test
  def testMapExplodeTableFunc(): Unit = {
    val func = new MapExplodeTableFunc()
    val list = new util.ArrayList[Row]()
    func.setCollector(new ListCollector[Row](list))
    func.eval(null.asInstanceOf[util.Map[Object, Object]])
  }
}
