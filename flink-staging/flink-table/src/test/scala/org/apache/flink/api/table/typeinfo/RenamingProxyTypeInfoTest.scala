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

package org.apache.flink.api.table.typeinfo

import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.util.TestLogger
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike

class RenamingProxyTypeInfoTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testRenamingProxyTypeEquality(): Unit = {
    val pojoTypeInfo1 = TypeExtractor.createTypeInfo(classOf[TestPojo])
      .asInstanceOf[CompositeType[TestPojo]]

    val tpeInfo1 = new RenamingProxyTypeInfo[TestPojo](
      pojoTypeInfo1,
      Array("someInt", "aString", "doubleArray"))

    val tpeInfo2 = new RenamingProxyTypeInfo[TestPojo](
      pojoTypeInfo1,
      Array("someInt", "aString", "doubleArray"))

    assert(tpeInfo1.equals(tpeInfo2))
    assert(tpeInfo1.hashCode() == tpeInfo2.hashCode())
  }

  @Test
  def testRenamingProxyTypeInequality(): Unit = {
    val pojoTypeInfo1 = TypeExtractor.createTypeInfo(classOf[TestPojo])
      .asInstanceOf[CompositeType[TestPojo]]

    val tpeInfo1 = new RenamingProxyTypeInfo[TestPojo](
      pojoTypeInfo1,
      Array("someInt", "aString", "doubleArray"))

    val tpeInfo2 = new RenamingProxyTypeInfo[TestPojo](
      pojoTypeInfo1,
      Array("foobar", "aString", "doubleArray"))

    assert(!tpeInfo1.equals(tpeInfo2))
  }
}

final class TestPojo {
  var someInt: Int = 0
  private var aString: String = null
  var doubleArray: Array[Double] = null

  def setaString(aString: String) {
    this.aString = aString
  }

  def getaString: String = {
    return aString
  }
}
