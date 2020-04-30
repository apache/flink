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

package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, ResultTypeQueryable}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.TypeExtractionTest.{CustomBeanClass, CustomTypeInputFormat}
import org.apache.flink.util.TestLogger
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike

import scala.beans.BeanProperty


class TypeExtractionTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testResultTypeQueryable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val producedType = env.createInput(new CustomTypeInputFormat).getType()
    assertEquals(producedType, BasicTypeInfo.LONG_TYPE_INFO)
  }

  @Test
  def testBeanPropertyClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val producedType = env.fromElements(new CustomBeanClass()).getType()
    assertTrue(producedType.isInstanceOf[PojoTypeInfo[_]])
    val pojoTypeInfo = producedType.asInstanceOf[PojoTypeInfo[_]]
    assertEquals(pojoTypeInfo.getTypeAt(0), BasicTypeInfo.INT_TYPE_INFO)
    assertEquals(pojoTypeInfo.getTypeAt(1), BasicTypeInfo.LONG_TYPE_INFO)
  }

}

object TypeExtractionTest {
  class CustomTypeInputFormat extends FileInputFormat[String] with ResultTypeQueryable[Long] {

    override def getProducedType: TypeInformation[Long] =
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]

    override def reachedEnd(): Boolean = throw new UnsupportedOperationException()

    override def nextRecord(reuse: String): String = throw new UnsupportedOperationException()
  }

  class CustomBeanClass(
      @BeanProperty var prop: Int,
      var prop2: Long) {
    def this() = this(0, 0L)
  }
}
