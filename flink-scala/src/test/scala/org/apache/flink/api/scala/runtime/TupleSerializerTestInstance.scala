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
package org.apache.flink.api.scala.runtime

import org.junit.Assert._
import org.apache.flink.api.common.typeutils.SerializerTestInstance
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.junit.Assert
import org.junit.Test


class TupleSerializerTestInstance[T <: Product] (
    serializer: TypeSerializer[T],
    typeClass: Class[T],
    length: Int,
    testData: Array[T])
  extends SerializerTestInstance[T](serializer, typeClass, length, testData: _*) {

  @Test
  override def testInstantiate(): Unit = {
    try {
      val serializer: TypeSerializer[T] = getSerializer
      val instance: T = serializer.createInstance
      assertNotNull("The created instance must not be null.", instance)
      val tpe: Class[T] = getTypeClass
      assertNotNull("The test is corrupt: type class is null.", tpe)
      // We cannot check this because Tuple1 instances are not actually of type Tuple1
      // but something like Tuple1$mcI$sp
//      assertEquals("Type of the instantiated object is wrong.", tpe, instance.getClass)
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  protected override def deepEquals(message: String, shouldTuple: T, isTuple: T) {
    Assert.assertEquals(shouldTuple.productArity, isTuple.productArity)
    for (i <- 0 until shouldTuple.productArity) {
      val should = shouldTuple.productElement(i)
      val is = isTuple.productElement(i)
      if (should.getClass.isArray) {
        should match {
          case booleans: Array[Boolean] =>
            Assert.assertTrue(message, booleans.sameElements(is.asInstanceOf[Array[Boolean]]))
          case bytes: Array[Byte] =>
            assertArrayEquals(message, bytes, is.asInstanceOf[Array[Byte]])
          case shorts: Array[Short] =>
            assertArrayEquals(message, shorts, is.asInstanceOf[Array[Short]])
          case ints: Array[Int] =>
            assertArrayEquals(message, ints, is.asInstanceOf[Array[Int]])
          case longs: Array[Long] =>
            assertArrayEquals(message, longs, is.asInstanceOf[Array[Long]])
          case floats: Array[Float] =>
            assertArrayEquals(message, floats, is.asInstanceOf[Array[Float]], 0.0f)
          case doubles: Array[Double] =>
            assertArrayEquals(message, doubles, is.asInstanceOf[Array[Double]], 0.0)
          case chars: Array[Char] =>
            assertArrayEquals(message, chars, is.asInstanceOf[Array[Char]])
          case _ =>
            assertArrayEquals(
              message,
              should.asInstanceOf[Array[AnyRef]],
              is.asInstanceOf[Array[AnyRef]])
        }
      } else {
        assertEquals(message, should, is)
      }
    }
  }
}

