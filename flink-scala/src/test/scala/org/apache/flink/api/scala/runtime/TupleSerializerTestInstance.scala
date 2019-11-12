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

import java.lang.{Boolean => JBoolean}
import java.util.function.BiFunction

import org.apache.flink.api.common.typeutils.{SerializerTestInstance, TypeSerializer}
import org.apache.flink.testutils.DeeplyEqualsChecker
import org.apache.flink.testutils.DeeplyEqualsChecker.CustomEqualityChecker
import org.junit.Assert._
import org.junit.Test


object TupleSerializerTestInstance {
  val isProduct: BiFunction[AnyRef, AnyRef, JBoolean] =
    new BiFunction[AnyRef, AnyRef, JBoolean] {
      override def apply(o1: scala.AnyRef, o2: scala.AnyRef): JBoolean =
        o1.isInstanceOf[Product] && o2.isInstanceOf[Product]
    }

  val compareProduct: CustomEqualityChecker =
    new CustomEqualityChecker {
      override def check(
          o1: AnyRef,
          o2: AnyRef,
          checker: DeeplyEqualsChecker): Boolean = {
        val p1 = o1.asInstanceOf[Product].productIterator
        val p2 = o2.asInstanceOf[Product].productIterator

        while (p1.hasNext && p2.hasNext) {
          val l = p1.next
          val r = p2.next
          if (!checker.deepEquals(l, r)) {
            return false
          }
        }
        !p1.hasNext && !p2.hasNext
      }
    }
}

class TupleSerializerTestInstance[T <: Product] (
    serializer: TypeSerializer[T],
    typeClass: Class[T],
    length: Int,
    testData: Array[T])
  extends SerializerTestInstance[T](
    new DeeplyEqualsChecker()
      .withCustomCheck(TupleSerializerTestInstance.isProduct,
        TupleSerializerTestInstance.compareProduct),
    serializer,
    typeClass,
    length,
    testData: _*) {

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
}

