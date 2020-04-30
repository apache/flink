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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{CompositeType, TypeComparator}
import org.apache.flink.api.scala._
import org.apache.flink.core.memory._
import org.apache.flink.runtime.operators.sort.{NormalizedKeySorter, QuickSort}

import java.util
import java.util.Random

import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito

case class CaseTestClass(a: Int, b: Int, c: Int, d: String)

/** Test that verifies that the case class comparators properly
 * generate normalized keys used during sorting.
 */
class CaseClassNormalizedKeySortingTest {

  @Test
  def testNormalizedKeyGeneration(): Unit = {
    val typeInfo = implicitly[TypeInformation[CaseTestClass]]
      .asInstanceOf[CompositeType[CaseTestClass]]

    val serializer = typeInfo.createSerializer(new ExecutionConfig)
    val comparator = new FailingCompareDeserializedWrapper(
      typeInfo.createComparator(
        Array[Int](0, 2),
        Array[Boolean](true, true),
        0,
        new ExecutionConfig))

    assertTrue(comparator.supportsNormalizedKey())
    assertEquals(8, comparator.getNormalizeKeyLen())
    assertFalse(comparator.isNormalizedKeyPrefixOnly(8))

    // validate the failing mock
    {
      val in1 : DataInputView = Mockito.mock(classOf[DataInputView])
      val in2 : DataInputView = Mockito.mock(classOf[DataInputView])

      try {
        comparator.compareSerialized(in1, in2)
        fail("should throw an exception")
      }
      catch {
        case e: UnsupportedOperationException => // fine
        case ee: Exception => fail("unexpected exception")
      }
    }


    val numMemSegs = 20
    val memory : util.List[MemorySegment] = new util.ArrayList[MemorySegment](numMemSegs)
    for (i <- 1 to numMemSegs) {
      memory.add(MemorySegmentFactory.allocateUnpooledSegment(32*1024))
    }

    val sorter : NormalizedKeySorter[CaseTestClass] = new NormalizedKeySorter[CaseTestClass](
      serializer, comparator, memory)

    val rnd = new Random()
    var moreToGo = true
    var num = 0

    while (moreToGo) {
      val next = CaseTestClass(rnd.nextInt(), rnd.nextInt(), rnd.nextInt(), "")
      moreToGo = sorter.write(next)
      num += 1
    }

    new QuickSort().sort(sorter)
  }

  class FailingCompareDeserializedWrapper[T](wrapped: TypeComparator[T]) extends TypeComparator[T] {

    def hash(record: T) : Int = wrapped.hash(record)

    def setReference(toCompare: T) = wrapped.setReference(toCompare)

    def equalToReference(candidate: T): Boolean = wrapped.equalToReference(candidate)

    def compareToReference(referencedComparator: TypeComparator[T]): Int
    = wrapped.compareToReference(referencedComparator)

    override def supportsCompareAgainstReference(): Boolean
    = wrapped.supportsCompareAgainstReference()

    def compare(first: T, second: T): Int = wrapped.compare(first, second)

    def compareSerialized(firstSource: DataInputView, secondSource: DataInputView): Int = {
      throw new UnsupportedOperationException("Not Supported")
    }

    def supportsNormalizedKey(): Boolean = wrapped.supportsNormalizedKey()

    def supportsSerializationWithKeyNormalization(): Boolean
    = wrapped.supportsSerializationWithKeyNormalization()

    def getNormalizeKeyLen(): Int = wrapped.getNormalizeKeyLen()

    def isNormalizedKeyPrefixOnly(keyBytes: Int): Boolean
    = wrapped.isNormalizedKeyPrefixOnly(keyBytes)

    def putNormalizedKey(record: T, target: MemorySegment, offset: Int, numBytes: Int): Unit
    = wrapped.putNormalizedKey(record, target, offset, numBytes)

    def writeWithKeyNormalization(record: T, target: DataOutputView): Unit
    = wrapped.writeWithKeyNormalization(record, target)

    def readWithKeyDenormalization(reuse: T, source: DataInputView): T
    = wrapped.readWithKeyDenormalization(reuse, source)

    def invertNormalizedKey(): Boolean = wrapped.invertNormalizedKey()

    def duplicate(): TypeComparator[T] = new FailingCompareDeserializedWrapper(wrapped.duplicate())

    def extractKeys(record: Object, target: Array[Object], index: Int): Int
    = wrapped.extractKeys(record, target, index)

    def getFlatComparators(): Array[TypeComparator[_]] = wrapped.getFlatComparators()

    override def compareAgainstReference(keys: Array[Comparable[_]]): Int = {
      throw new UnsupportedOperationException("Workaround hack.")
    }
  }
}
