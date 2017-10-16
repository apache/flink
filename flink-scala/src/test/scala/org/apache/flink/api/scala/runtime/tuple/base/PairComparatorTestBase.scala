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
package org.apache.flink.api.scala.runtime.tuple.base

import org.apache.flink.util.TestLogger
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.apache.flink.api.common.typeutils.TypePairComparator
import org.junit.Test

/**
 * Abstract test base for PairComparators.
 */
abstract class PairComparatorTestBase[T, R] extends TestLogger {
  protected def createComparator(ascending: Boolean): TypePairComparator[T, R]

  protected def getSortedTestData: (Array[T], Array[R])

  @Test
  def testEqualityWithReference(): Unit = {
    try {
      val comparator = getComparator(ascending = true)
      
      val (dataT, dataR) = getSortedData
      for (i <- 0 until dataT.length) {
        comparator.setReference(dataT(i))
        assertTrue(comparator.equalToReference(dataR(i)))
      }
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  @Test
  def testInequalityWithReference(): Unit = {
    testGreatSmallAscDescWithReference(ascending = true)
    testGreatSmallAscDescWithReference(ascending = false)
  }

  protected def testGreatSmallAscDescWithReference(ascending: Boolean) {
    try {
      val (dataT, dataR) = getSortedData
      val comparator = getComparator(ascending)
      for (x <- 0 until (dataT.length - 1)) {
        for (y <- (x + 1) until dataR.length) {
          comparator.setReference(dataT(x))
          if (ascending) {
            assertTrue(comparator.compareToReference(dataR(y)) > 0)
          }
          else {
            assertTrue(comparator.compareToReference(dataR(y)) < 0)
          }
        }
      }
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  protected def getComparator(ascending: Boolean): TypePairComparator[T, R] = {
    val comparator: TypePairComparator[T, R] = createComparator(ascending)
    if (comparator == null) {
      throw new RuntimeException("Test case corrupt. Returns null as comparator.")
    }
    comparator
  }

  protected def getSortedData: (Array[T], Array[R]) = {
    val (dataT, dataR) = getSortedTestData

    if (dataT == null || dataR == null) {
      throw new RuntimeException("Test case corrupt. Returns null as test data.")
    }
    if (dataT.length < 2 || dataR.length < 2) {
      throw new RuntimeException("Test case does not provide enough sorted test data.")
    }
    (dataT, dataR)
  }
}

