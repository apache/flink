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

import org.apache.flink.api.common.typeutils.{GenericPairComparator, TypeComparator, TypeSerializer}
import org.apache.flink.api.common.typeutils.base.{DoubleComparator, DoubleSerializer, IntComparator, IntSerializer}

import org.apache.flink.api.java.typeutils.runtime.TupleComparator
import org.apache.flink.api.scala.runtime.tuple.base.PairComparatorTestBase
import org.apache.flink.api.scala.typeutils.CaseClassComparator

class GenericPairComparatorTest
  extends PairComparatorTestBase[(Int, String, Double), (Int, Float, Long, Double)] {

  override protected def createComparator(ascending: Boolean):
  GenericPairComparator[(Int, String, Double), (Int, Float, Long, Double)] = {
    val fields1  = Array[Int](0, 2)
    val fields2 = Array[Int](0, 3)

    val comps1 =
      Array[TypeComparator[_]](new IntComparator(ascending), new DoubleComparator(ascending))
    val comps2 =
      Array[TypeComparator[_]](new IntComparator(ascending), new DoubleComparator(ascending))

    val sers1 =
      Array[TypeSerializer[_]](IntSerializer.INSTANCE, DoubleSerializer.INSTANCE)
    val sers2 =
      Array[TypeSerializer[_]](IntSerializer.INSTANCE, DoubleSerializer.INSTANCE)

    val comp1 = new CaseClassComparator[(Int, String, Double)](fields1, comps1, sers1)
    val comp2 = new CaseClassComparator[(Int, Float, Long, Double)](fields2, comps2, sers2)

    new GenericPairComparator[(Int, String, Double), (Int, Float, Long, Double)](comp1, comp2)
  }

  protected def getSortedTestData:
  (Array[(Int, String, Double)], Array[(Int, Float, Long, Double)]) = {
    (dataISD, dataIDL)
  }

  private val dataISD = Array(
    (4, "hello", 20.0),
    (4, "world", 23.2),
    (5, "hello", 18.0),
    (5, "world", 19.2),
    (6, "hello", 16.0),
    (6, "world", 17.2),
    (7, "hello", 14.0),
    (7,"world", 15.2))

  private val dataIDL = Array(
    (4, 0.11f, 14L, 20.0),
    (4, 0.221f, 15L, 23.2),
    (5, 0.33f, 15L, 18.0),
    (5, 0.44f, 20L, 19.2),
    (6, 0.55f, 20L, 16.0),
    (6, 0.66f, 29L, 17.2),
    (7, 0.77f, 29L, 14.0),
    (7, 0.88f, 34L, 15.2))
}

