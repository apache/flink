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
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeComparator}
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.runtime.tuple.base.TupleComparatorTestBase


class TupleComparatorILDC3Test extends TupleComparatorTestBase[(Int, Long, Double)] {

  protected def createComparator(ascending: Boolean): TypeComparator[(Int, Long, Double)] = {
    val ti = createTypeInformation[(Int, Long, Double)]
    ti.asInstanceOf[TupleTypeInfoBase[(Int, Long, Double)]]
      .createComparator(
        Array(2, 0, 1),
        Array(ascending, ascending, ascending),
        0,
        new ExecutionConfig)
  }

  protected def createSerializer: TypeSerializer[(Int, Long, Double)] = {
    val ti = createTypeInformation[(Int, Long, Double)]
    ti.createSerializer(new ExecutionConfig)
  }

  protected def getSortedTestData: Array[(Int, Long, Double)] = {
    dataISD
  }

  private val dataISD = Array(
    (4, 4L, 20.0),
    (5, 1L, 20.0),
    (5, 2L, 20.0),
    (5, 10L, 23.0),
    (5, 19L, 24.0),
    (5, 20L, 24.0),
    (5, 24L, 25.0),
    (5, 25L, 25.0)
  )
}

