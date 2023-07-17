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
package org.apache.flink.api.scala

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.SortPartitionOperator

import scala.reflect.ClassTag

/**
 * The result of [[DataSet.sortPartition]]. This can be used to append additional sort fields to the
 * one sort-partition operator.
 *
 * @tparam T
 *   The type of the DataSet, i.e., the type of the elements of the DataSet.
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@Public
class PartitionSortedDataSet[T: ClassTag](set: SortPartitionOperator[T]) extends DataSet[T](set) {

  /** Appends the given field and order to the sort-partition operator. */
  override def sortPartition(field: Int, order: Order): DataSet[T] = {
    if (set.useKeySelector()) {
      throw new InvalidProgramException(
        "Expression keys cannot be appended after selector " +
          "function keys")
    }

    this.set.sortPartition(field, order)
    this
  }

  /** Appends the given field and order to the sort-partition operator. */
  override def sortPartition(field: String, order: Order): DataSet[T] = {
    if (set.useKeySelector()) {
      throw new InvalidProgramException(
        "Expression keys cannot be appended after selector " +
          "function keys")
    }

    this.set.sortPartition(field, order)
    this
  }

  override def sortPartition[K: TypeInformation](fun: T => K, order: Order): DataSet[T] = {
    throw new InvalidProgramException("KeySelector cannot be chained.")
  }

}
