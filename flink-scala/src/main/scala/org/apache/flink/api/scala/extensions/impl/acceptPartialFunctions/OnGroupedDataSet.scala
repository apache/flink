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
package org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, GroupedDataSet}
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Wraps a grouped data set, allowing to use anonymous partial functions to
  * perform extraction of items in a tuple, case class instance or collection
  *
  * @param ds The wrapped grouped data set
  * @tparam T The type of the grouped data set items
  */
class OnGroupedDataSet[T](ds: GroupedDataSet[T]) {

  /**
    * Sorts a group using a sorting function `fun` and an `Order`
    *
    * @param fun The sorting function, defining the sorting key
    * @param order The ordering strategy (ascending, descending, etc.)
    * @tparam K The key type
    * @return A data set sorted group-wise
    */
  @PublicEvolving
  def sortGroupWith[K: TypeInformation](order: Order)(fun: T => K): GroupedDataSet[T] =
    ds.sortGroup(fun, order)

  /**
    * Reduces the whole data set with a reducer `fun`
    *
    * @param fun The reducing function
    * @return A reduced data set of Ts
    */
  @PublicEvolving
  def reduceWith(fun: (T, T) => T): DataSet[T] =
    ds.reduce(fun)

  /**
    * Reduces the data set group-wise with a reducer `fun`
    *
    * @param fun The reducing function
    * @tparam R The type of the items in the resulting data set
    * @return A data set of Rs reduced group-wise
    */
  @PublicEvolving
  def reduceGroupWith[R: TypeInformation: ClassTag](fun: Stream[T] => R): DataSet[R] =
    ds.reduceGroup {
      (it: Iterator[T], out: Collector[R]) =>
        out.collect(fun(it.toStream))
    }

  /**
    * Same as a reducing operation but only acts locally,
    * ideal to perform pre-aggregation before a reduction.
    *
    * @param fun The reducing function
    * @tparam R The type of the items in the resulting data set
    * @return A data set of Rs reduced group-wise
    */
  @PublicEvolving
  def combineGroupWith[R: TypeInformation: ClassTag](fun: Stream[T] => R): DataSet[R] =
    ds.combineGroup {
      (it: Iterator[T], out: Collector[R]) =>
        out.collect(fun(it.toStream))
    }

}
