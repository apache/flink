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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, GroupedDataSet}

import scala.reflect.ClassTag

/**
  * Wraps a data set, allowing to use anonymous partial functions to
  * perform extraction of items in a tuple, case class instance or collection
  *
  * @param ds The wrapped data set
  * @tparam T The type of the data set items
  */
class OnDataSet[T](ds: DataSet[T]) {

  /**
    * Applies a function `fun` to each item of the data set
    *
    * @param fun The function to be applied to each item
    * @tparam R The type of the items in the returned data set
    * @return A dataset of R
    */
  @PublicEvolving
  def mapWith[R: TypeInformation: ClassTag](fun: T => R): DataSet[R] =
    ds.map(fun)

  /**
    * Applies a function `fun` to a partition as a whole
    *
    * @param fun The function to be applied on the whole partition
    * @tparam R The type of the items in the returned data set
    * @return A dataset of R
    */
  @PublicEvolving
  def mapPartitionWith[R: TypeInformation: ClassTag](fun: Stream[T] => R): DataSet[R] =
    ds.mapPartition {
      (it, out) =>
        out.collect(fun(it.toStream))
    }

  /**
    * Applies a function `fun` to each item of the dataset, producing a collection of items
    * that will be flattened in the resulting data set
    *
    * @param fun The function to be applied to each item
    * @tparam R The type of the items in the returned data set
    * @return A dataset of R
    */
  @PublicEvolving
  def flatMapWith[R: TypeInformation: ClassTag](fun: T => TraversableOnce[R]): DataSet[R] =
    ds.flatMap(fun)

  /**
    * Applies a predicate `fun` to each item of the data set, keeping only those for which
    * the predicate holds
    *
    * @param fun The predicate to be tested on each item
    * @return A dataset of R
    */
  @PublicEvolving
  def filterWith(fun: T => Boolean): DataSet[T] =
    ds.filter(fun)

  /**
    * Applies a reducer `fun` to the data set
    *
    * @param fun The reducing function to be applied on the whole data set
    * @return A data set of Rs
    */
  @PublicEvolving
  def reduceWith(fun: (T, T) => T): DataSet[T] =
    ds.reduce(fun)

  /**
    * Applies a reducer `fun` to a grouped data set
    *
    * @param fun The function to be applied to the whole grouping
    * @tparam R The type of the items in the returned data set
    * @return A dataset of Rs
    */
  @PublicEvolving
  def reduceGroupWith[R: TypeInformation: ClassTag](fun: Stream[T] => R): DataSet[R] =
    ds.reduceGroup {
      (it, out) =>
        out.collect(fun(it.toStream))
    }

  /**
    * Groups the items according to a grouping function `fun`
    *
    * @param fun The grouping function
    * @tparam K The return type of the grouping function, for which type information must be known
    * @return A grouped data set of Ts
    */
  @PublicEvolving
  def groupingBy[K: TypeInformation](fun: T => K): GroupedDataSet[T] =
    ds.groupBy(fun)

}
