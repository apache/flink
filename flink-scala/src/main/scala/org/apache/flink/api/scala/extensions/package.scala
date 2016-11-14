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

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions._

/**
  * acceptPartialFunctions extends the original DataSet with methods with unique names
  * that delegate to core higher-order functions (e.g. `map`) so that we can work around
  * the fact that overloaded methods taking functions as parameters can't accept partial
  * functions as well. This enables the possibility to directly apply pattern matching
  * to decompose inputs such as tuples, case classes and collections.
  *
  * The following is a small example that showcases how this extensions would work on
  * a Flink data set:
  *
  * {{{
  *   object Main {
  *     import org.apache.flink.api.scala.extensions._
  *     case class Point(x: Double, y: Double)
  *     def main(args: Array[String]): Unit = {
  *       val env = ExecutionEnvironment.getExecutionEnvironment
  *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
  *       ds.filterWith {
  *         case Point(x, _) => x > 1
  *       }.reduceWith {
  *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
  *       }.mapWith {
  *         case Point(x, y) => (x, y)
  *       }.flatMapWith {
  *         case (x, y) => Seq('x' -> x, 'y' -> y)
  *       }.groupingBy {
  *         case (id, value) => id
  *       }
  *     }
  *   }
  * }}}
  *
  * The extension consists of several implicit conversions over all the data set representations
  * that could gain from this feature. To use this set of extensions methods the user has to
  * explicitly opt-in by importing `org.apache.flink.api.scala.extensions.acceptPartialFunctions`.
  *
  * For more information and usage examples please consult the Apache Flink official documentation.
  *
  */
package object extensions {

  @PublicEvolving
  implicit def acceptPartialFunctions[T](ds: DataSet[T]): OnDataSet[T] =
    new OnDataSet[T](ds)

  @PublicEvolving
  implicit def acceptPartialFunctions[L, R](
      ds: JoinFunctionAssigner[L, R]): OnJoinFunctionAssigner[L, R] =
    new OnJoinFunctionAssigner[L, R](ds)

  @PublicEvolving
  implicit def acceptPartialFunctions[L, R](ds: CrossDataSet[L, R]): OnCrossDataSet[L, R] =
    new OnCrossDataSet[L, R](ds)

  @PublicEvolving
  implicit def acceptPartialFunctions[T](ds: GroupedDataSet[T]): OnGroupedDataSet[T] =
    new OnGroupedDataSet[T](ds)

  @PublicEvolving
  implicit def acceptPartialFunctions[L, R](ds: CoGroupDataSet[L, R]): OnCoGroupDataSet[L, R] =
    new OnCoGroupDataSet[L, R](ds)

  @PublicEvolving
  implicit def acceptPartialFunctions[L, R, O](
      ds: HalfUnfinishedKeyPairOperation[L, R, O]): OnHalfUnfinishedKeyPairOperation[L, R, O] =
    new OnHalfUnfinishedKeyPairOperation[L, R, O](ds)

  @PublicEvolving
  implicit def acceptPartialFunctions[L, R, O](
      ds: UnfinishedKeyPairOperation[L, R, O]): OnUnfinishedKeyPairOperation[L, R, O] =
    new OnUnfinishedKeyPairOperation[L, R, O](ds)

}
