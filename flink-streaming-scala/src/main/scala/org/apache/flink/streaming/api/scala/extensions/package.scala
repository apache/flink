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
package org.apache.flink.streaming.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions._
import org.apache.flink.streaming.api.windowing.windows.Window

package object extensions {

  /**
    * acceptPartialFunctions extends the original DataStream with methods with unique names
    * that delegate to core higher-order functions (e.g. `map`) so that we can work around
    * the fact that overloaded methods taking functions as parameters can't accept partial
    * functions as well. This enables the possibility to directly apply pattern matching
    * to decompose inputs such as tuples, case classes and collections.
    *
    * e.g.
    * {{{
    *   object Main {
    *     import org.apache.flink.api.scala.extensions._
    *     case class Point(x: Double, y: Double)
    *     def main(args: Array[String]): Unit = {
    *       val env = StreamExecutionEnvironment.getExecutionEnvironment
    *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    *       ds.filterWith {
    *         case Point(x, _) => x > 1
    *       }.reduceWith {
    *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    *       }.mapWith {
    *         case Point(x, y) => (x, y)
    *       }.flatMapWith {
    *         case (x, y) => Seq('x' -> x, 'y' -> y)
    *       }.keyingBy {
    *         case (id, value) => id
    *       }
    *     }
    *   }
    * }}}
    *
    */
  implicit def acceptPartialFunctionsOnDataStream[T: TypeInformation](ds: DataStream[T]):
      OnDataStream[T] =
    new OnDataStream[T](ds)

  /**
    * acceptPartialFunctions extends the original DataStream with methods with unique names
    * that delegate to core higher-order functions (e.g. `map`) so that we can work around
    * the fact that overloaded methods taking functions as parameters can't accept partial
    * functions as well. This enables the possibility to directly apply pattern matching
    * to decompose inputs such as tuples, case classes and collections.
    *
    * e.g.
    * {{{
    *   object Main {
    *     import org.apache.flink.api.scala.extensions._
    *     case class Point(x: Double, y: Double)
    *     def main(args: Array[String]): Unit = {
    *       val env = StreamExecutionEnvironment.getExecutionEnvironment
    *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    *       ds.filterWith {
    *         case Point(x, _) => x > 1
    *       }.reduceWith {
    *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    *       }.mapWith {
    *         case Point(x, y) => (x, y)
    *       }.flatMapWith {
    *         case (x, y) => Seq('x' -> x, 'y' -> y)
    *       }.keyingBy {
    *         case (id, value) => id
    *       }
    *     }
    *   }
    * }}}
    *
    */
  implicit def acceptPartialFunctionsOnKeyedStream[T: TypeInformation, K](ds: KeyedStream[T, K]):
      OnKeyedStream[T, K] =
    new OnKeyedStream[T, K](ds)

  /**
    * acceptPartialFunctions extends the original DataStream with methods with unique names
    * that delegate to core higher-order functions (e.g. `map`) so that we can work around
    * the fact that overloaded methods taking functions as parameters can't accept partial
    * functions as well. This enables the possibility to directly apply pattern matching
    * to decompose inputs such as tuples, case classes and collections.
    *
    * e.g.
    * {{{
    *   object Main {
    *     import org.apache.flink.api.scala.extensions._
    *     case class Point(x: Double, y: Double)
    *     def main(args: Array[String]): Unit = {
    *       val env = StreamExecutionEnvironment.getExecutionEnvironment
    *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    *       ds.filterWith {
    *         case Point(x, _) => x > 1
    *       }.reduceWith {
    *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    *       }.mapWith {
    *         case Point(x, y) => (x, y)
    *       }.flatMapWith {
    *         case (x, y) => Seq('x' -> x, 'y' -> y)
    *       }.keyingBy {
    *         case (id, value) => id
    *       }
    *     }
    *   }
    * }}}
    *
    */
  implicit def acceptPartialFunctionsOnJoinedStream
      [L: TypeInformation, R: TypeInformation, K, W <: Window](
      ds: JoinedStreams[L, R]#Where[K]#EqualTo#WithWindow[W]) =
    new OnJoinedStream[L, R, K, W](ds)

  /**
    * acceptPartialFunctions extends the original DataStream with methods with unique names
    * that delegate to core higher-order functions (e.g. `map`) so that we can work around
    * the fact that overloaded methods taking functions as parameters can't accept partial
    * functions as well. This enables the possibility to directly apply pattern matching
    * to decompose inputs such as tuples, case classes and collections.
    *
    * e.g.
    * {{{
    *   object Main {
    *     import org.apache.flink.api.scala.extensions._
    *     case class Point(x: Double, y: Double)
    *     def main(args: Array[String]): Unit = {
    *       val env = StreamExecutionEnvironment.getExecutionEnvironment
    *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    *       ds.filterWith {
    *         case Point(x, _) => x > 1
    *       }.reduceWith {
    *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    *       }.mapWith {
    *         case Point(x, y) => (x, y)
    *       }.flatMapWith {
    *         case (x, y) => Seq('x' -> x, 'y' -> y)
    *       }.keyingBy {
    *         case (id, value) => id
    *       }
    *     }
    *   }
    * }}}
    *
    */
  implicit def acceptPartialFunctionsOnConnectedStream[IN1: TypeInformation, IN2: TypeInformation](
      ds: ConnectedStreams[IN1, IN2]) =
    new OnConnectedStream[IN1, IN2](ds)

  /**
    * acceptPartialFunctions extends the original DataStream with methods with unique names
    * that delegate to core higher-order functions (e.g. `map`) so that we can work around
    * the fact that overloaded methods taking functions as parameters can't accept partial
    * functions as well. This enables the possibility to directly apply pattern matching
    * to decompose inputs such as tuples, case classes and collections.
    *
    * e.g.
    * {{{
    *   object Main {
    *     import org.apache.flink.api.scala.extensions._
    *     case class Point(x: Double, y: Double)
    *     def main(args: Array[String]): Unit = {
    *       val env = StreamExecutionEnvironment.getExecutionEnvironment
    *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    *       ds.filterWith {
    *         case Point(x, _) => x > 1
    *       }.reduceWith {
    *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    *       }.mapWith {
    *         case Point(x, y) => (x, y)
    *       }.flatMapWith {
    *         case (x, y) => Seq('x' -> x, 'y' -> y)
    *       }.keyingBy {
    *         case (id, value) => id
    *       }
    *     }
    *   }
    * }}}
    *
    */
  implicit def acceptPartialFunctionsOnWindowedStream[T, K, W <: Window](
      ds: WindowedStream[T, K, W]) =
    new OnWindowedStream[T, K, W](ds)

}
