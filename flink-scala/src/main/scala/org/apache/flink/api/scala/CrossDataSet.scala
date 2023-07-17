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

import org.apache.flink.annotation.{Internal, Public}
import org.apache.flink.api.common.functions.{CrossFunction, RichCrossFunction}
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
 * A specific [[DataSet]] that results from a `cross` operation. The result of a default cross is a
 * tuple containing the two values from the two sides of the cartesian product. The result of the
 * cross can be changed by specifying a custom cross function using the `apply` method or by
 * providing a [[RichCrossFunction]].
 *
 * Example:
 * {{{
 *   val left = ...
 *   val right = ...
 *   val crossResult = left.cross(right) {
 *     (left, right) => new MyCrossResult(left, right)
 *   }
 * }}}
 *
 * @tparam L
 *   Type of the left input of the cross.
 * @tparam R
 *   Type of the right input of the cross.
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@Public
class CrossDataSet[L, R](
    defaultCross: CrossOperator[L, R, (L, R)],
    leftInput: DataSet[L],
    rightInput: DataSet[R])
  extends DataSet(defaultCross) {

  /**
   * Creates a new [[DataSet]] where the result for each pair of elements is the result of the given
   * function.
   */
  def apply[O: TypeInformation: ClassTag](fun: (L, R) => O): DataSet[O] = {
    require(fun != null, "Cross function must not be null.")
    val crosser = new CrossFunction[L, R, O] {
      val cleanFun = clean(fun)
      def cross(left: L, right: R): O = {
        cleanFun(left, right)
      }
    }
    val crossOperator = new CrossOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      crosser,
      implicitly[TypeInformation[O]],
      defaultCross.getCrossHint(),
      getCallLocationName())
    wrap(crossOperator)
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of values to the given function. The function
   * can output zero or more elements using the [[Collector]] which will form the result.
   *
   * A [[RichCrossFunction]] can be used to access the broadcast variables and the
   * [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[O: TypeInformation: ClassTag](crosser: CrossFunction[L, R, O]): DataSet[O] = {
    require(crosser != null, "Cross function must not be null.")
    val crossOperator = new CrossOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      crosser,
      implicitly[TypeInformation[O]],
      defaultCross.getCrossHint(),
      getCallLocationName())
    wrap(crossOperator)
  }
}

@Internal
private[flink] object CrossDataSet {

  /** Creates a default cross operation with Tuple2 as result. */
  def createCrossOperator[L, R](
      leftInput: DataSet[L],
      rightInput: DataSet[R],
      crossHint: CrossHint) = {

    val crosser = new CrossFunction[L, R, (L, R)] {
      def cross(left: L, right: R) = {
        (left, right)
      }
    }
    val returnType = createTuple2TypeInformation[L, R](leftInput.getType(), rightInput.getType())
    val crossOperator = new CrossOperator[L, R, (L, R)](
      leftInput.javaSet,
      rightInput.javaSet,
      crosser,
      returnType,
      crossHint,
      getCallLocationName())

    new CrossDataSet(crossOperator, leftInput, rightInput)
  }
}
