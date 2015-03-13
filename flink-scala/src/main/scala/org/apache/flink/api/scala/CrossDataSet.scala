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

import org.apache.commons.lang3.Validate
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.{RichCrossFunction, CrossFunction}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala.typeutils.{CaseClassSerializer, CaseClassTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint

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
 * @tparam L Type of the left input of the cross.
 * @tparam R Type of the right input of the cross.
 */
class CrossDataSet[L, R](
    defaultCross: CrossOperator[L, R, (L, R)],
    leftInput: DataSet[L],
    rightInput: DataSet[R])
  extends DataSet(defaultCross) {

  /**
   * Creates a new [[DataSet]] where the result for each pair of elements is the result
   * of the given function.
   */
  def apply[O: TypeInformation: ClassTag](fun: (L, R) => O): DataSet[O] = {
    Validate.notNull(fun, "Cross function must not be null.")
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
   * Creates a new [[DataSet]] by passing each pair of values to the given function.
   * The function can output zero or more elements using the [[Collector]] which will form the
   * result.
   *
   * A [[RichCrossFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[O: TypeInformation: ClassTag](crosser: CrossFunction[L, R, O]): DataSet[O] = {
    Validate.notNull(crosser, "Cross function must not be null.")
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

private[flink] object CrossDataSet {

  /**
   * Creates a default cross operation with Tuple2 as result.
   */
  def createCrossOperator[L, R](
      leftInput: DataSet[L],
      rightInput: DataSet[R],
      crossHint: CrossHint) = {
    
    val crosser = new CrossFunction[L, R, (L, R)] {
      def cross(left: L, right: R) = {
        (left, right)
      }
    }
    val returnType = new CaseClassTypeInfo[(L, R)](
      classOf[(L, R)],
      Array(leftInput.getType, rightInput.getType),
      Seq(leftInput.getType, rightInput.getType),
      Array("_1", "_2")) {

      override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[(L, R)] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity) {
          fieldSerializers(i) = types(i).createSerializer(executionConfig)
        }

        new CaseClassSerializer[(L, R)](classOf[(L, R)], fieldSerializers) {
          override def createInstance(fields: Array[AnyRef]) = {
            (fields(0).asInstanceOf[L], fields(1).asInstanceOf[R])
          }
        }
      }
    }
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
