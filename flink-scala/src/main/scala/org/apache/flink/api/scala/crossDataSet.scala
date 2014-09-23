/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala

import org.apache.commons.lang3.Validate
import org.apache.flink.api.common.functions.{RichCrossFunction, CrossFunction}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala.typeutils.{CaseClassSerializer, CaseClassTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
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
 * @tparam T Type of the left input of the cross.
 * @tparam O Type of the right input of the cross.
 */
trait CrossDataSet[T, O] extends DataSet[(T, O)] {

  /**
   * Creates a new [[DataSet]] where the result for each pair of elements is the result
   * of the given function.
   */
  def apply[R: TypeInformation: ClassTag](fun: (T, O) => R): DataSet[R]

  /**
   * Creates a new [[DataSet]] by passing each pair of values to the given function.
   * The function can output zero or more elements using the [[Collector]] which will form the
   * result.
   *
   * A [[RichCrossFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[R: TypeInformation: ClassTag](joiner: CrossFunction[T, O, R]): DataSet[R]
}

/**
 * Private implementation for [[CrossDataSet]] to keep the implementation details, i.e. the
 * parameters of the constructor, hidden.
 */
private[flink] class CrossDataSetImpl[T, O](
    crossOperator: CrossOperator[T, O, (T, O)],
    thisSet: JavaDataSet[T],
    otherSet: JavaDataSet[O])
  extends DataSet(crossOperator)
  with CrossDataSet[T, O] {

  def apply[R: TypeInformation: ClassTag](fun: (T, O) => R): DataSet[R] = {
    Validate.notNull(fun, "Cross function must not be null.")
    val crosser = new CrossFunction[T, O, R] {
      def cross(left: T, right: O): R = {
        fun(left, right)
      }
    }
    val crossOperator = new CrossOperator[T, O, R](
      thisSet,
      otherSet,
      crosser,
      implicitly[TypeInformation[R]])
    wrap(crossOperator)
  }

  def apply[R: TypeInformation: ClassTag](crosser: CrossFunction[T, O, R]): DataSet[R] = {
    Validate.notNull(crosser, "Cross function must not be null.")
    val crossOperator = new CrossOperator[T, O, R](
      thisSet,
      otherSet,
      crosser,
      implicitly[TypeInformation[R]])
    wrap(crossOperator)
  }
}

private[flink] object CrossDataSetImpl {
  def createCrossOperator[T, O](leftSet: JavaDataSet[T], rightSet: JavaDataSet[O]) = {
    val crosser = new CrossFunction[T, O, (T, O)] {
      def cross(left: T, right: O) = {
        (left, right)
      }
    }
    val returnType = new CaseClassTypeInfo[(T, O)](
      classOf[(T, O)], Seq(leftSet.getType, rightSet.getType), Array("_1", "_2")) {

      override def createSerializer: TypeSerializer[(T, O)] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity) {
          fieldSerializers(i) = types(i).createSerializer
        }

        new CaseClassSerializer[(T, O)](classOf[(T, O)], fieldSerializers) {
          override def createInstance(fields: Array[AnyRef]) = {
            (fields(0).asInstanceOf[T], fields(1).asInstanceOf[O])
          }
        }
      }
    }
    val crossOperator = new CrossOperator[T, O, (T, O)](leftSet, rightSet, crosser, returnType)

    new CrossDataSetImpl(crossOperator, leftSet, rightSet)
  }
}
