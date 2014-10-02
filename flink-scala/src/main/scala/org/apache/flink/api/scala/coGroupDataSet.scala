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
import org.apache.flink.api.common.functions.{RichCoGroupFunction, CoGroupFunction}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.api.scala.typeutils.{CaseClassSerializer, CaseClassTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.reflect.ClassTag


/**
 * A specific [[DataSet]] that results from a `coGroup` operation. The result of a default coGroup
 * is a tuple containing two arrays of values from the two sides of the coGroup. The result of the
 * coGroup can be changed by specifying a custom coGroup function using the `apply` method or by
 * providing a [[RichCoGroupFunction]].
 *
 * Example:
 * {{{
 *   val left = ...
 *   val right = ...
 *   val coGroupResult = left.coGroup(right).where(0, 2).isEqualTo(0, 1) {
 *     (left, right) => new MyCoGroupResult(left.min, right.max)
 *   }
 * }}}
 *
 * Or, using key selector functions with tuple data types:
 * {{{
 *   val left = ...
 *   val right = ...
 *   val coGroupResult = left.coGroup(right).where({_._1}).isEqualTo({_._1) {
 *     (left, right) => new MyCoGroupResult(left.max, right.min)
 *   }
 * }}}
 *
 * @tparam L Type of the left input of the coGroup.
 * @tparam R Type of the right input of the coGroup.
 */
class CoGroupDataSet[L, R](
    defaultCoGroup: CoGroupOperator[L, R, (Array[L], Array[R])],
    leftInput: DataSet[L],
    rightInput: DataSet[R],
    leftKeys: Keys[L],
    rightKeys: Keys[R])
  extends DataSet(defaultCoGroup) {

  /**
   * Creates a new [[DataSet]] where the result for each pair of co-grouped element lists is the
   * result of the given function.
   */
  def apply[O: TypeInformation: ClassTag](
      fun: (TraversableOnce[L], TraversableOnce[R]) => O): DataSet[O] = {
    Validate.notNull(fun, "CoGroup function must not be null.")
    val coGrouper = new CoGroupFunction[L, R, O] {
      def coGroup(left: java.lang.Iterable[L], right: java.lang.Iterable[R], out: Collector[O]) = {
        out.collect(fun(left.iterator.asScala, right.iterator.asScala))
      }
    }
    val coGroupOperator = new CoGroupOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      coGrouper,
      implicitly[TypeInformation[O]])

    wrap(coGroupOperator)
  }

  /**
   * Creates a new [[DataSet]] where the result for each pair of co-grouped element lists is the
   * result of the given function. The function can output zero or more elements using the
   * [[Collector]] which will form the result.
   */
  def apply[O: TypeInformation: ClassTag](
      fun: (TraversableOnce[L], TraversableOnce[R], Collector[O]) => Unit): DataSet[O] = {
    Validate.notNull(fun, "CoGroup function must not be null.")
    val coGrouper = new CoGroupFunction[L, R, O] {
      def coGroup(left: java.lang.Iterable[L], right: java.lang.Iterable[R], out: Collector[O]) = {
        fun(left.iterator.asScala, right.iterator.asScala, out)
      }
    }
    val coGroupOperator = new CoGroupOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      coGrouper,
      implicitly[TypeInformation[O]])

    wrap(coGroupOperator)
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of co-grouped element lists to the given
   * function. The function can output zero or more elements using the [[Collector]] which will form
   * the result.
   *
   * A [[RichCoGroupFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[O: TypeInformation: ClassTag](coGrouper: CoGroupFunction[L, R, O]): DataSet[O] = {
    Validate.notNull(coGrouper, "CoGroup function must not be null.")
    val coGroupOperator = new CoGroupOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      coGrouper,
      implicitly[TypeInformation[O]])

    wrap(coGroupOperator)
  }
}

/**
 * An unfinished coGroup operation that results from [[DataSet.coGroup]] The keys for the left and
 * right side must be specified using first `where` and then `isEqualTo`. For example:
 *
 * {{{
 *   val left = ...
 *   val right = ...
 *   val coGroupResult = left.coGroup(right).where(...).isEqualTo(...)
 * }}}
 * @tparam L The type of the left input of the coGroup.
 * @tparam R The type of the right input of the coGroup.
 */
class UnfinishedCoGroupOperation[L: ClassTag, R: ClassTag](
    leftInput: DataSet[L],
    rightInput: DataSet[R])
  extends UnfinishedKeyPairOperation[L, R, CoGroupDataSet[L, R]](leftInput, rightInput) {

  private[flink] def finish(leftKey: Keys[L], rightKey: Keys[R]) = {
    val coGrouper = new CoGroupFunction[L, R, (Array[L], Array[R])] {
      def coGroup(
                   left: java.lang.Iterable[L],
                   right: java.lang.Iterable[R],
                   out: Collector[(Array[L], Array[R])]) = {
        val leftResult = Array[Any](left.asScala.toSeq: _*).asInstanceOf[Array[L]]
        val rightResult = Array[Any](right.asScala.toSeq: _*).asInstanceOf[Array[R]]

        out.collect((leftResult, rightResult))
      }
    }

    // We have to use this hack, for some reason classOf[Array[T]] does not work.
    // Maybe because ObjectArrayTypeInfo does not accept the Scala Array as an array class.
    val leftArrayType =
      ObjectArrayTypeInfo.getInfoFor(new Array[L](0).getClass, leftInput.getType)
    val rightArrayType =
      ObjectArrayTypeInfo.getInfoFor(new Array[R](0).getClass, rightInput.getType)

    val returnType = new CaseClassTypeInfo[(Array[L], Array[R])](
      classOf[(Array[L], Array[R])], Seq(leftArrayType, rightArrayType), Array("_1", "_2")) {

      override def createSerializer: TypeSerializer[(Array[L], Array[R])] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity) {
          fieldSerializers(i) = types(i).createSerializer
        }

        new CaseClassSerializer[(Array[L], Array[R])](
          classOf[(Array[L], Array[R])],
          fieldSerializers) {
          override def createInstance(fields: Array[AnyRef]) = {
            (fields(0).asInstanceOf[Array[L]], fields(1).asInstanceOf[Array[R]])
          }
        }
      }
    }
    val coGroupOperator = new CoGroupOperator[L, R, (Array[L], Array[R])](
      leftInput.javaSet, rightInput.javaSet, leftKey, rightKey, coGrouper, returnType)

    new CoGroupDataSet(coGroupOperator, leftInput, rightInput, leftKey, rightKey)
  }
}
