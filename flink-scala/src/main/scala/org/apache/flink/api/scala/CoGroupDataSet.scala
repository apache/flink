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
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.functions.{CoGroupFunction, Partitioner, RichCoGroupFunction}
import org.apache.flink.api.common.operators.{Keys, Order}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.operators._
import org.apache.flink.util.Collector

import Keys.ExpressionKeys
import org.apache.commons.lang3.tuple.{ImmutablePair, Pair}

import scala.collection.JavaConverters._
import scala.collection.mutable
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
 * @tparam L
 *   Type of the left input of the coGroup.
 * @tparam R
 *   Type of the right input of the coGroup.
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@Public
class CoGroupDataSet[L, R](
    defaultCoGroup: CoGroupOperator[L, R, (Array[L], Array[R])],
    leftInput: DataSet[L],
    rightInput: DataSet[R],
    leftKeys: Keys[L],
    rightKeys: Keys[R])
  extends DataSet(defaultCoGroup) {

  private val groupSortKeyPositionsFirst = mutable.MutableList[Either[Int, String]]()
  private val groupSortKeyPositionsSecond = mutable.MutableList[Either[Int, String]]()
  private val groupSortOrdersFirst = mutable.MutableList[Order]()
  private val groupSortOrdersSecond = mutable.MutableList[Order]()

  private var customPartitioner: Partitioner[_] = _

  /**
   * Creates a new [[DataSet]] where the result for each pair of co-grouped element lists is the
   * result of the given function.
   */
  def apply[O: TypeInformation: ClassTag](fun: (Iterator[L], Iterator[R]) => O): DataSet[O] = {
    require(fun != null, "CoGroup function must not be null.")
    val coGrouper = new CoGroupFunction[L, R, O] {
      val cleanFun = clean(fun)
      def coGroup(left: java.lang.Iterable[L], right: java.lang.Iterable[R], out: Collector[O]) = {
        out.collect(cleanFun(left.iterator().asScala, right.iterator().asScala))
      }
    }
    val coGroupOperator = new CoGroupOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      coGrouper,
      implicitly[TypeInformation[O]],
      buildGroupSortList(leftInput.getType, groupSortKeyPositionsFirst, groupSortOrdersFirst),
      buildGroupSortList(rightInput.getType, groupSortKeyPositionsSecond, groupSortOrdersSecond),
      customPartitioner,
      getCallLocationName())

    wrap(coGroupOperator)
  }

  /**
   * Creates a new [[DataSet]] where the result for each pair of co-grouped element lists is the
   * result of the given function. The function can output zero or more elements using the
   * [[Collector]] which will form the result.
   */
  def apply[O: TypeInformation: ClassTag](
      fun: (Iterator[L], Iterator[R], Collector[O]) => Unit): DataSet[O] = {
    require(fun != null, "CoGroup function must not be null.")
    val coGrouper = new CoGroupFunction[L, R, O] {
      val cleanFun = clean(fun)
      def coGroup(left: java.lang.Iterable[L], right: java.lang.Iterable[R], out: Collector[O]) = {
        cleanFun(left.iterator.asScala, right.iterator.asScala, out)
      }
    }
    val coGroupOperator = new CoGroupOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      coGrouper,
      implicitly[TypeInformation[O]],
      buildGroupSortList(leftInput.getType, groupSortKeyPositionsFirst, groupSortOrdersFirst),
      buildGroupSortList(rightInput.getType, groupSortKeyPositionsSecond, groupSortOrdersSecond),
      customPartitioner,
      getCallLocationName())

    wrap(coGroupOperator)
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of co-grouped element lists to the given
   * function. The function can output zero or more elements using the [[Collector]] which will form
   * the result.
   *
   * A [[RichCoGroupFunction]] can be used to access the broadcast variables and the
   * [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[O: TypeInformation: ClassTag](coGrouper: CoGroupFunction[L, R, O]): DataSet[O] = {
    require(coGrouper != null, "CoGroup function must not be null.")
    val coGroupOperator = new CoGroupOperator[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      coGrouper,
      implicitly[TypeInformation[O]],
      buildGroupSortList(leftInput.getType, groupSortKeyPositionsFirst, groupSortOrdersFirst),
      buildGroupSortList(rightInput.getType, groupSortKeyPositionsSecond, groupSortOrdersSecond),
      customPartitioner,
      getCallLocationName())

    wrap(coGroupOperator)
  }

  // ----------------------------------------------------------------------------------------------
  //  Properties
  // ----------------------------------------------------------------------------------------------

  def withPartitioner[K: TypeInformation](partitioner: Partitioner[K]): CoGroupDataSet[L, R] = {
    if (partitioner != null) {
      val typeInfo: TypeInformation[K] = implicitly[TypeInformation[K]]

      leftKeys.validateCustomPartitioner(partitioner, typeInfo)
      rightKeys.validateCustomPartitioner(partitioner, typeInfo)
    }
    this.customPartitioner = partitioner
    defaultCoGroup.withPartitioner(partitioner)

    this
  }

  /** Gets the custom partitioner used by this join, or null, if none is set. */
  @Internal
  def getPartitioner[K](): Partitioner[K] = {
    customPartitioner.asInstanceOf[Partitioner[K]]
  }

  /**
   * Adds a secondary sort key to the first input of this [[CoGroupDataSet]].
   *
   * This only works on Tuple DataSets.
   */
  def sortFirstGroup(field: Int, order: Order): CoGroupDataSet[L, R] = {
    if (!defaultCoGroup.getInput1Type().isTupleType) {
      throw new InvalidProgramException(
        "Specifying order keys via field positions is only valid " +
          "for tuple data types.")
    }
    if (field >= defaultCoGroup.getInput1Type().getArity) {
      throw new IllegalArgumentException("Order key out of tuple bounds.")
    }
    groupSortKeyPositionsFirst += Left(field)
    groupSortOrdersFirst += order
    this
  }

  /** Adds a secondary sort key to the first input of this [[CoGroupDataSet]]. */
  def sortFirstGroup(field: String, order: Order): CoGroupDataSet[L, R] = {
    groupSortKeyPositionsFirst += Right(field)
    groupSortOrdersFirst += order
    this
  }

  /**
   * Adds a secondary sort key to the second input of this [[CoGroupDataSet]].
   *
   * This only works on Tuple DataSets.
   */
  def sortSecondGroup(field: Int, order: Order): CoGroupDataSet[L, R] = {
    if (!defaultCoGroup.getInput2Type().isTupleType) {
      throw new InvalidProgramException(
        "Specifying order keys via field positions is only valid " +
          "for tuple data types.")
    }
    if (field >= defaultCoGroup.getInput2Type().getArity) {
      throw new IllegalArgumentException("Order key out of tuple bounds.")
    }
    groupSortKeyPositionsSecond += Left(field)
    groupSortOrdersSecond += order
    this
  }

  /** Adds a secondary sort key to the second input of this [[CoGroupDataSet]]. */
  def sortSecondGroup(field: String, order: Order): CoGroupDataSet[L, R] = {
    groupSortKeyPositionsSecond += Right(field)
    groupSortOrdersSecond += order
    this
  }

  private def buildGroupSortList[T](
      typeInfo: TypeInformation[T],
      keys: mutable.MutableList[Either[Int, String]],
      orders: mutable.MutableList[Order]): java.util.List[Pair[java.lang.Integer, Order]] = {
    if (keys.isEmpty) {
      null
    } else {
      val result = new java.util.ArrayList[Pair[java.lang.Integer, Order]]

      keys.zip(orders).foreach {
        case (Left(position), order) =>
          result.add(new ImmutablePair[java.lang.Integer, Order](position, order))

        case (Right(expression), order) =>
          val ek = new ExpressionKeys[T](Array[String](expression), typeInfo)
          val groupOrderKeys: Array[Int] = ek.computeLogicalKeyPositions()

          for (k <- groupOrderKeys) {
            result.add(new ImmutablePair[java.lang.Integer, Order](k, order))
          }
      }

      result
    }
  }
}
