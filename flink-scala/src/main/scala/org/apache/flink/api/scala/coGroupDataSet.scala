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
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.functions.{RichCoGroupFunction, CoGroupFunction}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.api.java.{DataSet => JavaDataSet}
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
 * @tparam T Type of the left input of the coGroup.
 * @tparam O Type of the right input of the coGroup.
 */
trait CoGroupDataSet[T, O] extends DataSet[(Array[T], Array[O])] {

  /**
   * Creates a new [[DataSet]] where the result for each pair of co-grouped element lists is the
   * result of the given function.
   */
  def apply[R: TypeInformation: ClassTag](
      fun: (TraversableOnce[T], TraversableOnce[O]) => R): DataSet[R]

  /**
   * Creates a new [[DataSet]] where the result for each pair of co-grouped element lists is the
   * result of the given function. The function can output zero or more elements using the
   * [[Collector]] which will form the result.
   */
  def apply[R: TypeInformation: ClassTag](
      fun: (TraversableOnce[T], TraversableOnce[O], Collector[R]) => Unit): DataSet[R]

  /**
   * Creates a new [[DataSet]] by passing each pair of co-grouped element lists to the given
   * function. The function can output zero or more elements using the [[Collector]] which will form
   * the result.
   *
   * A [[RichCoGroupFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[R: TypeInformation: ClassTag](joiner: CoGroupFunction[T, O, R]): DataSet[R]
}

/**
 * Private implementation for [[CoGroupDataSet]] to keep the implementation details, i.e. the
 * parameters of the constructor, hidden.
 */
private[flink] class CoGroupDataSetImpl[T, O](
    coGroupOperator: CoGroupOperator[T, O, (Array[T], Array[O])],
    thisSet: DataSet[T],
    otherSet: DataSet[O],
    thisKeys: Keys[T],
    otherKeys: Keys[O]) extends DataSet(coGroupOperator) with CoGroupDataSet[T, O] {

  def apply[R: TypeInformation: ClassTag](
      fun: (TraversableOnce[T], TraversableOnce[O]) => R): DataSet[R] = {
    Validate.notNull(fun, "CoGroup function must not be null.")
    val coGrouper = new CoGroupFunction[T, O, R] {
      def coGroup(left: java.lang.Iterable[T], right: java.lang.Iterable[O], out: Collector[R]) = {
        out.collect(fun(left.iterator.asScala, right.iterator.asScala))
      }
    }
    val coGroupOperator = new CoGroupOperator[T, O, R](thisSet.set, otherSet.set, thisKeys,
      otherKeys, coGrouper, implicitly[TypeInformation[R]])
    wrap(coGroupOperator)
  }

  def apply[R: TypeInformation: ClassTag](
      fun: (TraversableOnce[T], TraversableOnce[O], Collector[R]) => Unit): DataSet[R] = {
    Validate.notNull(fun, "CoGroup function must not be null.")
    val coGrouper = new CoGroupFunction[T, O, R] {
      def coGroup(left: java.lang.Iterable[T], right: java.lang.Iterable[O], out: Collector[R]) = {
        fun(left.iterator.asScala, right.iterator.asScala, out)
      }
    }
    val coGroupOperator = new CoGroupOperator[T, O, R](thisSet.set, otherSet.set, thisKeys,
      otherKeys, coGrouper, implicitly[TypeInformation[R]])
    wrap(coGroupOperator)
  }

  def apply[R: TypeInformation: ClassTag](joiner: CoGroupFunction[T, O, R]): DataSet[R] = {
    Validate.notNull(joiner, "CoGroup function must not be null.")
    val coGroupOperator = new CoGroupOperator[T, O, R](thisSet.set, otherSet.set, thisKeys,
      otherKeys, joiner, implicitly[TypeInformation[R]])
    wrap(coGroupOperator)
  }
}

/**
 * An unfinished coGroup operation that results from [[DataSet.coGroup()]] The keys for the left and
 * right side must be specified using first `where` and then `isEqualTo`. For example:
 *
 * {{{
 *   val left = ...
 *   val right = ...
 *   val joinResult = left.coGroup(right).where(...).isEqualTo(...)
 * }}}
 * @tparam T The type of the left input of the coGroup.
 * @tparam O The type of the right input of the coGroup.
 */
trait UnfinishedCoGroupOperation[T, O]
  extends UnfinishedKeyPairOperation[T, O, CoGroupDataSet[T, O]]

/**
 * Private implementation for [[UnfinishedCoGroupOperation]] to keep the implementation details,
 * i.e. the parameters of the constructor, hidden.
 */
private[flink] class UnfinishedCoGroupOperationImpl[T: ClassTag, O: ClassTag](
    leftSet: DataSet[T],
    rightSet: DataSet[O])
  extends UnfinishedKeyPairOperation[T, O, CoGroupDataSet[T, O]](leftSet, rightSet)
  with UnfinishedCoGroupOperation[T, O] {

  private[flink] def finish(leftKey: Keys[T], rightKey: Keys[O]) = {
    val coGrouper = new CoGroupFunction[T, O, (Array[T], Array[O])] {
      def coGroup(
                   left: java.lang.Iterable[T],
                   right: java.lang.Iterable[O],
                   out: Collector[(Array[T], Array[O])]) = {
        val leftResult = Array[Any](left.asScala.toSeq: _*).asInstanceOf[Array[T]]
        val rightResult = Array[Any](right.asScala.toSeq: _*).asInstanceOf[Array[O]]

        out.collect((leftResult, rightResult))
      }
    }

    // We have to use this hack, for some reason classOf[Array[T]] does not work.
    // Maybe because ObjectArrayTypeInfo does not accept the Scala Array as an array class.
    val leftArrayType =
      ObjectArrayTypeInfo.getInfoFor(new Array[T](0).getClass, leftSet.set.getType)
    val rightArrayType =
      ObjectArrayTypeInfo.getInfoFor(new Array[O](0).getClass, rightSet.set.getType)

    val returnType = new CaseClassTypeInfo[(Array[T], Array[O])](
      classOf[(Array[T], Array[O])], Seq(leftArrayType, rightArrayType), Array("_1", "_2")) {

      override def createSerializer: TypeSerializer[(Array[T], Array[O])] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity()) {
          fieldSerializers(i) = types(i).createSerializer
        }

        new CaseClassSerializer[(Array[T], Array[O])](
          classOf[(Array[T], Array[O])],
          fieldSerializers) {
          override def createInstance(fields: Array[AnyRef]) = {
            (fields(0).asInstanceOf[Array[T]], fields(1).asInstanceOf[Array[O]])
          }
        }
      }
    }
    val coGroupOperator = new CoGroupOperator[T, O, (Array[T], Array[O])](
      leftSet.set, rightSet.set, leftKey, rightKey, coGrouper, returnType)

    // sanity check solution set key mismatches
    leftSet.set match {
      case solutionSet: DeltaIteration.SolutionSetPlaceHolder[_] =>
        leftKey match {
          case keyFields: Keys.FieldPositionKeys[_] =>
            val positions: Array[Int] = keyFields.computeLogicalKeyPositions
            solutionSet.checkJoinKeyFields(positions)
          case _ =>
            throw new InvalidProgramException("Currently, the solution set may only be joined " +
              "with " +
              "using tuple field positions.")
        }
      case _ =>
    }
    rightSet.set match {
      case solutionSet: DeltaIteration.SolutionSetPlaceHolder[_] =>
        rightKey match {
          case keyFields: Keys.FieldPositionKeys[_] =>
            val positions: Array[Int] = keyFields.computeLogicalKeyPositions
            solutionSet.checkJoinKeyFields(positions)
          case _ =>
            throw new InvalidProgramException("Currently, the solution set may only be joined " +
              "with " +
              "using tuple field positions.")
        }
      case _ =>
    }

    new CoGroupDataSetImpl(coGroupOperator, leftSet, rightSet, leftKey, rightKey)
  }
}
