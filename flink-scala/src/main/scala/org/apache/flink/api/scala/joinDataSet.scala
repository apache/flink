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
import org.apache.flink.api.common.functions.{JoinFunction, RichFlatJoinFunction, FlatJoinFunction}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin.WrappingFlatJoinFunction
import org.apache.flink.api.java.operators.JoinOperator.{EquiJoin, JoinHint}
import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala.typeutils.{CaseClassSerializer, CaseClassTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
 * A specific [[DataSet]] that results from a `join` operation. The result of a default join is a
 * tuple containing the two values from the two sides of the join. The result of the join can be
 * changed by specifying a custom join function using the `apply` method or by providing a
 * [[RichFlatJoinFunction]].
 *
 * Example:
 * {{{
 *   val left = ...
 *   val right = ...
 *   val joinResult = left.join(right).where(0, 2).isEqualTo(0, 1) {
 *     (left, right) => new MyJoinResult(left, right)
 *   }
 * }}}
 *
 * Or, using key selector functions with tuple data types:
 * {{{
 *   val left = ...
 *   val right = ...
 *   val joinResult = left.join(right).where({_._1}).isEqualTo({_._1) {
 *     (left, right) => new MyJoinResult(left, right)
 *   }
 * }}}
 *
 * @tparam T Type of the left input of the join.
 * @tparam O Type of the right input of the join.
 */
trait JoinDataSet[T, O] extends DataSet[(T, O)] {

  /**
   * Creates a new [[DataSet]] where the result for each pair of joined elements is the result
   * of the given function.
   */
  def apply[R: TypeInformation: ClassTag](fun: (T, O) => R): DataSet[R]

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function can output zero or more elements using the [[Collector]] which will form the
   * result.
   */
  def apply[R: TypeInformation: ClassTag](fun: (T, O, Collector[R]) => Unit): DataSet[R]

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function can output zero or more elements using the [[Collector]] which will form the
   * result.
   *
   * A [[RichFlatJoinFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[R: TypeInformation: ClassTag](joiner: FlatJoinFunction[T, O, R]): DataSet[R]

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function must output one value. The concatenation of those will be new the DataSet.
   *
   * A [[org.apache.flink.api.common.functions.RichJoinFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[R: TypeInformation: ClassTag](joiner: JoinFunction[T, O, R]): DataSet[R]
}

/**
 * Private implementation for [[JoinDataSet]] to keep the implementation details, i.e. the
 * parameters of the constructor, hidden.
 */
private[flink] class JoinDataSetImpl[T, O](
    joinOperator: EquiJoin[T, O, (T, O)],
    thisSet: JavaDataSet[T],
    otherSet: JavaDataSet[O],
    thisKeys: Keys[T],
    otherKeys: Keys[O])
  extends DataSet(joinOperator)
  with JoinDataSet[T, O] {

  def apply[R: TypeInformation: ClassTag](fun: (T, O) => R): DataSet[R] = {
    Validate.notNull(fun, "Join function must not be null.")
    val joiner = new FlatJoinFunction[T, O, R] {
      def join(left: T, right: O, out: Collector[R]) = {
        out.collect(fun(left, right))
      }
    }
    val joinOperator = new EquiJoin[T, O, R](thisSet, otherSet, thisKeys,
      otherKeys, joiner, implicitly[TypeInformation[R]], JoinHint.OPTIMIZER_CHOOSES)
    wrap(joinOperator)
  }

  def apply[R: TypeInformation: ClassTag](fun: (T, O, Collector[R]) => Unit): DataSet[R] = {
    Validate.notNull(fun, "Join function must not be null.")
    val joiner = new FlatJoinFunction[T, O, R] {
      def join(left: T, right: O, out: Collector[R]) = {
        fun(left, right, out)
      }
    }
    val joinOperator = new EquiJoin[T, O, R](thisSet, otherSet, thisKeys,
      otherKeys, joiner, implicitly[TypeInformation[R]], JoinHint.OPTIMIZER_CHOOSES)
    wrap(joinOperator)
  }

  def apply[R: TypeInformation: ClassTag](joiner: FlatJoinFunction[T, O, R]): DataSet[R] = {
    Validate.notNull(joiner, "Join function must not be null.")
    val joinOperator = new EquiJoin[T, O, R](thisSet, otherSet, thisKeys,
      otherKeys, joiner, implicitly[TypeInformation[R]], JoinHint.OPTIMIZER_CHOOSES)
    wrap(joinOperator)
  }

  def apply[R: TypeInformation: ClassTag](fun: JoinFunction[T, O, R]): DataSet[R] = {
    Validate.notNull(fun, "Join function must not be null.")

    val generatedFunction: FlatJoinFunction[T, O, R] = new WrappingFlatJoinFunction[T, O, R](fun)

    val joinOperator = new EquiJoin[T, O, R](thisSet, otherSet, thisKeys,
      otherKeys, generatedFunction, fun, implicitly[TypeInformation[R]], JoinHint.OPTIMIZER_CHOOSES)
    wrap(joinOperator)
  }
}

/**
 * An unfinished join operation that results from [[DataSet.join()]] The keys for the left and right
 * side must be specified using first `where` and then `isEqualTo`. For example:
 *
 * {{{
 *   val left = ...
 *   val right = ...
 *   val joinResult = left.join(right).where(...).isEqualTo(...)
 * }}}
 * @tparam T The type of the left input of the join.
 * @tparam O The type of the right input of the join.
 */
trait UnfinishedJoinOperation[T, O] extends UnfinishedKeyPairOperation[T, O, JoinDataSet[T, O]]

/**
 * Private implementation for [[UnfinishedJoinOperation]] to keep the implementation details,
 * i.e. the parameters of the constructor, hidden.
 */
private[flink] class UnfinishedJoinOperationImpl[T, O](
    leftSet: DataSet[T],
    rightSet: DataSet[O],
    joinHint: JoinHint)
  extends UnfinishedKeyPairOperation[T, O, JoinDataSet[T, O]](leftSet, rightSet)
  with UnfinishedJoinOperation[T, O] {

  private[flink] def finish(leftKey: Keys[T], rightKey: Keys[O]) = {
    val joiner = new FlatJoinFunction[T, O, (T, O)] {
      def join(left: T, right: O, out: Collector[(T, O)]) = {
        out.collect((left, right))
      }
    }
    val returnType = new CaseClassTypeInfo[(T, O)](
      classOf[(T, O)], Seq(leftSet.set.getType, rightSet.set.getType), Array("_1", "_2")) {

      override def createSerializer: TypeSerializer[(T, O)] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity()) {
          fieldSerializers(i) = types(i).createSerializer
        }

        new CaseClassSerializer[(T, O)](classOf[(T, O)], fieldSerializers) {
          override def createInstance(fields: Array[AnyRef]) = {
            (fields(0).asInstanceOf[T], fields(1).asInstanceOf[O])
          }
        }
      }
    }
    val joinOperator = new EquiJoin[T, O, (T, O)](
      leftSet.set, rightSet.set, leftKey, rightKey, joiner, returnType, joinHint)

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

    new JoinDataSetImpl(joinOperator, leftSet.set, rightSet.set, leftKey, rightKey)
  }
}
