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
 * @tparam L Type of the left input of the join.
 * @tparam R Type of the right input of the join.
 */
class JoinDataSet[L, R](
    defaultJoin: EquiJoin[L, R, (L, R)],
    leftInput: DataSet[L],
    rightInput: DataSet[R],
    leftKeys: Keys[L],
    rightKeys: Keys[R])
  extends DataSet(defaultJoin) {

  /**
   * Creates a new [[DataSet]] where the result for each pair of joined elements is the result
   * of the given function.
   */
  def apply[O: TypeInformation: ClassTag](fun: (L, R) => O): DataSet[O] = {
    Validate.notNull(fun, "Join function must not be null.")
    val joiner = new FlatJoinFunction[L, R, O] {
      def join(left: L, right: R, out: Collector[O]) = {
        out.collect(fun(left, right))
      }
    }
    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      joiner,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint)

    wrap(joinOperator)
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function can output zero or more elements using the [[Collector]] which will form the
   * result.
   */
  def apply[O: TypeInformation: ClassTag](fun: (L, R, Collector[O]) => Unit): DataSet[O] = {
    Validate.notNull(fun, "Join function must not be null.")
    val joiner = new FlatJoinFunction[L, R, O] {
      def join(left: L, right: R, out: Collector[O]) = {
        fun(left, right, out)
      }
    }
    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      joiner,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint)

    wrap(joinOperator)
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function can output zero or more elements using the [[Collector]] which will form the
   * result.
   *
   * A [[RichFlatJoinFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[O: TypeInformation: ClassTag](joiner: FlatJoinFunction[L, R, O]): DataSet[O] = {
    Validate.notNull(joiner, "Join function must not be null.")

    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      joiner,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint)

    wrap(joinOperator)
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function must output one value. The concatenation of those will be new the DataSet.
   *
   * A [[org.apache.flink.api.common.functions.RichJoinFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[O: TypeInformation: ClassTag](fun: JoinFunction[L, R, O]): DataSet[O] = {
    Validate.notNull(fun, "Join function must not be null.")

    val generatedFunction: FlatJoinFunction[L, R, O] = new WrappingFlatJoinFunction[L, R, O](fun)

    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      generatedFunction, fun,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint)

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
 * @tparam L The type of the left input of the join.
 * @tparam R The type of the right input of the join.
 */
class UnfinishedJoinOperation[L, R](
    leftSet: DataSet[L],
    rightSet: DataSet[R],
    val joinHint: JoinHint)
  extends UnfinishedKeyPairOperation[L, R, JoinDataSet[L, R]](leftSet, rightSet) {

  private[flink] def finish(leftKey: Keys[L], rightKey: Keys[R]) = {
    val joiner = new FlatJoinFunction[L, R, (L, R)] {
      def join(left: L, right: R, out: Collector[(L, R)]) = {
        out.collect((left, right))
      }
    }
    val returnType = new CaseClassTypeInfo[(L, R)](
      classOf[(L, R)], Seq(leftSet.getType, rightSet.getType), Array("_1", "_2")) {

      override def createSerializer: TypeSerializer[(L, R)] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity) {
          fieldSerializers(i) = types(i).createSerializer
        }

        new CaseClassSerializer[(L, R)](classOf[(L, R)], fieldSerializers) {
          override def createInstance(fields: Array[AnyRef]) = {
            (fields(0).asInstanceOf[L], fields(1).asInstanceOf[R])
          }
        }
      }
    }
    val joinOperator = new EquiJoin[L, R, (L, R)](
      leftSet.javaSet, rightSet.javaSet, leftKey, rightKey, joiner, returnType, joinHint)

    new JoinDataSet(joinOperator, leftSet, rightSet, leftKey, rightKey)
  }

}
