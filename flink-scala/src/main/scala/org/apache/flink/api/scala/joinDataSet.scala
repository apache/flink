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
import org.apache.flink.api.common.functions.{FlatJoinFunction, JoinFunction, Partitioner, RichFlatJoinFunction}
import org.apache.flink.api.common.operators.Keys
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin.WrappingFlatJoinFunction
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin
import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.operators.join.JoinType
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
 *   val joinResult = left.join(right).where(0, 2).equalTo(0, 1) {
 *     (left, right) => new MyJoinResult(left, right)
 *   }
 * }}}
 *
 * Or, using key selector functions with tuple data types:
 * {{{
 *   val left = ...
 *   val right = ...
 *   val joinResult = left.join(right).where({_._1}).equalTo({_._1) {
 *     (left, right) => new MyJoinResult(left, right)
 *   }
 * }}}
 *
 * @tparam L Type of the left input of the join.
 * @tparam R Type of the right input of the join.
 */
@Public
class JoinDataSet[L, R](
    defaultJoin: EquiJoin[L, R, (L, R)],
    leftInput: DataSet[L],
    rightInput: DataSet[R],
    leftKeys: Keys[L],
    rightKeys: Keys[R])
  extends DataSet(defaultJoin) with JoinFunctionAssigner[L, R] {

  private var customPartitioner : Partitioner[_] = _
  
  /**
   * Creates a new [[DataSet]] where the result for each pair of joined elements is the result
   * of the given function.
   */
  def apply[O: TypeInformation: ClassTag](fun: (L, R) => O): DataSet[O] = {
    require(fun != null, "Join function must not be null.")
    val joiner = new FlatJoinFunction[L, R, O] {
      val cleanFun = clean(fun)
      def join(left: L, right: R, out: Collector[O]) = {
        out.collect(cleanFun(left, right))
      }
    }
    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      joiner,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint,
      getCallLocationName(),
      defaultJoin.getJoinType)
    
    if (customPartitioner != null) {
      wrap(joinOperator.withPartitioner(customPartitioner))
    } else {
      wrap(joinOperator)
    }
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function can output zero or more elements using the [[Collector]] which will form the
   * result.
   */
  def apply[O: TypeInformation: ClassTag](fun: (L, R, Collector[O]) => Unit): DataSet[O] = {
    require(fun != null, "Join function must not be null.")
    val joiner = new FlatJoinFunction[L, R, O] {
      val cleanFun = clean(fun)
      def join(left: L, right: R, out: Collector[O]) = {
        cleanFun(left, right, out)
      }
    }
    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      joiner,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint,
      getCallLocationName(),
      defaultJoin.getJoinType)

    if (customPartitioner != null) {
      wrap(joinOperator.withPartitioner(customPartitioner))
    } else {
      wrap(joinOperator)
    }
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
    require(joiner != null, "Join function must not be null.")

    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      joiner,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint,
      getCallLocationName(),
      defaultJoin.getJoinType)

    if (customPartitioner != null) {
      wrap(joinOperator.withPartitioner(customPartitioner))
    } else {
      wrap(joinOperator)
    }
  }

  /**
   * Creates a new [[DataSet]] by passing each pair of joined values to the given function.
   * The function must output one value. The concatenation of those will be new the DataSet.
   *
   * A [[org.apache.flink.api.common.functions.RichJoinFunction]] can be used to access the
   * broadcast variables and the [[org.apache.flink.api.common.functions.RuntimeContext]].
   */
  def apply[O: TypeInformation: ClassTag](fun: JoinFunction[L, R, O]): DataSet[O] = {
    require(fun != null, "Join function must not be null.")

    val generatedFunction: FlatJoinFunction[L, R, O] = new WrappingFlatJoinFunction[L, R, O](fun)

    val joinOperator = new EquiJoin[L, R, O](
      leftInput.javaSet,
      rightInput.javaSet,
      leftKeys,
      rightKeys,
      generatedFunction, fun,
      implicitly[TypeInformation[O]],
      defaultJoin.getJoinHint,
      getCallLocationName(),
      defaultJoin.getJoinType)

    if (customPartitioner != null) {
      wrap(joinOperator.withPartitioner(customPartitioner))
    } else {
      wrap(joinOperator)
    }
  }
  
  // ----------------------------------------------------------------------------------------------
  //  Properties
  // ----------------------------------------------------------------------------------------------
  
  def withPartitioner[K : TypeInformation](partitioner : Partitioner[K]) : JoinDataSet[L, R] = {
    if (partitioner != null) {
      val typeInfo : TypeInformation[K] = implicitly[TypeInformation[K]]
      
      leftKeys.validateCustomPartitioner(partitioner, typeInfo)
      rightKeys.validateCustomPartitioner(partitioner, typeInfo)
    }
    this.customPartitioner = partitioner
    defaultJoin.withPartitioner(partitioner)
    
    this
  }

  /**
   * Gets the custom partitioner used by this join, or null, if none is set.
   */
  @Internal
  def getPartitioner[K]() : Partitioner[K] = {
    customPartitioner.asInstanceOf[Partitioner[K]]
  }
}

@Internal
private[flink] abstract class UnfinishedJoinOperationBase[L, R, O <: JoinFunctionAssigner[L, R]](
    leftSet: DataSet[L],
    rightSet: DataSet[R],
    val joinHint: JoinHint,
    val joinType: JoinType)
  extends UnfinishedKeyPairOperation[L, R, O](leftSet, rightSet) {

  def createJoinFunctionAssigner(leftKey: Keys[L], rightKey: Keys[R]): O

  private[flink] def createDefaultJoin(leftKey: Keys[L], rightKey: Keys[R]) = {
    val joiner = new FlatJoinFunction[L, R, (L, R)] {
      def join(left: L, right: R, out: Collector[(L, R)]) = {
        out.collect((left, right))
      }
    }
    val returnType = createTuple2TypeInformation[L, R](leftInput.getType(), rightInput.getType())
    val joinOperator = new EquiJoin[L, R, (L, R)](
      leftSet.javaSet,
      rightSet.javaSet,
      leftKey,
      rightKey,
      joiner,
      returnType,
      joinHint,
      getCallLocationName(),
      joinType)

    new JoinDataSet(joinOperator, leftSet, rightSet, leftKey, rightKey)
  }

  private[flink] def finish(leftKey: Keys[L], rightKey: Keys[R]) = {
    createJoinFunctionAssigner(leftKey, rightKey)
  }
}

/**
 * An unfinished inner join operation that results from calling [[DataSet.join()]].
 * The keys for the left and right side must be specified using first `where` and then `equalTo`.
 *
 * For example:
 *
 * {{{
 *   val left = ...
 *   val right = ...
 *   val joinResult = left.join(right).where(...).equalTo(...)
 * }}}
 *
 * @tparam L The type of the left input of the join.
 * @tparam R The type of the right input of the join.
 */
@Public
class UnfinishedJoinOperation[L, R](
    leftSet: DataSet[L],
    rightSet: DataSet[R],
    joinHint: JoinHint)
  extends UnfinishedJoinOperationBase[L, R, JoinDataSet[L, R]](
    leftSet, rightSet, joinHint, JoinType.INNER) {

  @Internal
  override def createJoinFunctionAssigner(leftKey: Keys[L], rightKey: Keys[R]) = {
    createDefaultJoin(leftKey, rightKey)
  }
}

/**
 * An unfinished outer join operation that results from calling, e.g. [[DataSet.fullOuterJoin()]].
 * The keys for the left and right side must be specified using first `where` and then `equalTo`.
 *
 * Note that a join function must always be specified explicitly when construction an outer join
 * operator.
 *
 * For example:
 *
 * {{{
 *   val left = ...
 *   val right = ...
 *   val joinResult = left.fullOuterJoin(right).where(...).equalTo(...) {
 *     (first, second) => ...
 *   }
 * }}}
 *
 * @tparam L The type of the left input of the join.
 * @tparam R The type of the right input of the join.
 */
@Public
class UnfinishedOuterJoinOperation[L, R](
    leftSet: DataSet[L],
    rightSet: DataSet[R],
    joinHint: JoinHint,
    joinType: JoinType)
  extends UnfinishedJoinOperationBase[L, R, JoinFunctionAssigner[L, R]](
    leftSet, rightSet, joinHint, joinType) {

  @Internal
  override def createJoinFunctionAssigner(leftKey: Keys[L], rightKey: Keys[R]):
      JoinFunctionAssigner[L, R] = {
    new DefaultJoinFunctionAssigner(createDefaultJoin(leftKey, rightKey))
  }

  private class DefaultJoinFunctionAssigner(val defaultJoin: JoinDataSet[L, R])
    extends JoinFunctionAssigner[L, R] {

    override def withPartitioner[K: TypeInformation](part: Partitioner[K]) =
      defaultJoin.withPartitioner(part)

    override def apply[O: TypeInformation : ClassTag](fun: (L, R) => O) =
      defaultJoin.apply(fun)

    override def apply[O: TypeInformation : ClassTag](fun: (L, R, Collector[O]) => Unit) =
      defaultJoin.apply(fun)

    override def apply[O: TypeInformation : ClassTag](fun: FlatJoinFunction[L, R, O]) =
      defaultJoin.apply(fun)

    override def apply[O: TypeInformation : ClassTag](fun: JoinFunction[L, R, O]) =
      defaultJoin.apply(fun)
  }

}

@Public
trait JoinFunctionAssigner[L, R] {

  def withPartitioner[K : TypeInformation](part : Partitioner[K]) : JoinFunctionAssigner[L, R]
  def apply[O: TypeInformation: ClassTag](fun: (L, R) => O): DataSet[O]
  def apply[O: TypeInformation: ClassTag](fun: (L, R, Collector[O]) => Unit): DataSet[O]
  def apply[O: TypeInformation: ClassTag](fun: FlatJoinFunction[L, R, O]): DataSet[O]
  def apply[O: TypeInformation: ClassTag](fun: JoinFunction[L, R, O]): DataSet[O]

}
