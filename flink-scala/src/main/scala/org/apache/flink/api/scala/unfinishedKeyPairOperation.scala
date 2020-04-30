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

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.operators.Keys

import org.apache.flink.api.java.functions.KeySelector
import Keys.ExpressionKeys
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * This is for dealing with operations that require keys and use a fluent interface (join, and
 * coGroup for now). For each operation we need a subclass that implements `finish` to
 * create the actual operation using the provided keys.
 *
 * This way, we have a central point where all the key-providing happens and don't need to change
 * the specific operations if the supported key types change.
 *
 * We use the type parameter `O` to specify the type of the resulting operation. For join
 * this would be `JoinDataSet[L, R]` and for coGroup it would be `CoGroupDataSet[L, R]`. This
 * way the user gets the correct type for the finished operation.
 *
 * @tparam L Type of the left input [[DataSet]].
 * @tparam R Type of the right input [[DataSet]].
 * @tparam O The type of the resulting Operation.
 */
@Internal
private[flink] abstract class UnfinishedKeyPairOperation[L, R, O](
    private[flink] val leftInput: DataSet[L],
    private[flink] val rightInput: DataSet[R]) {

  private[flink] def finish(leftKey: Keys[L], rightKey: Keys[R]): O

  /**
   * Specify the key fields for the left side of the key based operation. This returns
   * a [[HalfUnfinishedKeyPairOperation]] on which `equalTo` must be called to specify the
   * key for the right side. The result after specifying the right side key is the finished
   * operation.
   *
   * This only works on Tuple [[DataSet]].
   */
  def where(leftKeys: Int*) = {
    val leftKey = new ExpressionKeys[L](leftKeys.toArray, leftInput.getType)
    new HalfUnfinishedKeyPairOperation[L, R, O](this, leftKey)
  }

  /**
   * Specify the key fields for the left side of the key based operation. This returns
   * a [[HalfUnfinishedKeyPairOperation]] on which `equalTo` must be called to specify the
   * key for the right side. The result after specifying the right side key is the finished
   * operation.
   */
  def where(firstLeftField: String, otherLeftFields: String*) = {
    val leftKey = new ExpressionKeys[L](
      firstLeftField +: otherLeftFields.toArray,
      leftInput.getType)
    new HalfUnfinishedKeyPairOperation[L, R, O](this, leftKey)
  }

  /**
   * Specify the key selector function for the left side of the key based operation. This returns
   * a [[HalfUnfinishedKeyPairOperation]] on which `equalTo` must be called to specify the
   * key for the right side. The result after specifying the right side key is the finished
   * operation.
   */
  def where[K: TypeInformation](fun: (L) => K) = {
    val keyType = implicitly[TypeInformation[K]]
    val keyExtractor = new KeySelector[L, K] {
      val cleanFun = leftInput.clean(fun)
      def getKey(in: L) = cleanFun(in)
    }
    val leftKey = new Keys.SelectorFunctionKeys[L, K](keyExtractor, leftInput.getType, keyType)
    new HalfUnfinishedKeyPairOperation[L, R, O](this, leftKey)
  }

  /**
   * Specify the key selector function for the left side of the key based operation. This returns
   * a [[HalfUnfinishedKeyPairOperation]] on which `equalTo` must be called to specify the
   * key for the right side. The result after specifying the right side key is the finished
   * operation.
   */
  def where[K: TypeInformation](fun: KeySelector[L, K]) = {
    val keyType = implicitly[TypeInformation[K]]
    val leftKey =
      new Keys.SelectorFunctionKeys[L, K](leftInput.clean(fun), leftInput.getType, keyType)
    new HalfUnfinishedKeyPairOperation[L, R, O](this, leftKey)
  }
}

@Internal
private[flink] class HalfUnfinishedKeyPairOperation[L, R, O](
    unfinished: UnfinishedKeyPairOperation[L, R, O], leftKey: Keys[L]) {

  /**
   * Specify the key fields for the right side of the key based operation. This returns
   * the finished operation.
   *
   * This only works on a Tuple [[DataSet]].
   */
  def equalTo(rightKeys: Int*): O = {
    val rightKey = new ExpressionKeys[R](rightKeys.toArray, unfinished.rightInput.getType)
    if (!leftKey.areCompatible(rightKey)) {
      throw new InvalidProgramException("The types of the key fields do not match. Left: " +
        leftKey + " Right: " + rightKey)
    }
    unfinished.finish(leftKey, rightKey)
  }

  /**
   * Specify the key fields for the right side of the key based operation. This returns
   * the finished operation.
   */
  def equalTo(firstRightField: String, otherRightFields: String*): O = {
    val rightKey = new ExpressionKeys[R](
      firstRightField +: otherRightFields.toArray,
      unfinished.rightInput.getType)
    if (!leftKey.areCompatible(rightKey)) {
      throw new InvalidProgramException("The types of the key fields do not match. Left: " +
        leftKey + " Right: " + rightKey)
    }
    unfinished.finish(leftKey, rightKey)
  }

  /**
   * Specify the key selector function for the right side of the key based operation. This returns
   * the finished operation.
   */
  def equalTo[K: TypeInformation](fun: (R) => K): O = {
    val keyType = implicitly[TypeInformation[K]]
    val keyExtractor = new KeySelector[R, K] {
      val cleanFun = unfinished.leftInput.clean(fun)
      def getKey(in: R) = cleanFun(in)
    }
    val rightKey = new Keys.SelectorFunctionKeys[R, K](
      keyExtractor,
      unfinished.rightInput.getType,
      keyType)

    if (!leftKey.areCompatible(rightKey)) {
      throw new InvalidProgramException("The types of the key fields do not match. Left: " +
        leftKey + " Right: " + rightKey)
    }
    unfinished.finish(leftKey, rightKey)
  }

  /**
   * Specify the key selector function for the right side of the key based operation. This returns
   * the finished operation.
   */
  def equalTo[K: TypeInformation](fun: KeySelector[R, K]): O = {

    val keyType = implicitly[TypeInformation[K]]
    val rightKey = new Keys.SelectorFunctionKeys[R, K](
      unfinished.leftInput.clean(fun),
      unfinished.rightInput.getType,
      keyType)
    
    if (!leftKey.areCompatible(rightKey)) {
      throw new InvalidProgramException("The types of the key fields do not match. Left: " +
        leftKey + " Right: " + rightKey)
    }
    unfinished.finish(leftKey, rightKey)
  }
}
