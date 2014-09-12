/**
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

import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.java.{DataSet => JavaDataSet}

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.operators.Keys
import org.apache.flink.api.java.operators.Keys.FieldPositionKeys
import org.apache.flink.types.TypeInformation

/**
 * This is for dealing with operations that require keys and use a fluent interface (join, and
 * coGroup for now). For each operation we need a subclass that implements `finish` to
 * create the actual operation using the provided keys.
 *
 * This way, we have a central point where all the key-providing happens and don't need to change
 * the specific operations if the supported key types change.
 *
 * We use the type parameter `R` to specify the type of the resulting operation. For join
 * this would be `JoinDataSet[T, O]` and for coGroup it would be `CoGroupDataSet[T, O]`. This
 * way the user gets the correct type for the finished operation.
 *
 * @tparam T Type of the left input [[DataSet]].
 * @tparam O Type of the right input [[DataSet]].
 * @tparam R The type of the resulting Operation.
 */
private[flink] abstract class UnfinishedKeyPairOperation[T, O, R](
    private[flink] val leftSet: JavaDataSet[T],
    private[flink] val rightSet: JavaDataSet[O]) {

  private[flink] def finish(leftKey: Keys[T], rightKey: Keys[O]): R

  /**
   * Specify the key fields for the left side of the key based operation. This returns
   * a [[HalfUnfinishedKeyPairOperation]] on which `isEqualTo` must be called to specify the
   * key for the right side. The result after specifying the right side key is the finished
   * operation.
   *
   * This only works on a Tuple [[DataSet]].
   */
  def where(leftKeys: Int*) = {
    val leftKey = new FieldPositionKeys[T](leftKeys.toArray, leftSet.getType)
    new HalfUnfinishedKeyPairOperation[T, O, R](this, leftKey)
  }

  /**
   * Specify the key selector function for the left side of the key based operation. This returns
   * a [[HalfUnfinishedKeyPairOperation]] on which `isEqualTo` must be called to specify the
   * key for the right side. The result after specifying the right side key is the finished
   * operation.
   */
  def where[K: TypeInformation](fun: (T) => K) = {
    val keyType = implicitly[TypeInformation[K]]
    val keyExtractor = new KeySelector[T, K] {
      def getKey(in: T) = fun(in)
    }
    val leftKey = new Keys.SelectorFunctionKeys[T, K](keyExtractor, leftSet.getType, keyType)
    new HalfUnfinishedKeyPairOperation[T, O, R](this, leftKey)
  }
}

private[flink] class HalfUnfinishedKeyPairOperation[T, O, R](
    unfinished: UnfinishedKeyPairOperation[T, O, R], leftKey: Keys[T]) {

  /**
   * Specify the key fields for the right side of the key based operation. This returns
   * the finished operation.
   *
   * This only works on a Tuple [[DataSet]].
   */
  def equalTo(rightKeys: Int*): R = {
    val rightKey = new FieldPositionKeys[O](rightKeys.toArray, unfinished.rightSet.getType)
    if (!leftKey.areCompatibale(rightKey)) {
      throw new InvalidProgramException("The types of the key fields do not match. Left: " +
        leftKey + " Right: " + rightKey)
    }
    unfinished.finish(leftKey, rightKey)
  }

  /**
   * Specify the key selector function for the right side of the key based operation. This returns
   * the finished operation.
   */
  def equalTo[K: TypeInformation](fun: (O) => K) = {
    val keyType = implicitly[TypeInformation[K]]
    val keyExtractor = new KeySelector[O, K] {
      def getKey(in: O) = fun(in)
    }
    val rightKey =
      new Keys.SelectorFunctionKeys[O, K](keyExtractor, unfinished.rightSet.getType, keyType)
    if (!leftKey.areCompatibale(rightKey)) {
      throw new InvalidProgramException("The types of the key fields do not match. Left: " +
        leftKey + " Right: " + rightKey)
    }
    unfinished.finish(leftKey, rightKey)
  }
}
