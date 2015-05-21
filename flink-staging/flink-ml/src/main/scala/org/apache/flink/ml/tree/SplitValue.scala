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


package org.apache.flink.ml.tree

import java.util

import org.apache.flink.ml.math.Vector

/**
 * Implements a split value class which determines whether to send an instance to the
 * left tree or the right tree
 *
 * Attribute is the index of the feature at which to split. Starts from 0 to {d-1} where d
 * is the dimensionality
 * splitType is true if the split is done using "<= splitValueDouble" for continuous fields
 * splitType is false if the split is done using "in splitValueList" for categorical fields
 *
 * getSplitDirection returns true if the instance should go the left tree and false if it should
 * go to the right tree
 **/

class SplitValue(
                  val attribute: Int,
                  val splitType: Boolean,
                  val splitValueDouble: Double = 0,
                  val splitValueList: util.ArrayList[Double] = new util.ArrayList[Double]) {

  override def toString: String = {
    if (splitType)
      s"Attribute Index: $attribute, Split: Continuous Value at $splitValueDouble"
    else
      s"Attribute Index: $attribute, Split: Categorical at $splitValueList"
  }

  def getSplitDirection(vector: Vector): Boolean = {
    if (splitType)
      vector.apply(attribute) <= splitValueDouble // go left if less than equal to
    else
      splitValueList.contains(vector.apply(attribute)) // go left is exists
  }
}