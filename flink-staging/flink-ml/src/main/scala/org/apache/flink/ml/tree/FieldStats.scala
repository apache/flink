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

/** Keeps useful statistics about a field
  * fieldType is false for categorical fields, true for continuous fields
  * For continuous field, minimum and maximum values.
  * Usually, min-(max-min) and max+(max-min) from the data should suffice
  * For categorical field, list of categories
  *
  */

class FieldStats(
                  val fieldType: Boolean,
                  val fieldMinValue: Double = -java.lang.Double.MAX_VALUE,
                  val fieldMaxValue: Double = java.lang.Double.MAX_VALUE,
                  val fieldCategories: collection.mutable.HashMap[Double, Int] =
                  new collection.mutable.HashMap[Double, Int]) {

  override def toString: String = {
    if (fieldType)
      s"Continuous field: Range: ($fieldMinValue,$fieldMaxValue)"
    else
      s"Categorical field: Number of categories: $fieldCategories"
  }
}