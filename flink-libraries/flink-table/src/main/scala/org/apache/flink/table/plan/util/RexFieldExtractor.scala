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

package org.apache.flink.table.plan.util

import org.apache.calcite.rex.RexNode

object RexFieldExtractor {

  /**
    * Extracts the indexes of input fields accessed by the RexNode.
    *
    * @param rex RexNode to analyze
    * @return The indexes of accessed input fields
    */
  def extractRefInputFields(rex: RexNode): Array[Int] = {
    val visitor = new RefFieldsVisitor
    rex.accept(visitor)
    visitor.getFields
  }
}
