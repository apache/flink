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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.planner.plan.`trait`.MiniBatchInterval

import org.apache.calcite.rex.RexBuilder

/**
  * A OptimizeContext allows to obtain stream table environment information when optimizing.
  */
trait StreamOptimizeContext extends FlinkOptimizeContext {

  /**
   * Returns true if the root is required to send UPDATE_BEFORE message with
   * UPDATE_AFTER message together for update changes.
   */
  def isUpdateBeforeRequired: Boolean

  /**
    * Returns the mini-batch interval that sink requests.
    */
  def getMiniBatchInterval: MiniBatchInterval

}
