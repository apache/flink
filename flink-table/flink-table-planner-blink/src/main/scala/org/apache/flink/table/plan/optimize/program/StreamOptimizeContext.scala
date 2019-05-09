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

package org.apache.flink.table.plan.optimize.program

import org.apache.calcite.rex.RexBuilder

/**
  * A OptimizeContext allows to obtain stream table environment information when optimizing.
  */
trait StreamOptimizeContext extends FlinkOptimizeContext {

  /**
    * Gets the Calcite [[RexBuilder]] defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getRexBuilder: RexBuilder

  /**
    * Returns true if the sink requests updates as retraction messages
    * defined in [[org.apache.flink.table.plan.optimize.StreamOptimizer.optimize]].
    */
  def updateAsRetraction: Boolean

  /**
    * Returns true if the output node needs final TimeIndicator conversion
    * defined in [[org.apache.flink.table.api.TableEnvironment.optimize]].
    */
  def needFinalTimeIndicatorConversion: Boolean

}
