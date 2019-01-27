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
package org.apache.flink.table.plan.nodes.exec

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor

import java.lang.{Double => JDouble}

trait BatchExecNode[T] extends ExecNode[BatchTableEnvironment, T] {

  /**
    * Returns [[DamBehavior]] of this node.
    */
  def getDamBehavior: DamBehavior

  /**
    * Accepts a visit from a [[BatchExecNodeVisitor]].
    *
    * @param visitor BatchExecNodeVisitor
    */
  def accept(visitor: BatchExecNodeVisitor): Unit

  /**
    * Gets estimated row count it need process.
    */
  def getEstimatedRowCount: JDouble

  /**
    * Gets estimated total mem when it processes.
    */
  def getEstimatedTotalMem: JDouble

  /**
    * Gets estimated row average size it process.
    */
  def getEstimatedAverageRowSize: JDouble

}
