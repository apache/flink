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

package org.apache.flink.table.planner.plan.reuse

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.process.{DAGProcessContext, DAGProcessor}

import java.util

import scala.collection.JavaConversions._

/**
 * A DeadlockBreakupProcessor that finds out all deadlocks in the DAG, and resolves them.
 *
 * <p>NOTE: This processor can be only applied on [[BatchExecNode]] DAG.
 */
class DeadlockBreakupProcessor extends DAGProcessor {

  def process(
      rootNodes: util.List[ExecNode[_, _]],
      context: DAGProcessContext): util.List[ExecNode[_, _]] = {
    if (!rootNodes.forall(_.isInstanceOf[BatchExecNode[_]])) {
      throw new TableException("Only BatchExecNode DAG is supported now")
    }

    val resolver = new InputPriorityConflictResolver(rootNodes)
    resolver.detectAndResolve()
    rootNodes
  }

}
