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

import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.Logging

import java.util

import scala.collection.JavaConversions._

/**
  * Base class for [[StreamExecNode]].
  */
trait BaseStreamExecNode[T] extends StreamExecNode[T] with Logging {

  private lazy val inputNodes: util.List[ExecNode[StreamTableEnvironment, _]] =
    new util.ArrayList[ExecNode[StreamTableEnvironment, _]](
      getFlinkPhysicalRel.getInputs.map(_.asInstanceOf[ExecNode[StreamTableEnvironment, _]]))

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = inputNodes

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    require(ordinalInParent >= 0 && ordinalInParent < inputNodes.size())
    inputNodes.set(ordinalInParent, newInputNode)
  }

}

trait RowStreamExecNode extends BaseStreamExecNode[BaseRow]
