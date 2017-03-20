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

package org.apache.flink.runtime.testingUtils

import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.jobmanager.MemoryArchivist
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.{ExecutionGraphFound, ExecutionGraphNotFound, RequestExecutionGraph}

/** Memory archivist extended by testing messages
  *
  * @param maxEntries number of maximum number of archived jobs
  */
class TestingMemoryArchivist(maxEntries: Int, archivePath: Option[Path])
  extends MemoryArchivist(maxEntries, archivePath) {

  override def handleMessage: Receive = {
    handleTestingMessage orElse super.handleMessage
  }

  def handleTestingMessage: Receive = {
    case RequestExecutionGraph(jobID) =>
      val executionGraph = graphs.get(jobID)
      
      executionGraph match {
        case Some(graph) => sender ! decorateMessage(ExecutionGraphFound(jobID, graph))
        case None => sender ! decorateMessage(ExecutionGraphNotFound(jobID))
      }
  }
}
