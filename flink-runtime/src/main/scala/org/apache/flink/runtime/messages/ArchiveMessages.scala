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

package org.apache.flink.runtime.messages

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.executiongraph.{ArchivedExecutionGraph, ExecutionGraph}

/**
 * This object contains the archive specific messages.
 */
object ArchiveMessages {
  
  case class ArchiveExecutionGraph(jobID: JobID, graph: ArchivedExecutionGraph)

  /**
   * Request the currently archived jobs in the archiver. The resulting response is [[ArchivedJobs]]
   */
  case object RequestArchivedJobs

  /**
   * Requests the number of finished, canceled, and failed jobs
   */
  case object RequestJobCounts

  /**
   * Request a specific ExecutionGraph by JobID. The response is [[RequestArchivedJob]]
   * @param jobID
   */
  case class RequestArchivedJob(jobID: JobID)

  case class ArchivedJob(job: Option[ArchivedExecutionGraph])

  /**
   * Response to [[RequestArchivedJobs]] message. The response contains the archived jobs.
   * @param jobs
   */
  case class ArchivedJobs(jobs: Iterable[ArchivedExecutionGraph]){
    def asJavaIterable: java.lang.Iterable[ArchivedExecutionGraph] = {
      import scala.collection.JavaConverters._
      jobs.asJava
    }

    def asJavaCollection: java.util.Collection[ArchivedExecutionGraph] = {
      import scala.collection.JavaConverters._
      jobs.asJavaCollection
    }
  }

  // --------------------------------------------------------------------------
  // Utility methods to allow simpler case object access from Java
  // --------------------------------------------------------------------------
  
  def getRequestArchivedJobs : AnyRef = {
    RequestArchivedJobs
  }

  def getRequestJobCounts : AnyRef = {
    RequestJobCounts
  }
}
