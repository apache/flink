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

package org.apache.flink.runtime.jobmanager

import java.io.IOException
import java.net.URI
import java.util

import akka.actor.ActorRef
import grizzled.slf4j.Logger
import org.apache.flink.api.common.JobID
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.messages.accumulators._
import org.apache.flink.runtime.webmonitor.WebMonitorUtils
import org.apache.flink.runtime.{FlinkActor, LogMessages}
import org.apache.flink.runtime.messages.webmonitor._
import org.apache.flink.runtime.executiongraph.{ArchivedExecutionGraph, ExecutionGraph}
import org.apache.flink.runtime.history.FsJobArchivist
import org.apache.flink.runtime.messages.ArchiveMessages._
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview.JobIdWithStatus

import scala.collection.mutable
import scala.concurrent.future

/**
 * Actor which stores terminated Flink jobs. The number of stored Flink jobs is set by max_entries.
 * If this number is exceeded, the oldest job will be discarded. One can interact with the actor by
 * the following messages:
 *
 *  - [[ArchiveExecutionGraph]] archives the attached [[ExecutionGraph]]
 *
 *  - [[RequestArchivedJobs]] returns all currently stored [[ExecutionGraph]]s to the sender
 *  encapsulated in a [[ArchivedJobs]] message.
 *
 *  - [[RequestJob]] returns the corresponding [[org.apache.flink.runtime.jobgraph.JobGraph]]
 *  encapsulated in [[JobFound]] if the job is stored by the MemoryArchivist. If not, then
 *  [[JobNotFound]] is returned.
 *
 *  - [[RequestJobStatus]] returns the last state of the corresponding job. If the job can be found,
 *  then a [[CurrentJobStatus]] message with the last state is returned to the sender, otherwise
 *  a [[JobNotFound]] message is returned
 *
 *  - [[RequestJobCounts]] returns the number of finished, canceled, and failed jobs as a Tuple3
 *
 * @param maxEntries Maximum number of stored Flink jobs
 * @param archivePath Optional path of the job archive directory
 */
class MemoryArchivist(
    private val maxEntries: Int,
    private val archivePath: Option[Path])
  extends FlinkActor
  with LogMessages {

  override val log = Logger(getClass)

  /*
   * Map of execution graphs belonging to recently started jobs with the time stamp of the last
   * received job event. The insert order is preserved through a LinkedHashMap.
   */
  protected val graphs = mutable.LinkedHashMap[JobID, ArchivedExecutionGraph]()

  /* Counters for finished, canceled, and failed jobs */
  private[this] var finishedCnt: Int = 0
  private[this] var canceledCnt: Int = 0
  private[this] var failedCnt: Int = 0

  override def preStart(): Unit = {
    log.info(s"Started memory archivist ${self.path}")
  }

  override def handleMessage: Receive = {

    /* Receive Execution Graph to archive */
    case ArchiveExecutionGraph(jobID, graph) =>
      // Keep lru order in case we override a graph (from multiple job submission in one session).
      // This deletes old ExecutionGraph with this JobID from the history but avoids to store
      // redundant ExecutionGraphs.
      // TODO Allow ExecutionGraphs with the same jobID to be stored and displayed in web interface
      graphs.remove(jobID)
      graphs.put(jobID, graph)
      // update job counters
      graph.getState match {
        case JobStatus.FINISHED => finishedCnt += 1
        case JobStatus.CANCELED => canceledCnt += 1
        case JobStatus.FAILED => failedCnt += 1
          // ignore transitional states, e.g. Cancelling, Running, Failing, etc.
        case _ =>
      }

      archiveJsonFiles(graph)

      trimHistory()

    case msg : InfoMessage => handleWebServerInfoMessage(msg, sender())

    case RequestArchivedJob(jobID: JobID) =>
      val graph = graphs.get(jobID)
      sender ! decorateMessage(ArchivedJob(graph))

    case RequestArchivedJobs =>
      sender ! decorateMessage(ArchivedJobs(graphs.values))

    case RequestJob(jobID) =>
      graphs.get(jobID) match {
        case Some(graph) => sender ! decorateMessage(JobFound(jobID, graph))
        case None => sender ! decorateMessage(JobNotFound(jobID))
      }

    case RequestJobStatus(jobID) =>
      graphs.get(jobID) match {
        case Some(graph) => sender ! decorateMessage(CurrentJobStatus(jobID, graph.getState))
        case None => sender ! decorateMessage(JobNotFound(jobID))
      }

    case RequestJobCounts =>
      sender ! decorateMessage((finishedCnt, canceledCnt, failedCnt))

    case RequestAccumulatorResults(jobID) =>
      try {
        graphs.get(jobID) match {
          case Some(graph) =>
            val accumulatorValues = graph.getAccumulatorsSerialized()
            sender() ! decorateMessage(AccumulatorResultsFound(jobID, accumulatorValues))
          case None =>
            sender() ! decorateMessage(AccumulatorResultsNotFound(jobID))
        }
      } catch {
        case e: Exception =>
          log.error("Cannot serialize accumulator result.", e)
          sender() ! decorateMessage(AccumulatorResultsErroneous(jobID, e))
      }

      case RequestAccumulatorResultsStringified(jobID) =>
        graphs.get(jobID) match {
          case Some(graph) =>
            val accumulatorValues = graph.getAccumulatorResultsStringified()
            sender() ! decorateMessage(AccumulatorResultStringsFound(jobID, accumulatorValues))
          case None =>
            sender() ! decorateMessage(AccumulatorResultsNotFound(jobID))
        }
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }


  private def handleWebServerInfoMessage(message: InfoMessage, theSender: ActorRef): Unit = {
    message match {
      case _ : RequestJobsOverview =>
        try {
          sender ! decorateMessage(createJobsOverview())
        }
        catch {
          case t: Throwable => log.error("Exception while creating the jobs overview", t)
        }

      case _ : RequestJobsWithIDsOverview =>
        try {
          sender ! decorateMessage(createJobsWithIDsOverview())
        }
        catch {
          case t: Throwable => log.error("Exception while creating the jobs overview", t)
        }

      case _ : RequestJobDetails =>
        val details = graphs.values.map {
          v => WebMonitorUtils.createDetailsForJob(v)
        }.toArray[JobDetails]

        theSender ! decorateMessage(new MultipleJobsDetails(util.Arrays.asList(details: _*)))
    }
  }

  private def archiveJsonFiles(graph: ArchivedExecutionGraph) {
    // a suspended job is expected to continue on another job manager,
    // so we aren't archiving it yet.
    if (archivePath.isDefined && graph.getState.isGloballyTerminalState) {
      try {
        val p = validateAndNormalizeUri(archivePath.get.toUri)
        future {
          try {
            FsJobArchivist.archiveJob(p, graph)
          } catch {
            case e: Exception =>
              log.error("Failed to archive job.", e)
          }
        }(context.dispatcher)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to create Path for $archivePath. Job will not be archived.", e)
      }
    }
  }

  // --------------------------------------------------------------------------
  //  Request Responses
  // --------------------------------------------------------------------------

  private def createJobsOverview() : JobsOverview = {
    new JobsOverview(0, finishedCnt, canceledCnt, failedCnt)
  }

  private def createJobsWithIDsOverview() : JobIdsWithStatusOverview = {
    val jobIdsWithStatuses =
      new java.util.ArrayList[JobIdWithStatus](graphs.size)

    graphs.values.foreach { graph =>
      jobIdsWithStatuses.add(
        new JobIdWithStatus(graph.getJobID, graph.getState))
    }

    new JobIdsWithStatusOverview(jobIdsWithStatuses)
  }

  // --------------------------------------------------------------------------
  //  Utilities
  // --------------------------------------------------------------------------

  /**
   * Remove old ExecutionGraphs belonging to a jobID
   * * if more than max_entries are in the queue.
   */
  private def trimHistory(): Unit = {
    while (graphs.size > maxEntries) {
      // get first graph inserted
      val (jobID, value) = graphs.head
      graphs.remove(jobID)
    }
  }

  /**
    * Checks and normalizes the archive path URI. This method first checks the validity of the
    * URI (scheme, path, availability of a matching file system) and then normalizes the URL
    * to a path.
    *
    * If the URI does not include an authority, but the file system configured for the URI has an
    * authority, then the normalized path will include this authority.
    *
    * @param archivePathUri The URI to check and normalize.
    * @return a normalized URI as a Path.
    *
    * @throws IllegalArgumentException Thrown, if the URI misses schema or path.
    * @throws IOException Thrown, if no file system can be found for the URI's scheme.
    */
  @throws[IOException]
  private def validateAndNormalizeUri(archivePathUri: URI): Path = {
    val scheme = archivePathUri.getScheme
    val path = archivePathUri.getPath

    // some validity checks
    if (scheme == null) {
      throw new IllegalArgumentException("The scheme (hdfs://, file://, etc) is null. " +
        "Please specify the file system scheme explicitly in the URI: " + archivePathUri)
    }

    if (path == null) {
      throw new IllegalArgumentException("The path to store the job archives is null. " +
        "Please specify a directory path for storing job archives. and the URI is: " +
        archivePathUri)
    }

    if (path.length == 0 || path == "/") {
      throw new IllegalArgumentException("Cannot use the root directory for storing job archives.")
    }

    try {
      FileSystem.get(archivePathUri)
    }
    catch {
      case e: Exception =>
          throw new IllegalArgumentException(s"No file system found for URI '${archivePathUri}'.")
    }
    new Path(archivePathUri)
  }
}
