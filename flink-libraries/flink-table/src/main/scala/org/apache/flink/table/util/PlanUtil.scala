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

package org.apache.flink.table.util

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.util.FlinkNodeOptUtil
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics._
import org.apache.flink.table.util.PlanUtil._

import org.apache.calcite.sql.SqlExplainLevel
import org.json.JSONObject

import java.io.{PrintWriter, StringWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * The class is to convert [[StreamGraph]] to plan with metrics.
  *
  * @param streamGraph the stream graph to convert
  */
class PlanWihMetrics(streamGraph: StreamGraph) {

  /**
    * Dump plan with accumulated operator metrics to the given file.
    *
    * @param dumpFilePath file path to dump the stream graph plans with operator metrics.
    * @param jobResult    job result of stream graph
    */
  def dumpPlanWithMetrics(dumpFilePath: String, jobResult: JobExecutionResult): Unit = {
    val planWithMetrics = getPlanWithMetrics(jobResult)
    writeContentToFile(planWithMetrics, dumpFilePath)
  }

  /**
    * Get plan with accumulated operator metrics.
    *
    * @param jobResult job result of stream graph
    * @return plan with accumulated operator metrics.
    */
  def getPlanWithMetrics(jobResult: JobExecutionResult): String = {
    val operatorMetrics = extractMetricAccFromResult(jobResult)
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    streamGraph.getSinkIDs.foreach { sinkId =>
      print(sinkId, operatorMetrics, pw)
    }
    pw.close()
    val planWithMetrics = sw.toString
    if (LOG.isDebugEnabled) {
      val jobName = streamGraph.getJobName
      LOG.debug(s"planWithMetrics of Job [$jobName] : \n $planWithMetrics")
    }
    planWithMetrics
  }

  /**
    * Extracts the operator metrics from the accumulators in job result.
    *
    * @param jobResult job result of stream graph
    * @return the operator metrics from the accumulators in job result.
    */
  private def extractMetricAccFromResult(
      jobResult: JobExecutionResult): mutable.Map[Integer, AnyRef] =
    jobResult
      .getAllAccumulatorResults
      .filter(_._1.startsWith(ACCUMULATOR_PREFIX))
      .map { case (accName, accValue) =>
        val nodeID = new Integer(accName.drop(ACCUMULATOR_PREFIX.length))
        (nodeID, accValue)
      }

  private def print(
      vertexID: Int,
      operatorMetrics: mutable.Map[Integer, AnyRef],
      pw: PrintWriter,
      lastChildren: Seq[Boolean] = Nil): Unit = {
    val s = new StringBuilder
    if (lastChildren.nonEmpty) {
      lastChildren.init.foreach { isLast =>
        s.append(if (isLast) "   " else ":  ")
      }
      s.append(if (lastChildren.last) "+- " else ":- ")
    }
    val streamNode = streamGraph.getStreamNode(vertexID)
    val nodeType = if (streamGraph.getSourceIDs.contains(vertexID)) {
      "Data Source"
    } else if (streamGraph.getSinkIDs.contains(vertexID)) {
      "Data Sink"
    } else {
      "Operator"
    }
    s.append(s"Stage $vertexID : $nodeType(${streamNode.getOperatorName}")
    operatorMetrics.get(vertexID) match {
      case Some(metric) =>
        val metricJson = new JSONObject
        // TODO collect more stats
        metricJson.put("rowCount", metric)
        s.append(s" , metric=$metricJson")
      case None =>
    }
    s.append(")")
    pw.println(s)
    val inputNodes = streamNode.getInEdges.map(_.getSourceId)
    if (inputNodes.size() > 1) {
      inputNodes.init.foreach { n =>
        print(n, operatorMetrics, pw, lastChildren :+ false)
      }
    }
    if (inputNodes.nonEmpty) {
      print(inputNodes.last, operatorMetrics, pw, lastChildren :+ true)
    }
  }

}

object PlanUtil extends Logging {

  implicit def toPlanWihMetrics(streamGraph: StreamGraph): PlanWihMetrics =
    new PlanWihMetrics(streamGraph)

  /**
    * Dump ExecNodes into file.
    *
    * @param nodes        ExecNodes to dump
    * @param dumpFilePath file path to write
    */
  def dumpExecNodes(nodes: Seq[ExecNode[_, _]], dumpFilePath: String): Unit = {
    val nodeStr = FlinkNodeOptUtil.dagToString(
      nodes,
      SqlExplainLevel.ALL_ATTRIBUTES,
      withRelNodeId = true,
      withMemCost = true)
    writeContentToFile(nodeStr, dumpFilePath)
    if (LOG.isDebugEnabled) {
      LOG.debug(s"dump ExecNode: \n $nodeStr")
    }
  }

  /**
    * Write content into file.
    * Create a new file if it does not exist.
    * Overwrite the original file if it already exists.
    *
    * @param content  content to dump
    * @param filePath file path to write
    */
  def writeContentToFile(content: String, filePath: String): Unit = {
    require(content != null, "content can not be null! ")
    require(filePath != null && filePath.nonEmpty, "filePath can not be null or empty! ")
    val path = Paths.get(filePath)
    Files.deleteIfExists(path)
    Files.write(
      path,
      Seq(content),
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE)
  }

  /**
    * Converting an StreamGraph to a human-readable string.
    *
    * @param graph stream graph
    */
  def explainPlan(graph: StreamGraph): String = {
    def isSource(id: Int): Boolean = graph.getSourceIDs.contains(id)

    def isSink(id: Int): Boolean = graph.getSinkIDs.contains(id)

    // can not convert to single abstract method because it will throw compile error
    implicit val order: Ordering[Int] = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = (isSink(x), isSink(y)) match {
        case (true, false) => 1
        case (false, true) => -1
        case (_, _) => x - y
      }
    }

    val operatorIDs = graph.getStreamNodes.map(_.getId).toList.sorted(order)
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)

    var tabs = 0
    operatorIDs.foreach { id =>
      val op = graph.getStreamNode(id)
      val (nodeType, content) = if (isSource(id)) {
        tabs = 0
        ("Data Source", "collect elements with CollectionInputFormat")
      } else if (isSink(id)) {
        ("Data Sink", op.getOperatorName)
      } else {
        ("Operator", op.getOperatorName)
      }

      pw.append("\t" * tabs).append(s"Stage $id : $nodeType\n")
        .append("\t" * (tabs + 1)).append(s"content : $content\n")

      if (!isSource(id)) {
        val partition = op.getInEdges.head.getPartitioner.toString
        pw.append("\t" * (tabs + 1)).append(s"ship_strategy : $partition\n")
      }

      pw.append("\n")
      tabs += 1
    }

    pw.close()
    sw.toString
  }

}

