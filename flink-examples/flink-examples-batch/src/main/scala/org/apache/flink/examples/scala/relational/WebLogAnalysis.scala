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

package org.apache.flink.examples.scala.relational

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.examples.java.relational.util.WebLogData
import org.apache.flink.util.Collector

/**
 * This program processes web logs and relational data.
 * It implements the following relational query:
 *
 * {{{
 * SELECT
 *       r.pageURL,
 *       r.pageRank,
 *       r.avgDuration
 * FROM documents d JOIN rankings r
 *                  ON d.url = r.url
 * WHERE CONTAINS(d.text, [keywords])
 *       AND r.rank > [rank]
 *       AND NOT EXISTS
 *           (
 *              SELECT * FROM Visits v
 *              WHERE v.destUrl = d.url
 *                    AND v.visitDate < [date]
 *           );
 * }}}
 *
 *
 * Input files are plain text CSV files using the pipe character ('|') as field separator.
 * The tables referenced in the query can be generated using the
 * [org.apache.flink.examples.java.relational.util.WebLogDataGenerator]] and
 * have the following schemas
 *
 * {{{
 * CREATE TABLE Documents (
 *                url VARCHAR(100) PRIMARY KEY,
 *                contents TEXT );
 *
 * CREATE TABLE Rankings (
 *                pageRank INT,
 *                pageURL VARCHAR(100) PRIMARY KEY,
 *                avgDuration INT );
 *
 * CREATE TABLE Visits (
 *                sourceIP VARCHAR(16),
 *                destURL VARCHAR(100),
 *                visitDate DATE,
 *                adRevenue FLOAT,
 *                userAgent VARCHAR(64),
 *                countryCode VARCHAR(3),
 *                languageCode VARCHAR(6),
 *                searchWord VARCHAR(32),
 *                duration INT );
 * }}}
 *
 *
 * Usage
 * {{{
 *   WebLogAnalysis --documents <path> --ranks <path> --visits <path> --output <path>
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.relational.util.WebLogData]].
 *
 * This example shows how to use:
 *
 *  - tuple data types
 *  - projection and join projection
 *  - the CoGroup transformation for an anti-join
 *
 */
object WebLogAnalysis {

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val documents = getDocumentsDataSet(env, params)
    val ranks = getRanksDataSet(env, params)
    val visits = getVisitsDataSet(env, params)

    val filteredDocs = documents
      .filter(doc => doc._2.contains(" editors ") && doc._2.contains(" oscillations "))

    val filteredRanks = ranks
      .filter(rank => rank._1 > 40)

    val filteredVisits = visits
      .filter(visit => visit._2.substring(0, 4).toInt == 2007)

    val joinDocsRanks = filteredDocs.join(filteredRanks).where(0).equalTo(1) {
      (doc, rank) => rank
    }.withForwardedFieldsSecond("*")

    val result = joinDocsRanks.coGroup(filteredVisits).where(1).equalTo(0) {
      (
        ranks: Iterator[(Int, String, Int)],
        visits: Iterator[(String, String)],
        out: Collector[(Int, String, Int)]) =>
          if (visits.isEmpty) for (rank <- ranks) out.collect(rank)
    }.withForwardedFieldsFirst("*")

    // emit result
    if (params.has("output")) {
      result.writeAsCsv(params.get("output"), "\n", "|")
      env.execute("Scala WebLogAnalysis Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      result.print()
    }

  }

  private def getDocumentsDataSet(env: ExecutionEnvironment, params: ParameterTool):
  DataSet[(String, String)] = {
    if (params.has("documents")) {
      env.readCsvFile[(String, String)](
        params.get("documents"),
        fieldDelimiter = "|",
        includedFields = Array(0, 1))
    } else {
      println("Executing WebLogAnalysis example with default documents data set.")
      println("Use --documents to specify file input.")
      val documents = WebLogData.DOCUMENTS map {
        case Array(x, y) => (x.asInstanceOf[String], y.asInstanceOf[String])
      }
      env.fromCollection(documents)
    }
  }

  private def getRanksDataSet(env: ExecutionEnvironment, params: ParameterTool):
  DataSet[(Int, String, Int)] = {
    if (params.has("ranks")) {
      env.readCsvFile[(Int, String, Int)](
        params.get("ranks"),
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 2))
    } else {
      println("Executing WebLogAnalysis example with default ranks data set.")
      println("Use --ranks to specify file input.")
      val ranks = WebLogData.RANKS map {
        case Array(x, y, z) => (x.asInstanceOf[Int], y.asInstanceOf[String], z.asInstanceOf[Int])
      }
      env.fromCollection(ranks)
    }
  }

  private def getVisitsDataSet(env: ExecutionEnvironment, params: ParameterTool):
  DataSet[(String, String)] = {
    if (params.has("visits")) {
      env.readCsvFile[(String, String)](
        params.get("visits"),
        fieldDelimiter = "|",
        includedFields = Array(1, 2))
    } else {
      println("Executing WebLogAnalysis example with default visits data set.")
      println("Use --visits to specify file input.")
      val visits = WebLogData.VISITS map {
        case Array(x, y) => (x.asInstanceOf[String], y.asInstanceOf[String])
      }
      env.fromCollection(visits)
    }
  }
}
