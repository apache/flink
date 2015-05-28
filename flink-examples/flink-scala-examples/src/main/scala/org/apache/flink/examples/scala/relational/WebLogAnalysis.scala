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
 *   WebLogAnalysis <documents path> <ranks path> <visits path> <result path>
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
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val documents = getDocumentsDataSet(env)
    val ranks = getRanksDataSet(env)
    val visits = getVisitsDataSet(env)

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
      (ranks, visits, out: Collector[(Int, String, Int)]) =>
        if (visits.isEmpty) for (rank <- ranks) out.collect(rank)
    }.withForwardedFieldsFirst("*")




    // emit result
    if (fileOutput) {
      result.writeAsCsv(outputPath, "\n", "|")
      env.execute("Scala WebLogAnalysis Example")
    } else {
      result.print()
    }

  }

  private var fileOutput: Boolean = false
  private var documentsPath: String = null
  private var ranksPath: String = null
  private var visitsPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 4) {
        documentsPath = args(0)
        ranksPath = args(1)
        visitsPath = args(2)
        outputPath = args(3)
      }
      else {
        System.err.println("Usage: WebLogAnalysis <documents path> <ranks path> <visits path> " +
          "<result path>")
        return false
      }
    }
    else {
      System.out.println("Executing WebLog Analysis example with built-in default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  We provide a data generator to create synthetic input files for this " +
        "program.")
      System.out.println("  Usage: WebLogAnalysis <documents path> <ranks path> <visits path> " +
        "<result path>")
    }
    true
  }

  private def getDocumentsDataSet(env: ExecutionEnvironment): DataSet[(String, String)] = {
    if (fileOutput) {
      env.readCsvFile[(String, String)](
        documentsPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 1))
    }
    else {
      val documents = WebLogData.DOCUMENTS map {
        case Array(x, y) => (x.asInstanceOf[String], y.asInstanceOf[String])
      }
      env.fromCollection(documents)
    }
  }

  private def getRanksDataSet(env: ExecutionEnvironment): DataSet[(Int, String, Int)] = {
    if (fileOutput) {
      env.readCsvFile[(Int, String, Int)](
        ranksPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 2))
    }
    else {
      val ranks = WebLogData.RANKS map {
        case Array(x, y, z) => (x.asInstanceOf[Int], y.asInstanceOf[String], z.asInstanceOf[Int])
      }
      env.fromCollection(ranks)
    }
  }

  private def getVisitsDataSet(env: ExecutionEnvironment): DataSet[(String, String)] = {
    if (fileOutput) {
      env.readCsvFile[(String, String)](
        visitsPath,
        fieldDelimiter = "|",
        includedFields = Array(1, 2))
    }
    else {
      val visits = WebLogData.VISITS map {
        case Array(x, y) => (x.asInstanceOf[String], y.asInstanceOf[String])
      }
      env.fromCollection(visits)
    }
  }
}
