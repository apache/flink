/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala.examples.relational;

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

object RunWebLogAnalysis {
  def main(args: Array[String]) {
    val webLogAnalysis = new WebLogAnalysis
    if (args.size < 5) {
      println(webLogAnalysis.getDescription)
      return
    }
    val plan = webLogAnalysis.getScalaPlan(args(0).toInt, args(1), args(2), args(3), args(4))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

/**
 * Implements the following relational OLAP query as PACT program:
 * 
 * <code><pre>
 * SELECT r.pageURL, r.pageRank, r.avgDuration
 * FROM Documents d JOIN Rankings r
 * 	ON d.url = r.url
 * WHERE CONTAINS(d.text, [keywords])
 * 	AND r.rank > [rank]
 * 	AND NOT EXISTS (
 * 		SELECT * FROM Visits v
 * 		WHERE v.destUrl = d.url
 * 			AND v.visitDate < [date]); 
 *  * </pre></code> 
 * 
 * Table Schemas: <code><pre>
 * CREATE TABLE Documents (
 * 					url VARCHAR(100) PRIMARY KEY,
 * 					contents TEXT );
 * 
 * CREATE TABLE Rankings (
 * 					pageRank INT,
 * 					pageURL VARCHAR(100) PRIMARY KEY,     
 * 					avgDuration INT );       
 * 
 * CREATE TABLE Visits (
 * 					sourceIP VARCHAR(16),
 * 					destURL VARCHAR(100),
 * 					visitDate DATE,
 * 					adRevenue FLOAT,
 * 					userAgent VARCHAR(64),
 * 					countryCode VARCHAR(3),
 * 					languageCode VARCHAR(6),
 * 					searchWord VARCHAR(32),
 * 					duration INT );
 * </pre></code>
 * 
 */
class WebLogAnalysis extends PlanAssembler with PlanAssemblerDescription with Serializable {
  
  override def getDescription() = {
    "Parameters: [numSubStasks], [docs], [rankings], [visits], [output]"
  }
  
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3), args(4))
  }
  
  // document tuple
  case class Doc(url: String, text: String)
  // rank tuple
  case class Rank(rank: Int, url: String, avgDuration: Int)
  // visit tuple
  case class Visit(url: String, date: String)
  
  // format rank for output
  def formatRank = (r: Rank) => "%d|%s|%d".format(r.rank, r.url, r.avgDuration)

  def getScalaPlan(numSubTasks: Int, docsInput: String, rankingsInput: String, visitsInput: String, ranksOutput: String) = {
    
    // read documents data
    val docs = DataSource(docsInput, CsvInputFormat[Doc]("\n", "|"))
    // read ranks data
    val ranks = DataSource(rankingsInput, CsvInputFormat[Rank]("\n", "|"))
    // read visits data and project to visits tuple afterwards
    val visits = DataSource(visitsInput, CsvInputFormat[(String, String, String)]("\n", "|")) map (x => Visit(x._2, x._3))

    // filter on documents that contain certain key words and project to URL
    val filteredDocs = docs filter {d => d.text.contains(" editors ") && d.text.contains(" oscillations ") && d.text.contains(" convection ")} map { d => d.url }
    filteredDocs.avgRecordsEmittedPerCall(.15f).avgBytesPerRecord(60).uniqueKey({url => url})
    filteredDocs.observes(d => (d.text))
    filteredDocs.preserves({d => d.url}, {url => url} )
    
    // filter on ranks that have a certain minimum rank
    val filteredRanks = ranks filter {r => r.rank > 50}
    filteredRanks.avgRecordsEmittedPerCall(.25f).uniqueKey(_.url)
    filteredRanks.observes(r => (r.rank))
    filteredRanks.preserves({r => r}, {r => r} )
    
    // filter on visits of the year 2010 and project to URL
    val filteredVisits = visits filter {v => v.date.substring(0,4).equals("2010")} map { v => v.url }
    filteredVisits.avgRecordsEmittedPerCall(.2f).avgBytesPerRecord(60)
    filteredVisits.observes(v => (v.date))
    filteredVisits.preserves( {v => v.url}, {url => url} )
    
    // filter for ranks on documents that contain certain key words 
    val ranksFilteredByDocs = filteredDocs join filteredRanks where {url => url} isEqualTo {r => r.url} map ((d,r) => r)
    ranksFilteredByDocs.avgRecordsEmittedPerCall(1.0f)
    ranksFilteredByDocs.left.neglects( {d => d} )
    ranksFilteredByDocs.right.preserves( {r => r}, {r => r} )
    
    // filter for ranks on documents that have not been visited in 2010
    val ranksFilteredByDocsAndVisits = ranksFilteredByDocs cogroup filteredVisits where {r => r.url} isEqualTo {url => url} map ( (rs, vs) => if (vs.hasNext) Nil else rs.toList ) flatMap {rs => rs.iterator }
    ranksFilteredByDocsAndVisits.avgRecordsEmittedPerCall(.8f)
    ranksFilteredByDocs.left.preserves( {r => r}, {r => r} )
    ranksFilteredByDocs.right.neglects( {v => v} )
    
    // emit the resulting ranks
    val output = ranksFilteredByDocsAndVisits.write(ranksOutput, DelimitedOutputFormat(formatRank))

    val plan = new ScalaPlan(Seq(output), "WebLog Analysis")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }

}

