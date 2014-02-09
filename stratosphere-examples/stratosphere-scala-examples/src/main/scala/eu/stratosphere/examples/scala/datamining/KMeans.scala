/**
 * *********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * ********************************************************************************************************************
 */

package eu.stratosphere.examples.scala.datamining

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._


class KMeans extends Program with ProgramDescription with Serializable {

  case class Point(x: Double, y: Double, z: Double) {
    
    def +(other: Point) = { Point(x + other.x, y + other.y, z + other.z) }
    
    def /(div: Int) = { Point(x / div, y / div, z / div) }
    
    def computeEuclidianDistance(other: Point) = {
      math.sqrt(math.pow(x - other.x, 2) + math.pow(y - other.y, 2) + math.pow(z - other.z, 2))
    }
  }

  case class Distance(dataPoint: Point, clusterId: Int, distance: Double)
  
  def formatCenterOutput = ((cid: Int, p: Point) => "%d|%.1f|%.1f|%.1f|".format(cid, p.x, p.y, p.z)).tupled


  def getScalaPlan(dop: Int, dataPointInput: String, clusterInput: String, clusterOutput: String, numIterations: Int) = {
    
    val dataPoints = DataSource(dataPointInput, CsvInputFormat[(Int, Double, Double, Double)]("\n", '|')) 
                       .map { case (id, x, y, z) => (id, Point(x, y, z)) }
  
    val clusterPoints = DataSource(clusterInput, CsvInputFormat[(Int, Double, Double, Double)]("\n", '|'))
                       .map { case (id, x, y, z) => (id, Point(x, y, z)) }


    // iterate the K-Means function, starting with the initial cluster points
    val finalCenters = clusterPoints.iterate(numIterations, { centers =>

        // compute the distance between each point and all current centroids
        val distances = dataPoints cross centers map { (point, center) =>
            val ((pid, dataPoint), (cid, clusterPoint)) = (point, center)
            val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)
            (pid, Distance(dataPoint, cid, distToCluster))
        }
      
        // pick for each point the closest centroid
        val nearestCenters = distances groupBy { case (pid, _) => pid } reduceGroup { ds => ds.minBy(_._2.distance) }
        
        // for each centroid, average among all data points that have chosen it as the closest one
        // the average is computed as sum, count, finalized as sum/count
        nearestCenters
              .map { case (_, Distance(dataPoint, cid, _)) => (cid, dataPoint, 1) }
              .groupBy {_._1} .reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3) }
              .map { case (cid, centerPoint, num) => (cid, centerPoint / num) }
    })

    val output = finalCenters.write(clusterOutput, DelimitedOutputFormat(formatCenterOutput))

    new ScalaPlan(Seq(output), "KMeans Iteration")
  }

  
  /**
   * The program entry point for the packaged version of the program.
   * 
   * @param args The command line arguments, including, consisting of the following parameters:
   *             <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>"
   * @return The program plan of the kmeans example program.
   */
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3), args(4).toInt)
  }
    
  override def getDescription() = {
    "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>"
  }
}

/**
 * Entry point to make the example standalone runnable with the local executor
 */
object RunKMeans {

  def main(args: Array[String]) {
    val km = new KMeans
    if (args.size < 5) {
      println(km.getDescription)
      return
    }
    val plan = km.getScalaPlan(args(0).toInt, args(1), args(2), args(3), args(4).toInt)
    LocalExecutor.execute(plan)
  }
}