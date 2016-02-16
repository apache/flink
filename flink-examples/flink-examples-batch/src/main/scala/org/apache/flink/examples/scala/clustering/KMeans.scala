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
package org.apache.flink.examples.scala.clustering

import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.examples.java.clustering.util.KMeansData

import scala.collection.JavaConverters._

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * K-Means is an iterative clustering algorithm and works as follows:
 * K-Means is given a set of data points to be clustered and an initial set of ''K'' cluster
 * centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (''mean'') of all points that have
 * been assigned to it.
 * The moved cluster centers are fed into the next iteration. 
 * The algorithm terminates after a fixed number of iterations (as in this implementation) 
 * or if cluster centers do not (significantly) move in an iteration.
 * This is the Wikipedia entry for the [[http://en.wikipedia
 * .org/wiki/K-means_clustering K-Means Clustering algorithm]].
 *
 * This implementation works on two-dimensional data points.
 * It computes an assignment of data points to cluster centers, i.e., 
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 * Input files are plain text files and must be formatted as follows:
 *
 *  - Data points are represented as two double values separated by a blank character.
 *    Data points are separated by newline characters.
 *    For example `"1.2 2.3\n5.3 7.2\n"` gives two data points (x=1.2, y=2.3) and (x=5.3,
 *    y=7.2).
 *  - Cluster centers are represented by an integer id and a point value.
 *    For example `"1 6.2 3.2\n2 2.9 5.7\n"` gives two centers (id=1, x=6.2,
 *    y=3.2) and (id=2, x=2.9, y=5.7).
 *
 * Usage:
 * {{{
 *   KMeans <points path> <centers path> <result path> <num iterations>
 * }}}
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.clustering.util.KMeansData]]
 * and 10 iterations.
 *
 * This example shows how to use:
 *
 *  - Bulk iterations
 *  - Broadcast variables in bulk iterations
 *  - Custom Java objects (PoJos)
 */
object KMeans {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val points: DataSet[Point] = getPointDataSet(env)
    val centroids: DataSet[Centroid] = getCentroidDataSet(env)

    val finalCentroids = centroids.iterate(numIterations) { currentCentroids =>
      val newCentroids = points
        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }.withForwardedFields("_1; _2")
        .groupBy(0)
        .reduce { (p1, p2) => (p1._1, p1._2.add(p2._2), p1._3 + p2._3) }.withForwardedFields("_1")
        .map { x => new Centroid(x._1, x._2.div(x._3)) }.withForwardedFields("_1->id")
      newCentroids
    }

    val clusteredPoints: DataSet[(Int, Point)] =
      points.map(new SelectNearestCenter).withBroadcastSet(finalCentroids, "centroids")

    if (fileOutput) {
      clusteredPoints.writeAsCsv(outputPath, "\n", " ")
      env.execute("Scala KMeans Example")
    }
    else {
      clusteredPoints.print()
    }

  }

  private def parseParameters(programArguments: Array[String]): Boolean = {
    if (programArguments.length > 0) {
      fileOutput = true
      if (programArguments.length == 4) {
        pointsPath = programArguments(0)
        centersPath = programArguments(1)
        outputPath = programArguments(2)
        numIterations = Integer.parseInt(programArguments(3))

        true
      }
      else {
        System.err.println("Usage: KMeans <points path> <centers path> <result path> <num " +
          "iterations>")

        false
      }
    }
    else {
      System.out.println("Executing K-Means example with default parameters and built-in default " +
        "data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  We provide a data generator to create synthetic input files for this " +
        "program.")
      System.out.println("  Usage: KMeans <points path> <centers path> <result path> <num " +
        "iterations>")

      true
    }
  }

  private def getPointDataSet(env: ExecutionEnvironment): DataSet[Point] = {
    if (fileOutput) {
      env.readCsvFile[(Double, Double)](
        pointsPath,
        fieldDelimiter = " ",
        includedFields = Array(0, 1))
        .map { x => new Point(x._1, x._2)}
    }
    else {
      val points = KMeansData.POINTS map {
        case Array(x, y) => new Point(x.asInstanceOf[Double], y.asInstanceOf[Double])
      }
      env.fromCollection(points)
    }
  }

  private def getCentroidDataSet(env: ExecutionEnvironment): DataSet[Centroid] = {
    if (fileOutput) {
      env.readCsvFile[(Int, Double, Double)](
        centersPath,
        fieldDelimiter = " ",
        includedFields = Array(0, 1, 2))
        .map { x => new Centroid(x._1, x._2, x._3)}
    }
    else {
      val centroids = KMeansData.CENTROIDS map {
        case Array(id, x, y) =>
          new Centroid(id.asInstanceOf[Int], x.asInstanceOf[Double], y.asInstanceOf[Double])
      }
      env.fromCollection(centroids)
    }
  }

  private var fileOutput: Boolean = false
  private var pointsPath: String = null
  private var centersPath: String = null
  private var outputPath: String = null
  private var numIterations: Int = 10

  /**
   * A simple two-dimensional point.
   */
  class Point(var x: Double, var y: Double) extends Serializable {
    def this() {
      this(0, 0)
    }

    def add(other: Point): Point = {
      x += other.x
      y += other.y
      this
    }

    def div(other: Long): Point = {
      x /= other
      y /= other
      this
    }

    def euclideanDistance(other: Point): Double = {
      Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y))
    }

    def clear(): Unit = {
      x = 0
      y = 0
    }

    override def toString: String = {
      x + " " + y
    }
  }

  /**
   * A simple two-dimensional centroid, basically a point with an ID.
   */
  class Centroid(var id: Int, x: Double, y: Double) extends Point(x, y) {
    def this() {
      this(0, 0, 0)
    }

    def this(id: Int, p: Point) {
      this(id, p.x, p.y)
    }

    override def toString: String = {
      id + " " + super.toString
    }
  }

  /** Determines the closest cluster center for a data point. */
  @ForwardedFields(Array("*->_2"))
  final class SelectNearestCenter extends RichMapFunction[Point, (Int, Point)] {
    private var centroids: Traversable[Centroid] = null

    /** Reads the centroid values from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[Centroid]("centroids").asScala
    }

    def map(p: Point): (Int, Point) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for (centroid <- centroids) {
        val distance = p.euclideanDistance(centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = centroid.id
        }
      }
      (closestCentroidId, p)
    }

  }
}


