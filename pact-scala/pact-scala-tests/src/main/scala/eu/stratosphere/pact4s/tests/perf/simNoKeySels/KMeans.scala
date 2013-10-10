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

package eu.stratosphere.pact4s.tests.perf.simNoKeySels

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class KMeansDescriptor extends PactDescriptor[KMeans] {
  override val name = "KMeans (SimNoKeySels)"
  override val parameters = "[-numIterations <int:2>] -dataPoints <file> -clusterCenters <file> -output <file>"

  override def createInstance(args: Pact4sArgs) = new KMeans(args("numIterations", "2").toInt, args("dataPoints"), args("clusterCenters"), args("output"))
}

class KMeans(numIterations: Int, dataPointInput: String, clusterInput: String, clusterOutput: String) extends PactProgram {

  import KMeans._

  val dataPoints = new DataSource(dataPointInput, new RecordDataSourceFormat[Point]("\n", "|"))
  val clusterPoints = new DataSource(clusterInput, new RecordDataSourceFormat[Point]("\n", "|"))
  val newClusterPoints = new DataSink(clusterOutput, new RecordDataSinkFormat[Point]("\n", "|", true))

  val finalCenters = computeNewCenters repeat (n = numIterations, s0 = clusterPoints)

  override def outputs = newClusterPoints <~ finalCenters

  def computeNewCenters = (centers: DataStream[Point]) => {

    val distances = dataPoints cross centers map { (p, c) =>
      val dist = math.sqrt(math.pow(p.x - c.x, 2) + math.pow(p.y - c.y, 2))
      Distance(p.id, p.x, p.y, c.id, dist)
    }

    val nearestCenters = (distances map { identity _ }) groupBy { _.pid } combine { ds => ds.minBy(_.dist) } map { d => PointSum(d.cid, d.px, d.py, 1) }

    val newCenters = (nearestCenters map { identity _ }) groupBy { _.id } combine { ds =>

      var p: PointSum = null
      var count = 0
      var xSum = 0d
      var ySum = 0d

      while (ds.hasNext) {
        p = ds.next
        count += p.count
        xSum += p.xSum
        ySum += p.ySum
      }

      PointSum(p.id, xSum, ySum, count)
      
    } map { ps =>
      Point(ps.id, math.round(ps.xSum * 100.0) / 100.0, math.round(ps.ySum * 100.0) / 100.0)
    }

    distances.left neglects { p => p.id }
    distances.left preserves { p => (p.id, p.x, p.y) } as { d => (d.pid, d.px, d.py) }
    distances.right neglects { c => c.id }
    distances.right preserves { c => c.id } as { d => d.cid }

    nearestCenters neglects { case d => d.pid }

    newCenters neglects { ps => ps.id }
    newCenters.preserves { ps => ps.id } as { c => c.id }

    distances.avgBytesPerRecord(48)
    nearestCenters.avgBytesPerRecord(40)
    newCenters.avgBytesPerRecord(36)

    newCenters
  }

  clusterPoints.degreeOfParallelism(1)
}

object KMeans {
  case class Point(val id: Int, val x: Double, val y: Double)
  case class Distance(val pid: Int, val px: Double, val py: Double, val cid: Int, val dist: Double)
  case class PointSum(val id: Int, val xSum: Double, val ySum: Double, val count: Int)  
}
