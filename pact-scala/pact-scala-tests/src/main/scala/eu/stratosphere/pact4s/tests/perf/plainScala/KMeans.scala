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

package eu.stratosphere.pact4s.tests.perf.plainScala

import java.util.Iterator

import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.FileDataSource
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable
import eu.stratosphere.pact.common.io.RecordInputFormat
import eu.stratosphere.pact.common.io.RecordOutputFormat
import eu.stratosphere.pact.common.plan.Plan
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.common.stubs.CrossStub
import eu.stratosphere.pact.common.stubs.ReduceStub
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.base.PactDouble
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextDoubleParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextIntParser
import eu.stratosphere.pact.common.util.FieldSet

class KMeans extends PlanAssembler with PlanAssemblerDescription {

  import KMeans._

  override def getPlan(args: String*): Plan = {

    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val dataPointInput = if (args.length > 1) args(1) else ""
    val clusterInput = if (args.length > 2) args(2) else ""
    val output = if (args.length > 3) args(3) else ""

    val dataPoints = new FileDataSource(classOf[RecordInputFormat], dataPointInput, "Data Points")
    RecordInputFormat.configureRecordFormat(dataPoints)
      .recordDelimiter('\n').fieldDelimiter('|')
      .field(classOf[DecimalTextIntParser], 0)
      .field(classOf[DecimalTextDoubleParser], 1)
      .field(classOf[DecimalTextDoubleParser], 2)
    dataPoints.getCompilerHints().setUniqueField(new FieldSet(0))

    val clusterPoints = new FileDataSource(classOf[RecordInputFormat], clusterInput, "Centers")
    RecordInputFormat.configureRecordFormat(clusterPoints)
      .recordDelimiter('\n').fieldDelimiter('|')
      .field(classOf[DecimalTextIntParser], 0)
      .field(classOf[DecimalTextDoubleParser], 1)
      .field(classOf[DecimalTextDoubleParser], 2)
    clusterPoints.setDegreeOfParallelism(1)
    clusterPoints.getCompilerHints().setUniqueField(new FieldSet(0))

    val computeDistance = CrossContract.builder(classOf[ComputeDistance])
      .input1(dataPoints)
      .input2(clusterPoints)
      .name("Compute Distances")
      .build()
    computeDistance.getCompilerHints().setAvgBytesPerRecord(48)

    val findNearestClusterCenters = new ReduceContract.Builder(classOf[FindNearestCenter], classOf[PactInteger], 0)
      .input(computeDistance)
      .name("Find Nearest Centers")
      .build()
    findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(40)

    val recomputeClusterCenter = new ReduceContract.Builder(classOf[RecomputeClusterCenter], classOf[PactInteger], 0)
      .input(findNearestClusterCenters)
      .name("Recompute Center Positions")
      .build()
    recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36)

    val newClusterPoints = new FileDataSink(classOf[RecordOutputFormat], output, recomputeClusterCenter, "New Center Positions")
    RecordOutputFormat.configureRecordFormat(newClusterPoints)
      .recordDelimiter('\n').fieldDelimiter('|').lenient(true)
      .field(classOf[PactInteger], 0)
      .field(classOf[PactDouble], 1)
      .field(classOf[PactDouble], 2)

    val plan = new Plan(newClusterPoints, "KMeans Iteration")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }

  override def getDescription() = "Parameters: [numSubStasks] [dataPoints] [clusterCenters] [output]"
}

object KMeans {

  @ConstantFieldsFirst(fields = Array(0, 1, 2))
  @OutCardBounds(lowerBound = 1, upperBound = 1)
  class ComputeDistance extends CrossStub {

    private val cId = new PactInteger()
    private val cx = new PactDouble()
    private val cy = new PactDouble()
    private val px = new PactDouble()
    private val py = new PactDouble()
    private val dist = new PactDouble()

    override def cross(dataPoint: PactRecord, clusterCenter: PactRecord, out: Collector[PactRecord]) = {
      
	  val cId = clusterCenter.getField(0, this.cId)

	  val cx = clusterCenter.getField(1, this.cx).getValue()
	  val cy = clusterCenter.getField(2, this.cy).getValue()

	  val px = dataPoint.getField(1, this.px).getValue()
	  val py = dataPoint.getField(2, this.py).getValue()

	  val distance = math.sqrt(math.pow(px - cx, 2) + math.pow(py - cy, 2))
	  this.dist.setValue(distance)

	  dataPoint.setField(3, cId)
	  dataPoint.setField(4, this.dist)

	  out.collect(dataPoint)
	}
  }

  @ConstantFields(fields = Array(1, 2))
  @OutCardBounds(lowerBound = 1, upperBound = 1)
  @ReduceContract.Combinable
  class FindNearestCenter extends ReduceStub {

    private val cId = new PactInteger()
    private val px = new PactDouble()
    private val py = new PactDouble()
    private val dist = new PactDouble()
    private val one = new PactInteger(1)
    private val result = new PactRecord(3)
    private val nearest = new PactRecord()

    override def reduce(pointsWithDistance: Iterator[PactRecord], out: Collector[PactRecord]) = {
      var nearestDistance = Double.MaxValue
      var nearestClusterId = 0	

      while (pointsWithDistance.hasNext())
      {
      	val res = pointsWithDistance.next()

      	val distance = res.getField(4, this.dist).getValue()

      	if (distance < nearestDistance) {
      		nearestDistance = distance
      		nearestClusterId = res.getField(3, this.cId).getValue()
      		res.getFieldInto(1, this.px)
      		res.getFieldInto(2, this.py)
      	}
      }
      
      this.cId.setValue(nearestClusterId)
      this.result.setField(0, this.cId)
      this.result.setField(1, this.px)
      this.result.setField(2, this.py)
      this.result.setField(3, this.one)

      out.collect(this.result)
    }

    override def combine(pointsWithDistance: Iterator[PactRecord], out: Collector[PactRecord]) = {
      var nearestDistance = Double.MaxValue

      while (pointsWithDistance.hasNext())
      {
        val res = pointsWithDistance.next()
        
        val distance = res.getField(4, this.dist).getValue()

        if (distance < nearestDistance) {
          nearestDistance = distance
          res.copyTo(this.nearest)
        }
      }

      out.collect(this.nearest)
    }
  }

  @ConstantFields(fields = Array(0))
  @OutCardBounds(lowerBound = 1, upperBound = 1)
  @ReduceContract.Combinable
  class RecomputeClusterCenter extends ReduceStub {

    private val x = new PactDouble()
    private val y = new PactDouble()
    private val count = new PactInteger()

    override def reduce(dataPoints: Iterator[PactRecord], out: Collector[PactRecord]) = {
      
      var next: PactRecord = null
      var sumX = 0d
      var sumY = 0d
      var count = 0

      while (dataPoints.hasNext())
      {
        next = dataPoints.next()

        sumX += next.getField(1, this.x).getValue()
        sumY += next.getField(2, this.y).getValue()
        count += next.getField(3, this.count).getValue()
      }
      
      this.x.setValue(math.round((sumX / count) * 100.0) / 100.0)
      this.y.setValue(math.round((sumY / count) * 100.0) / 100.0)

      next.setField(1, this.x)
      next.setField(2, this.y)
      next.setNull(3)

      out.collect(next)
    }

    override def combine(dataPoints: Iterator[PactRecord], out: Collector[PactRecord]) = {
      
      var next: PactRecord = null
      var sumX = 0d
      var sumY = 0d
      var count = 0

      while (dataPoints.hasNext())
      {
        next = dataPoints.next()

        sumX += next.getField(1, this.x).getValue()
        sumY += next.getField(2, this.y).getValue()
        count += next.getField(3, this.count).getValue()
      }

      this.x.setValue(sumX)
      this.y.setValue(sumY)
      this.count.setValue(count)
      
      next.setField(1, this.x)
      next.setField(2, this.y)
      next.setField(3, this.count)

      out.collect(next)
    }
  }
}
