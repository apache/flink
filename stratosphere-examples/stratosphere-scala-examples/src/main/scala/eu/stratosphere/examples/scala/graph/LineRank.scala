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

package eu.stratosphere.examples.scala.graph

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.DataSource
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.{ ProgramDescription, Program }
import eu.stratosphere.api.scala.operators.CsvInputFormat

object RunLineRankLocal {
  
  def main(args: Array[String]) {

    val job = new LineRank

    val plan = job.getScalaPlan(
      2,
      "file:///home/cicero/Desktop/centralities/src/main/resources/sourceIncidence.csv",
      "file:///home/cicero/Desktop/centralities/src/main/resources/targetIncidence.csv",
      9,
      "file:///tmp/centralities")

//    LocalExecutor.execute(plan)
    println(LocalExecutor.optimizerPlanAsJSON(plan));
  }
}

case class Edge(source: Int, target: Int, weight: Double)
case class VectorElement(index: Int, value: Double)

class LineRank extends Program with Serializable {

  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3).toInt, args(4))
  }

  def sumElements(elem1: VectorElement, elem2: VectorElement) = VectorElement(elem1.index, elem1.value + elem2.value)

  def sgtTimes(SGT: DataSet[Edge], vector: DataSet[VectorElement]) = {
    SGT.join(vector).where(_.source).isEqualTo(_.index)
      .map((edge, elem) => VectorElement(edge.target, edge.weight * elem.value))
      .groupBy(_.index).reduce(sumElements)
  }

  def tgTimes(TG: DataSet[Edge], vector: DataSet[VectorElement]) = {
    TG.join(vector).where(_.target).isEqualTo(_.index)
      .map((edge, elem) => VectorElement(edge.source, edge.weight * elem.value))
  }

  def rescale(v3: DataSet[VectorElement], c: Double, r: Double) = {
    v3.map(elem => { VectorElement(elem.index, c * elem.value + (1 - c) * r) })
  }

  def powerMethod(SGT: DataSet[Edge], TG: DataSet[Edge], d: DataSet[VectorElement], c: Double, r: Double)(v: DataSet[VectorElement]) = {

    val v1 = d.join(v).where(_.index).isEqualTo(_.index)
      .map((leftElem, rightElem) => VectorElement(leftElem.index, leftElem.value * rightElem.value))

    val v2 = sgtTimes(SGT, v1)
    val v3 = tgTimes(TG, v2)
    val nextV = rescale(v3, c, r)

    nextV
  }

  def getScalaPlan(numSubTasks: Int, sourceIncidenceMatrixPath: String, targetIncidenceMatrixPath: String, m: Int,
    outputPath: String) = {

    val c = .85
    val r = 1.0 / m

    val SGT = DataSource(sourceIncidenceMatrixPath, CsvInputFormat[Edge]())
    val TG = DataSource(targetIncidenceMatrixPath, CsvInputFormat[Edge]())

    val d1 = SGT.map(edge => VectorElement(edge.target, edge.weight))
      .groupBy(_.index)
      .reduce(sumElements)

    val d2 = tgTimes(TG, d1)

    val d = d2.map(elem => VectorElement(elem.index, 1 / elem.value))

    val initialV1 = d.map(elem => VectorElement(elem.index, elem.value * m))
    val initialV2 = sgtTimes(SGT, initialV1)
    val initialV3 = tgTimes(TG, initialV2)
    val initialV = rescale(initialV3, c, r)

    val v = initialV.iterate(5, powerMethod(SGT, TG, d, c, r))

    val output = v.write(outputPath, CsvOutputFormat())

    val plan = new ScalaPlan(Seq(output), "LineRank")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}