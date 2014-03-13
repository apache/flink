package eu.stratosphere.examples.scala.iterative

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.types.DoubleValue


class TerminationCriterion extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: <maxNumberIterations> <output>"
  }

  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1))
  }

  def getScalaPlan(maxNumberIterations: Int, resultOutput: String) = {
    val dataSource = CollectionDataSource[Double](List(1.0))

    val halve = (partialSolution: DataSet[Double]) => {
      partialSolution map { x => x /2 }
    }

    val terminationCriterion = (prev: DataSet[Double], cur: DataSet[Double]) => {
      val diff = prev cross cur map { (valuePrev, valueCurrent) => math.abs(valuePrev - valueCurrent) }
      diff filter {
        difference => difference > 0.1
      }
    }

    val iteration = dataSource.iterateWithTermination(maxNumberIterations, halve, terminationCriterion)


    val sink = iteration.write(resultOutput, CsvOutputFormat())

    val plan = new ScalaPlan(Seq(sink))
    plan.setDefaultParallelism(1)
    plan
  }
}

object RunTerminationCriterion {
  def main(args: Array[String]) {
    val tc = new TerminationCriterion

    if(args.size < 2) {
      println(tc.getDescription())
      return
    }
    val plan = tc.getScalaPlan(args(0).toInt, args(1))
    LocalExecutor.execute(plan)
  }
}
