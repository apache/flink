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

package org.apache.flink.examples.scala.ml

import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.examples.java.ml.util.LinearRegressionData

import scala.collection.JavaConverters._

/**
 * This example implements a basic Linear Regression  to solve the y = theta0 + theta1*x problem
 * using batch gradient descent algorithm.
 *
 * Linear Regression with BGD(batch gradient descent) algorithm is an iterative algorithm and
 * works as follows:
 *
 * Giving a data set and target set, the BGD try to find out the best parameters for the data set
 * to fit the target set.
 * In each iteration, the algorithm computes the gradient of the cost function and use it to
 * update all the parameters.
 * The algorithm terminates after a fixed number of iterations (as in this implementation).
 * With enough iteration, the algorithm can minimize the cost function and find the best parameters
 * This is the Wikipedia entry for the
 * [[http://en.wikipedia.org/wiki/Linear_regression Linear regression]] and
 * [[http://en.wikipedia.org/wiki/Gradient_descent Gradient descent algorithm]].
 *
 * This implementation works on one-dimensional data and finds the best two-dimensional theta to
 * fit the target.
 *
 * Input files are plain text files and must be formatted as follows:
 *
 *  - Data points are represented as two double values separated by a blank character. The first
 *    one represent the X(the training data) and the second represent the Y(target). Data points are
 *    separated by newline characters.
 *    For example `"-0.02 -0.04\n5.3 10.6\n"`gives two data points
 *    (x=-0.02, y=-0.04) and (x=5.3, y=10.6).
 *
 * This example shows how to use:
 *
 *  - Bulk iterations
 *  - Broadcast variables in bulk iterations
 */
object LinearRegression {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = getDataSet(env)
    val parameters = getParamsDataSet(env)

    val result = parameters.iterate(numIterations) { currentParameters =>
      val newParameters = data
        .map(new SubUpdate).withBroadcastSet(currentParameters, "parameters")
        .reduce { (p1, p2) =>
          val result = p1._1 + p2._1
          (result, p1._2 + p2._2)
        }
        .map { x => x._1.div(x._2) }
      newParameters
    }

    if (fileOutput) {
      result.writeAsText(outputPath)
      env.execute("Scala Linear Regression example")
    }
    else {
      result.print()
    }
  }

  /**
   * A simple data sample, x means the input, and y means the target.
   */
  case class Data(var x: Double, var y: Double)

  /**
   * A set of parameters -- theta0, theta1.
   */
  case class Params(theta0: Double, theta1: Double) {
    def div(a: Int): Params = {
      Params(theta0 / a, theta1 / a)
    }

    def + (other: Params) = {
      Params(theta0 + other.theta0, theta1 + other.theta1)
    }
  }

  // *************************************************************************
  //     USER FUNCTIONS
  // *************************************************************************

  /**
   * Compute a single BGD type update for every parameters.
   */
  class SubUpdate extends RichMapFunction[Data, (Params, Int)] {

    private var parameter: Params = null

    /** Reads the parameters from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      val parameters = getRuntimeContext.getBroadcastVariable[Params]("parameters").asScala
      parameter = parameters.head
    }

    def map(in: Data): (Params, Int) = {
      val theta0 =
        parameter.theta0 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in.x)) - in.y)
      val theta1 =
        parameter.theta1 - 0.01 * (((parameter.theta0 + (parameter.theta1 * in.x)) - in.y) * in.x)
      (Params(theta0, theta1), 1)
    }
  }

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************
  private var fileOutput: Boolean = false
  private var dataPath: String = null
  private var outputPath: String = null
  private var numIterations: Int = 10

  private def parseParameters(programArguments: Array[String]): Boolean = {
    if (programArguments.length > 0) {
      fileOutput = true
      if (programArguments.length == 3) {
        dataPath = programArguments(0)
        outputPath = programArguments(1)
        numIterations = programArguments(2).toInt

        true
      }
      else {
        System.err.println("Usage: LinearRegression <data path> <result path> <num iterations>")

        false
      }
    }
    else {
      System.out.println("Executing Linear Regression example with default parameters and " +
        "built-in default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  We provide a data generator to create synthetic input files for this " +
        "program.")
      System.out.println("  Usage: LinearRegression <data path> <result path> <num iterations>")

      true
    }
  }

  private def getDataSet(env: ExecutionEnvironment): DataSet[Data] = {
    if (fileOutput) {
      env.readCsvFile[(Double, Double)](
        dataPath,
        fieldDelimiter = " ",
        includedFields = Array(0, 1))
        .map { t => new Data(t._1, t._2) }
    }
    else {
      val data = LinearRegressionData.DATA map {
        case Array(x, y) => Data(x.asInstanceOf[Double], y.asInstanceOf[Double])
      }
      env.fromCollection(data)
    }
  }

  private def getParamsDataSet(env: ExecutionEnvironment): DataSet[Params] = {
    val params = LinearRegressionData.PARAMS map {
      case Array(x, y) => Params(x.asInstanceOf[Double], y.asInstanceOf[Double])
    }
    env.fromCollection(params)
  }
}
