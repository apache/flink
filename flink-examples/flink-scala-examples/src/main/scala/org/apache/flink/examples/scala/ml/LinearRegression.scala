/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.scala.ml

import java.io.Serializable

import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.example.java.clustering.util.KMeansData
import org.apache.flink.example.java.ml.util.LinearRegressionData

import scala.collection.JavaConverters._

/**
 * This example implements a basic Linear Regression  to solve the y = theta0 + theta1*x problem using batch gradient descent algorithm.
 *
 * <p>
 * Linear Regression with BGD(batch gradient descent) algorithm is an iterative clustering algorithm and works as follows:<br>
 * Giving a data set and target set, the BGD try to find out the best parameters for the data set to fit the target set.
 * In each iteration, the algorithm computes the gradient of the cost function and use it to update all the parameters.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * With enough iteration, the algorithm can minimize the cost function and find the best parameters
 * This is the Wikipedia entry for the <a href = "http://en.wikipedia.org/wiki/Linear_regression">Linear regression</a> and <a href = "http://en.wikipedia.org/wiki/Gradient_descent">Gradient descent algorithm</a>.
 * 
 * <p>
 * This implementation works on one-dimensional data. And find the two-dimensional theta.<br>
 * It find the best Theta parameter to fit the target.
 * 
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character. The first one represent the X(the training data) and the second represent the Y(target).
 * Data points are separated by newline characters.<br>
 * For example <code>"-0.02 -0.04\n5.3 10.6\n"</code> gives two data points (x=-0.02, y=-0.04) and (x=5.3, y=10.6).
 * </ul>
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li> Bulk iterations
 * <li> Broadcast variables in bulk iterations
 * <li> Custom Java objects (PoJos)
 * </ul>
 */
object LinearRegression {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	def main(args: Array[String]) {
		if (!parseParameters(args)) {
			return
		}

		val env = ExecutionEnvironment.getExecutionEnvironment
		val data: DataSet[Data] = getDataSet(env)
		val parameters: DataSet[Params] = getParamsDataSet(env)
		val result = parameters.iterate(numIterations) { currentParameters =>
			val newParameters = data
				.map(new SubUpdate).withBroadcastSet(currentParameters, "parameters")
				.reduce { (val1, val2) =>
				val new_theta0: Double = val1._1.getTheta0 + val2._1.getTheta0
				val new_theta1: Double = val1._1.getTheta1 + val2._1.getTheta1
				val result: Params = new Params(new_theta0, new_theta1)
				(result, val1._2 + val2._2)
			}
				.map { x => x._1.div(x._2) }
			newParameters
		}

		if (fileOutput) {
			result.writeAsText(outputPath)
		}
		else {
			result.print
		}
		env.execute("Scala Linear Regression example")
	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************
	/**
	 * A simple data sample, x means the input, and y means the target.
	 */
	class Data extends Serializable {

		def this(x: Double, y: Double) {
			this()
			this.x = x
			this.y = y
		}

		override def toString: String = {
			"(" + x + "|" + y + ")"
		}

		var x: Double = .0
		var y: Double = .0
	}

	/**
	 * A set of parameters -- theta0, theta1.
	 */
	class Params extends Serializable {

		def this(x0: Double, x1: Double) {
			this()
			this.theta0 = x0
			this.theta1 = x1
		}

		override def toString: String = {
			theta0 + " " + theta1
		}

		def getTheta0: Double = {
			theta0
		}

		def getTheta1: Double = {
			theta1
		}

		def setTheta0(theta0: Double) {
			this.theta0 = theta0
		}

		def setTheta1(theta1: Double) {
			this.theta1 = theta1
		}

		def div(a: Integer): Params = {
			this.theta0 = theta0 / a
			this.theta1 = theta1 / a
			return this
		}

		private var theta0: Double = .0
		private var theta1: Double = .0
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Compute a single BGD type update for every parameters.
	 */
	class SubUpdate extends RichMapFunction[Data, Tuple2[Params, Integer]] {

		private var parameters: Traversable[Params] = null
		var parameter: Params = null
		private var count: Int = 1

		/** Reads the parameters from a broadcast variable into a collection. */
		override def open(parameters: Configuration) {
			this.parameters = getRuntimeContext.getBroadcastVariable[Params]("parameters").asScala
		}

		def map(in: Data): Tuple2[Params, Integer] = {
			for (p <- parameters) {
				this.parameter = p
			}
			val theta_0: Double = parameter.getTheta0 - 0.01 * ((parameter.getTheta0 + (parameter.getTheta1 * in.x)) - in.y)
			val theta_1: Double = parameter.getTheta1 - 0.01 * (((parameter.getTheta0 + (parameter.getTheta1 * in.x)) - in.y) * in.x)
			new Tuple2[Params, Integer](new Params(theta_0, theta_1), count)
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
				numIterations = Integer.parseInt(programArguments(2))
			}
			else {
				System.err.println("Usage: LinearRegression <data path> <result path> <num iterations>")
				false
			}
		}
		else {
			System.out.println("Executing Linear Regression example with default parameters and built-in default data.")
			System.out.println("  Provide parameters to read input data from files.")
			System.out.println("  See the documentation for the correct format of input files.")
			System.out.println("  We provide a data generator to create synthetic input files for this program.")
			System.out.println("  Usage: LinearRegression <data path> <result path> <num iterations>")
		}
		true
	}

	private def getDataSet(env: ExecutionEnvironment): DataSet[Data] = {
		if (fileOutput) {
			env.readCsvFile[(Double, Double)](
				dataPath,
				fieldDelimiter = ' ',
				includedFields = Array(0, 1))
				.map { t => new Data(t._1, t._2) }
		}
		else {
			val data = LinearRegressionData.DATA map {
				case Array(x, y) => new Data(x.asInstanceOf[Double], y.asInstanceOf[Double])
			}
			env.fromCollection(data)
		}
	}

	private def getParamsDataSet(env: ExecutionEnvironment): DataSet[Params] = {
		val params = LinearRegressionData.PARAMS map {
			case Array(x, y) => new Params(x.asInstanceOf[Double], y.asInstanceOf[Double])
		}
		env.fromCollection(params)
	}
}
