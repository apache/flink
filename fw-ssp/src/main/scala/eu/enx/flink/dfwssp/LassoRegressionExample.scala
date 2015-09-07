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

package eu.enx.flink.dfwssp

import breeze.linalg.VectorBuilder
import com.typesafe.config.ConfigFactory
import eu.enx.flink.dfwssp.model.{LassoWithPS, ColumnVector}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE
import org.apache.flink.parameterserver.impl.ignite.ParameterServerIgniteImpl
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * Frank-Wolfe Lasso regression on the Dorothea dataset.
 * Parameters of the job are defined in src/resources/job.conf
 * For use with SSP and {@link org.apache.flink.parameterserver.impl.ignite}
 * parameter server implementation.
 */

object LassoRegressionExample {

  def main(args: Array[String]): Unit = {
    val jobConf = ConfigFactory.load("job.conf")

    def getPropInt(
      prop: String): Int = Try(jobConf.getInt(prop)).getOrElse(jobConf.getInt(prop))
    def getPropString(
      prop: String): String = Try(jobConf.getString(prop)).getOrElse(jobConf.getString(prop))
    def getPropDouble(
      prop: String): Double = Try(jobConf.getDouble(prop)).getOrElse(jobConf.getDouble(prop))

    val EPSILON = getPropDouble("epsilon")
    val PARALLELISM = getPropInt("cluster.nodes")
    val NUMITER = getPropInt("max.iterations")
    val LINESEARCH = true
    val OPT = "CD"
    val SLACK = getPropInt("slack")
    val BETA = getPropDouble("beta")
    val TARGET_PATH = jobConf.getString("target.path")
    val DATA_PATH = jobConf.getString("data.path")

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(PARALLELISM)

    val model = new LassoWithPS(
      beta = BETA,
      numIter = NUMITER,
      line_search = LINESEARCH,
      epsilon = EPSILON,
      opt = OPT,
      slack = SLACK)

    val y = env.readTextFile(TARGET_PATH).setParallelism(1).map(x => x.toDouble)
    val Y = y.reduceGroup(iterator => iterator.toArray)
    val dimension = y.count.toInt
    val rows = loadSparseMatrix(env, DATA_PATH, 10000, 0, ignoreFirstLine = true)
    val cols = loadSparseMatrix(env, DATA_PATH, dimension, 1, ignoreFirstLine = true)
    model.fit(cols, Y, log = true)
    model.predict(rows).print()
    ParameterServerIgniteImpl.shutDown();
  }

  /**
   * Load data from a file where each line is of the form id, col_i:val_i col_j:val_j ...
   * representing a matrix. Return a set of vectors that represents rows (if axis == 0) or
   * columns (axis == 1) of the matrix.
   * @param env
   * @param filename
   * @param dimension
   * @param axis
   * @param ignoreFirstLine
   * @return
   */
  def loadSparseMatrix(
    env: ExecutionEnvironment, filename: String, dimension: Int, axis: Int = 1,
    ignoreFirstLine: Boolean = true): DataSet[ColumnVector] = {

    assert(axis == 1 || axis == 0)

    val csv = env.readCsvFile[(Int, String)](filename, ignoreFirstLine = true)
    val bagOfEntry = csv.flatMap {
      tuple => {
        val id = tuple._1
        val nonZeroCols = tuple._2.split(" ").map { x => x.split(":") }.map(a => (id, a(0).toInt, a
          (1).toDouble))
        nonZeroCols
      }
    }.groupBy(axis)

    val out = bagOfEntry.reduceGroup {
      iterator => {
        val s = iterator.toSeq
        val idx = if (axis == 1) s(0)._2 else s(0)._1
        val v = new VectorBuilder[Double](dimension)
        for ((row, col, value) <- s) {
          if (axis == 1)
            v.add(row, value)
          else
            v.add(col, value)
        }
        ColumnVector(idx, v.toDenseVector.toArray)
      }
    }
    out
  }
}
