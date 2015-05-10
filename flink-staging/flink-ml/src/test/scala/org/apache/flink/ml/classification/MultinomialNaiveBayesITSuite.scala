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

package org.apache.flink.ml.classification

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class MultinomialNaiveBayesITSuite extends FlatSpec with Matchers with FlinkTestBase {

  val saveLocationModel = "/Users/jonathanhasenburg/Desktop/nbtemp/"

  behavior of "The MultinomialNaiveBayes implementation"
/*
  it should "use the CRQSelection transformer" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val crq = new CRQSelection()

    val trainingDS = env.readCsvFile[(String, Int, String)]("/Users/jonathanhasenburg/" +
      "OneDrive/datasets/mostTrivialCRQ/input/train.csv", "\n", "\t", lenient=true)
    crq.fit(trainingDS);

    env.execute()
  } */


  it should "train a NaiveBayesClassifier" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes()

    val trainingDS = env.readCsvFile[(String, String)]("/Users/jonathanhasenburg/" +
      "OneDrive/datasets/webkb/input/train.csv", "\n", "\t", lenient=true)
    nnb.fit(trainingDS);

    nnb.saveModelDataSet(saveLocationModel + "wordRelated", saveLocationModel + "classRelated")
    //nnb.saveImprovementDataSet(saveLocationModel + "improvementData")

    env.execute()
  }

  it should "use the trained model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nnb = MultinomialNaiveBayes()
    nnb.setModelDataSet(env
      .readCsvFile[(String, String, Double)](saveLocationModel + "wordRelated", "\n", "|"), env
      .readCsvFile[(String, Double, Double)](saveLocationModel + "classRelated", "\n", "|"))

    //nnb.setImprovementDataSet(
      //env.readCsvFile[(String, Double)](saveLocationModel + "improvementData", "\n", "|"))

    val solution = nnb.predict(env.readCsvFile[(Int, String)]("/Users/jonathanhasenburg/" +
      "OneDrive/datasets/webkb/input/test.csv", "\n", "\t", lenient = true))

    solution.writeAsCsv("/Users/jonathanhasenburg/" +
      "OneDrive/datasets/webkb/run/runtmp/solution.csv",
      "\n", "\t", WriteMode.OVERWRITE)

    env.execute()

  }

}
