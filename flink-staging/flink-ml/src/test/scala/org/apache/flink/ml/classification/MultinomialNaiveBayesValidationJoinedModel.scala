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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Sorting

/**
 * This test is used to compare whether the different versions of
 * [[MultinomialNaiveBayesJoinedModel]] resulting of the chooseable "possibilities" give the
 * same result, what they should.
 */
class MultinomialNaiveBayesValidationJoinedModel extends FlatSpec with Matchers with FlinkTestBase {

  val outputFolder = "/Users/jonathanhasenburg/Desktop/TestOutput/"


  behavior of "The MultinomialNaiveBayesJoinedModel implementation"


  it should "train the classifier with the basic configuration" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayesJoinedModel()

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "basicConfigModel.csv")

    env.execute()
  }

  it should "use the basicConfigModel model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nnb = MultinomialNaiveBayesJoinedModel()
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double, Double, Double)](outputFolder +
      "/basicConfigModel.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "basicConfigSolution.csv", "\n", "\t", WriteMode.OVERWRITE)

    env.execute()

  }

  it should "validate, that basicConfig predicted everything but 357 and 358 correctly" in {
    val basicA = scala.io.Source.fromFile(outputFolder + "basicConfigSolution.csv").getLines()
      .toArray
    for (line <- basicA) {
      val split = line.split("\t")
      if (split(0).toInt <= 10) {
        assert(split(1).equals("business"))
      } else if (split(0).toInt <= 356 || split(0).toInt >= 359) {
        assert(split(1).equals("entertainment"))
      } else if (split(0).toInt == 357 || split(0).toInt == 358) {
        assert(split(1).equals("business")) //wrong predicted, but we want this
      } else {
        fail("unknown identifier number in basicConfigSolution.csv" + split(0))
      }
    }

  }

  it should "train the classifier with p1 = 0" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayesJoinedModel().setP1(0)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p1_0Model.csv")

    env.execute()
  }

  it should "use the p1 = 0 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayesJoinedModel().setP2(0)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double, Double, Double)](outputFolder +
      "/p1_0Model.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p1_0Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "train the classifier with p1 = 1" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayesJoinedModel().setP1(1)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p1_1Model.csv")

    env.execute()
  }

  it should "use the p1 = 1 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayesJoinedModel().setP2(1)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double, Double, Double)](outputFolder +
      "/p1_1Model.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p1_1Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }


  it should "compare p1 = 0, p1 = 1 and the basic solutions" in {
    // Compare Models
    val basicM = scala.io.Source.fromFile(outputFolder + "basicConfigModel.csv").getLines().toArray
    Sorting.quickSort(basicM)

    val p1_0M = scala.io.Source.fromFile(outputFolder + "p1_0Model.csv").getLines().toArray
    Sorting.quickSort(p1_0M)

    val p1_1M = scala.io.Source.fromFile(outputFolder + "p1_1Model.csv").getLines().toArray
    Sorting.quickSort(p1_1M)

    assert(basicM.deep == p1_0M.deep)
    assert(p1_0M.deep == p1_1M.deep)

    // Compare Solutions
    val basicS = scala.io.Source.fromFile(outputFolder + "basicConfigSolution.csv").getLines()
      .toArray
    Sorting.quickSort(basicS)

    val p1_0S = scala.io.Source.fromFile(outputFolder + "p1_0Solution.csv").getLines().toArray
    Sorting.quickSort(p1_0S)

    val p1_1S = scala.io.Source.fromFile(outputFolder + "p1_1Solution.csv").getLines().toArray
    Sorting.quickSort(p1_1S)

    assert(basicS.deep == p1_0S.deep)
    assert(p1_0S.deep == p1_1S.deep)
  }

  it should "train the classifier with p2 = 0" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayesJoinedModel().setP2(0)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p2_0Model.csv")

    env.execute()
  }

  it should "use the p2 = 0 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayesJoinedModel().setP2(0)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double, Double, Double)](outputFolder +
      "/p2_0Model.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p2_0Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "train the classifier with p2 = 1" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayesJoinedModel().setP2(1)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p2_1Model.csv")

    env.execute()
  }

  it should "use the p2 = 1 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayesJoinedModel().setP2(1)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double, Double, Double)](outputFolder +
      "/p2_1Model.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p2_1Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "compare p2 = 0, p2 = 1 and the basic solutions" in {
    // Compare Models
    val basicM = scala.io.Source.fromFile(outputFolder + "basicConfigModel.csv").getLines().toArray
    Sorting.quickSort(basicM)

    val p2_0M = scala.io.Source.fromFile(outputFolder + "p2_0Model.csv").getLines().toArray
    Sorting.quickSort(p2_0M)

    val p2_1M = scala.io.Source.fromFile(outputFolder + "p2_1Model.csv").getLines().toArray
    Sorting.quickSort(p2_1M)

    assert(basicM.deep == p2_0M.deep)
    assert(p2_0M.deep == p2_1M.deep)

    // Compare Solutions
    val basicS = scala.io.Source.fromFile(outputFolder + "basicConfigSolution.csv").getLines()
      .toArray
    Sorting.quickSort(basicS)

    val p2_0S = scala.io.Source.fromFile(outputFolder + "p2_0Solution.csv").getLines().toArray
    Sorting.quickSort(p2_0S)

    val p2_1S = scala.io.Source.fromFile(outputFolder + "p2_1Solution.csv").getLines().toArray
    Sorting.quickSort(p2_1S)

    assert(basicS.deep == p2_0S.deep)
    assert(p2_0S.deep == p2_1S.deep)
  }

  it should "train the classifier with p3 = 0" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayesJoinedModel().setP3(0)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p3_0Model.csv")

    env.execute()
  }

  it should "use the p3 = 0 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayesJoinedModel().setP3(0)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double, Double, Double)](outputFolder +
      "/p3_0Model.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p3_0Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "train the classifier with p3 = 1" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayesJoinedModel().setP3(1)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p3_1Model.csv")

    env.execute()
  }

  it should "use the p3 = 1 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayesJoinedModel().setP3(1)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double, Double, Double)](outputFolder +
      "/p3_1Model.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p3_1Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "compare p3 = 0, p3 = 1 and the basic solutions" in {
    // Compare Models
    val basicM = scala.io.Source.fromFile(outputFolder + "basicConfigModel.csv").getLines().toArray
    Sorting.quickSort(basicM)

    val p3_0M = scala.io.Source.fromFile(outputFolder + "p3_0Model.csv").getLines().toArray
    Sorting.quickSort(p3_0M)

    val p3_1M = scala.io.Source.fromFile(outputFolder + "p3_1Model.csv").getLines().toArray
    Sorting.quickSort(p3_1M)

    assert(basicM.deep == p3_0M.deep)
    assert(p3_0M.deep == p3_1M.deep)

    // Compare Solutions
    val basicS = scala.io.Source.fromFile(outputFolder + "basicConfigSolution.csv").getLines()
      .toArray
    Sorting.quickSort(basicS)

    val p3_0S = scala.io.Source.fromFile(outputFolder + "p3_0Solution.csv").getLines().toArray
    Sorting.quickSort(p3_0S)

    val p3_1S = scala.io.Source.fromFile(outputFolder + "p3_1Solution.csv").getLines().toArray
    Sorting.quickSort(p3_1S)

    assert(basicS.deep == p3_0S.deep)
    assert(p3_0S.deep == p3_1S.deep)
  }


  it should "delete the tmp folder" in {
    FileUtils.deleteDirectory(new File(outputFolder));
  }

}
