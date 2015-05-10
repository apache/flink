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
 * This test is used to compare whether the different versions of [[MultinomialNaiveBayes]]
 * resulting of the chooseable "possibilities" give the same result, what they should.
 */
class MultinomialNaiveBayesValidation extends FlatSpec with Matchers with FlinkTestBase {

  val outputFolder = "/Users/jonathanhasenburg/Desktop/TestOutput/"


  behavior of "The MultinomialNaiveBayes implementation"


  it should "train the classifier with the basic configuration" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes()

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "basicConfigModelWord.csv", outputFolder +
      "basicConfigModelClass.csv")

    env.execute()
  }

  it should "use the basicConfigModel model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nnb = MultinomialNaiveBayes()
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double)](outputFolder +
      "/basicConfigModelWord.csv", "\n", "|"),
      env.readCsvFile[(String, Double, Double)](outputFolder +
        "/basicConfigModelClass.csv", "\n", "|"))

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
    val nnb = MultinomialNaiveBayes().setP1(0)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p1_0ModelWord.csv", outputFolder + "p1_0ModelClass.csv")

    env.execute()
  }

  it should "use the p1 = 0 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayes().setP2(0)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double)](outputFolder +
      "/p1_0ModelWord.csv", "\n", "|"),
      env.readCsvFile[(String, Double, Double)](outputFolder + "/p1_0ModelClass.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p1_0Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "train the classifier with p1 = 1" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes().setP1(1)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p1_1ModelWord.csv", outputFolder + "p1_1ModelClass.csv")

    env.execute()
  }

  it should "use the p1 = 1 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayes().setP2(1)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double)](outputFolder +
      "/p1_1ModelWord.csv", "\n", "|"),
      env.readCsvFile[(String, Double, Double)](outputFolder + "/p1_1ModelClass.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p1_1Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }


  it should "compare p1 = 0, p1 = 1 and the basic solutions" in {
    // Compare Model Word
    val basicMW = scala.io.Source.fromFile(outputFolder + "basicConfigModelWord.csv").getLines()
      .toArray
    Sorting.quickSort(basicMW)

    val p1_0MW = scala.io.Source.fromFile(outputFolder + "p1_0ModelWord.csv").getLines().toArray
    Sorting.quickSort(p1_0MW)

    val p1_1MW = scala.io.Source.fromFile(outputFolder + "p1_1ModelWord.csv").getLines().toArray
    Sorting.quickSort(p1_1MW)

    assert(basicMW.deep == p1_0MW.deep)
    assert(p1_0MW.deep == p1_1MW.deep)

    // Comapre Model Class
    val basicMC = scala.io.Source.fromFile(outputFolder + "basicConfigModelClass.csv").getLines()
      .toArray
    Sorting.quickSort(basicMC)

    val p1_0MC = scala.io.Source.fromFile(outputFolder + "p1_0ModelClass.csv").getLines().toArray
    Sorting.quickSort(p1_0MC)

    val p1_1MC = scala.io.Source.fromFile(outputFolder + "p1_1ModelClass.csv").getLines().toArray
    Sorting.quickSort(p1_1MC)

    assert(basicMC.deep == p1_0MC.deep)
    assert(p1_0MC.deep == p1_1MC.deep)

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
    val nnb = MultinomialNaiveBayes().setP2(0)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p2_0ModelWord.csv", outputFolder + "p2_0ModelClass.csv")

    env.execute()
  }

  it should "use the p2 = 0 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayes().setP2(0)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double)](outputFolder +
      "/p2_0ModelWord.csv", "\n", "|"),
      env.readCsvFile[(String, Double, Double)](outputFolder + "/p2_0ModelClass.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p2_0Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "train the classifier with p2 = 1" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes().setP2(1)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p2_1ModelWord.csv", outputFolder + "p2_1ModelClass.csv")

    env.execute()
  }

  it should "use the p2 = 1 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayes().setP2(1)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double)](outputFolder +
      "/p2_1ModelWord.csv", "\n", "|"),
      env.readCsvFile[(String, Double, Double)](outputFolder + "/p2_1ModelClass.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p2_1Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "compare p2 = 0, p2 = 1 and the basic solutions" in {
    // Compare Model Word
    val basicMW = scala.io.Source.fromFile(outputFolder + "basicConfigModelWord.csv").getLines()
      .toArray
    Sorting.quickSort(basicMW)

    val p2_0MW = scala.io.Source.fromFile(outputFolder + "p2_0ModelWord.csv").getLines().toArray
    Sorting.quickSort(p2_0MW)

    val p2_1MW = scala.io.Source.fromFile(outputFolder + "p2_1ModelWord.csv").getLines().toArray
    Sorting.quickSort(p2_1MW)

    assert(basicMW.deep == p2_0MW.deep)
    assert(p2_0MW.deep == p2_1MW.deep)

    // Comapre Model Class
    val basicMC = scala.io.Source.fromFile(outputFolder + "basicConfigModelClass.csv").getLines()
      .toArray
    Sorting.quickSort(basicMC)

    val p2_0MC = scala.io.Source.fromFile(outputFolder + "p2_0ModelClass.csv").getLines().toArray
    Sorting.quickSort(p2_0MC)

    val p2_1MC = scala.io.Source.fromFile(outputFolder + "p2_1ModelClass.csv").getLines().toArray
    Sorting.quickSort(p2_1MC)

    assert(basicMC.deep == p2_0MC.deep)
    assert(p2_0MC.deep == p2_1MC.deep)

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
    val nnb = MultinomialNaiveBayes().setP3(0)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p3_0ModelWord.csv", outputFolder + "p3_0ModelClass.csv")

    env.execute()
  }

  it should "use the p3 = 0 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayes().setP3(0)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double)](outputFolder +
      "/p3_0ModelWord.csv", "\n", "|"),
      env.readCsvFile[(String, Double, Double)](outputFolder + "/p3_0ModelClass.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p3_0Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "train the classifier with p3 = 1" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes().setP3(1)

    val trainingDS = env.fromCollection(Classification.bbcTrainData)
    nnb.fit(trainingDS)

    nnb.saveModelDataSet(outputFolder + "p3_1ModelWord.csv", outputFolder + "p3_1ModelClass.csv")

    env.execute()
  }

  it should "use the p3 = 1 model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val nnb = MultinomialNaiveBayes().setP3(1)
    nnb.setModelDataSet(env.readCsvFile[(String, String, Double)](outputFolder +
      "/p3_1ModelWord.csv", "\n", "|"),
      env.readCsvFile[(String, Double, Double)](outputFolder + "/p3_1ModelClass.csv", "\n", "|"))

    val solution = nnb.predict(env.fromCollection(Classification.bbcTestData))
    solution.writeAsCsv(outputFolder + "p3_1Solution.csv", "\n", "\t", WriteMode.OVERWRITE)


    env.execute()

  }

  it should "compare p3 = 0, p3 = 1 and the basic solutions" in {
    // Compare Model Word
    val basicMW = scala.io.Source.fromFile(outputFolder + "basicConfigModelWord.csv").getLines()
      .toArray
    Sorting.quickSort(basicMW)

    val p3_0MW = scala.io.Source.fromFile(outputFolder + "p3_0ModelWord.csv").getLines().toArray
    Sorting.quickSort(p3_0MW)

    val p3_1MW = scala.io.Source.fromFile(outputFolder + "p3_1ModelWord.csv").getLines().toArray
    Sorting.quickSort(p3_1MW)

    assert(basicMW.deep == p3_0MW.deep)
    assert(p3_0MW.deep == p3_1MW.deep)

    // Comapre Model Class
    val basicMC = scala.io.Source.fromFile(outputFolder + "basicConfigModelClass.csv").getLines()
      .toArray
    Sorting.quickSort(basicMC)

    val p3_0MC = scala.io.Source.fromFile(outputFolder + "p3_0ModelClass.csv").getLines().toArray
    Sorting.quickSort(p3_0MC)

    val p3_1MC = scala.io.Source.fromFile(outputFolder + "p3_1ModelClass.csv").getLines().toArray
    Sorting.quickSort(p3_1MC)

    assert(basicMC.deep == p3_0MC.deep)
    assert(p3_0MC.deep == p3_1MC.deep)

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
