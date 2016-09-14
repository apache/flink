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

package org.apache.flink.ml.nlp

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.nlp.Word2VecData._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class Word2VecITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The Word2Vec implementation"

  it should "form an initial vector set given a training corpus" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val w2v = Word2Vec()
      .setVectorSize(10)
      .setBatchSize(10)

    val text = env.fromCollection(sentence.split(" ").grouped(10).map(_.toList).toList)

    w2v.fit(text)

    val fitted = w2v.wordVectors

    fitted should not be empty

    val localFitted = fitted.get.collect()

    localFitted.size should be (1)

    val localFittedHead = localFitted.head

    localFittedHead.leafMap.size should be (sentence.split(" ").distinct.length)
    localFittedHead.innerMap.size should be (localFittedHead.leafMap.size - 1)
    localFittedHead.leafVectors.size should be (localFittedHead.leafMap.size)
    localFittedHead.innerVectors.size should be (localFittedHead.innerMap.size)

    localFittedHead.leafVectors.head._2.length should be (10)

  }

  it should "learn vector representations that reflect context" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val w2v = Word2Vec()
      .setVectorSize(10)
      .setWindowSize(2)
      .setBatchSize(10)
      .setTargetCount(1)
      .setIterations(2)

    val capitalCities = capitals.map(_.split(" ").head)

    val text = env.fromCollection(capitals).map(_.split(" ").toList)

    w2v.fit(text)

    val vectors = w2v.transform(text).collect()
      .map(x => x._1 -> DenseVector(x._2.toArray))

    //extract the learned vector for Rome
    val rome = vectors.filter(_._1 == "Rome").head

    //calculate similarities
    val romeSimilarity =
      vectors
        .filter(_._1 != "Rome")
        .map(x => x._1 -> (x._2.dot(rome._2) / (x._2.magnitude * rome._2.magnitude)))
        .sortBy(_._2).reverse

    capitalCities should contain (romeSimilarity.head._1)
  }

  it should "learn vector representations that reflect analogy" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val w2v = Word2Vec()
      .setVectorSize(100)
      .setWindowSize(6)
      .setBatchSize(10)
      .setTargetCount(1)
      .setIterations(50)

    val unique = capitals.flatMap(_.split(" ")).distinct

    val text = env.fromCollection(capitals).map(_.split(" ").toList)

    w2v.fit(text)

    val vectors = w2v.transform(text).collect()
      .map(x => x._1 -> DenseVector(x._2.toArray))

    //extract the learned vector for Rome
    val rome = vectors.filter(_._1 == "Rome").head._2
    //extract the learned vector for Italy
    val italy = vectors.filter(_._1 == "Italy").head._2
    //calculate difference vector Rome - Italy
    val difference = rome.zip(italy).map(x => x._1._1 -> (x._1._2 - x._2._2))

    //extract the learned vector for Russia
    val russia = vectors.filter(_._1 == "Russia").head._2
    //calculate summation of Moscow + (Rome - Italy)
    val test_moscow = DenseVector(russia.zip(difference).map(x => x._1._2 + x._2._2).toArray)

    //calculate analogy: Rome is to Italy as Moscow is to ???
    val moscowAnalogy =
      vectors
        .map(x => x._1 -> (x._2.dot(test_moscow) / (x._2.magnitude * test_moscow.magnitude)))
        .sortBy(_._2).reverse

    moscowAnalogy.map(_._1).slice(0, unique.size / 4) should contain ("Moscow")
  }
}
