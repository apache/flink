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

package org.apache.flink.ml.feature

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.math.log

class TfIdfTransformerSuite extends FlatSpec with Matchers with FlinkTestBase {


  behavior of "the tf idf transformer implementation when tf" +
    " is divided by the most frequent word from all documents"

  it should "calculate one for words in only one document" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = "This is one test for the transformer".toLowerCase.split(" ").toSeq
    val documentKey = 1

    val inputDs = env.fromCollection(Seq((documentKey, input)))
    val transformer = new TfIdfTransformer()

    val params = new ParameterMap()

    transformer.fit(inputDs, params)
    val result = transformer.transform(inputDs, params).collect()

    result.length should be(1)
    result.head._1 should be(documentKey)
    result.head._2.size should be(7)
    for ((id, vector) <- result.head._2) {
      vector should be(1.0)
    }
  }

  it should "calculate two times one for four words in only " +
    "one document with stop word parameters" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = "This is one test".toLowerCase.split(" ").toSeq
    val documentKey = 1

    val inputDs = env.fromCollection(Seq((documentKey, input)))
    val transformer = new TfIdfTransformer()

    val params = new ParameterMap()
    params.add(TfIdfTransformer.StopWordParameter, Set("test", "one"))

    transformer.fit(inputDs, params)
    val result = transformer.transform(inputDs, params).collect()

    result.length should be(1)
    result.head._1 should be(documentKey)
    result.head._2.size should be(2)

    for ((id, vector) <- result.head._2) {
      vector should be(1.0)
    }
  }

  it should "ignore single character words" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = "a b c".toLowerCase.split(" ").toSeq
    val documentKey = 1

    val inputDs = env.fromCollection(Seq((documentKey, input)))
    val transformer = new TfIdfTransformer()

    val params = new ParameterMap()

    transformer.fit(inputDs, params)
    val result = transformer.transform(inputDs, params).collect()

    result.length should be(0)
  }

  it should "calculate non zero result for tfidf over two documents" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val documentKey = 1
    val input1 = (documentKey, Seq("this"))
    val documentKey2 = 2
    val input2 = (documentKey2, "this test test test".split(" ").toSeq)
    val inputDs = env.fromCollection(Seq(input1, input2))
    val transformer = new TfIdfTransformer()

    transformer.fit(inputDs)
    val result = transformer.transform(inputDs).collect()

    result.length should be(2)
    for ((id, vector) <- result) {
      if (id.equals(documentKey)) {
        for (x <- vector) {
          x._2 should be(log(2 / 2) + 1).or(be(0.0))
        }
      }
      else if (id.equals(documentKey2)) {
        for (x <- vector) {
          x._2 should be(1 * (log(2 / 2) + 1)).or(be(3 * (log(2 / 1) + 1)))
        }
      }
      else {
        fail("Unexpected value as document id encountered! Value: " + id)
      }
    }
  }

  it should "calculate valid tfidfs for three poems" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val poem1: Seq[String] = env.readTextFile(
      this.getClass.getClassLoader.getResource("the_passionate_pilgrim.txt").toString)
      .flatMap(s => s.split(" ").toSeq).collect()
    val poem2: Seq[String] = env.readTextFile(
      this.getClass.getClassLoader.getResource("a_lovers_complaint.txt").toString)
      .flatMap(s => s.split(" ").toSeq).collect()
    val poem3: Seq[String] = env.readTextFile(
      this.getClass.getClassLoader.getResource("the_phoenix_and_the_turtle.txt").toString)
      .flatMap(s => s.split(" ").toSeq).collect()
    //example results gotten from sklearn with the TfidfVectorizer
    // options smooth_idf=False, norm=None, use_idf=True, sublinear_tf=False
    val expectedResults = env.readTextFile(
      this.getClass.getClassLoader.getResource("poems_out.txt").toString)
      .map(s => {
      val expr = "\\((.*), (.*)\\), (.*)".r
      //we cannot use the index from sklearn because they calculate it differently
      val expr(doc, _, number) = s
      (doc.toInt, number.toDouble)
    }).collect()
    val transformer = new TfIdfTransformer()
    transformer.fit(env.fromCollection(Seq((0, poem1), (1, poem2), (2, poem3))))
    val result = transformer.transform(env.fromCollection(Seq((0, poem1), (1, poem2), (2, poem3))))
      .collect()
    for ((id, vector) <- result) {
      //sort both lists and get rid of the zeros so we can compare both
      val expectedResultsForDocument = expectedResults.filter(t => t._1 == id).map(t => t._2).sorted
      val mappedResult = vector.map(t => t._2).filter(x => x > 0.0).toList.sorted
      for (x <- 0 until expectedResultsForDocument.size) {
        expectedResultsForDocument(x) should be(mappedResult(x) +- 0.0000001)
      }
    }
  }
}
