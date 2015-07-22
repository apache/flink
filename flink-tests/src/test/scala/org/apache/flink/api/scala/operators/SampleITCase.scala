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
package org.apache.flink.api.scala.operators

import java.util.{List => JavaList}

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Test}

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class SampleITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  private var result: JavaList[String] = null;

  @After
  def after() = {
    TestBaseUtils.containsResultAsText(result, getSourceStrings)
  }

  @Test
  def testSamplerWithFractionWithoutReplacement {
    val sampled = getSourceDataSet().sample(false, 0.2d, 1)
    result = sampled.collect().asJava
  }

  @Test
  def testSamplerWithFractionWithReplacement: Unit = {
    val sampled = getSourceDataSet().sample(true, 0.2d, 1)
    result = sampled.collect().asJava
  }

  @Test
  def testSamplerWithSizeWithoutReplacement {
    val numSamples = 2;
    val sampled = getSourceDataSet().sampleWithSize(false, numSamples, 1)
    result = sampled.collect().asJava
    assertTrue(result.size == numSamples)
  }

  @Test
  def testSamplerWithSizeWithReplacement {
    val numSamples = 2;
    val sampled = getSourceDataSet().sampleWithSize(true, numSamples, 1)
    result = sampled.collect().asJava
    assertTrue(result.size == numSamples)
  }

  private def getSourceDataSet(): DataSet[String] = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDataSet = CollectionDataSets.get3TupleDataSet(env)
    tupleDataSet.map(x => x._3)
  }

  private def getSourceStrings: String = {
    return "Hi\n" +
      "Hello\n" +
      "Hello world\n" +
      "Hello world, how are you?\n" +
      "I am fine.\n" +
      "Luke Skywalker\n" +
      "Comment#1\n" +
      "Comment#2\n" +
      "Comment#3\n" +
      "Comment#4\n" +
      "Comment#5\n" +
      "Comment#6\n" +
      "Comment#7\n" +
      "Comment#8\n" +
      "Comment#9\n" +
      "Comment#10\n" +
      "Comment#11\n" +
      "Comment#12\n" +
      "Comment#13\n" +
      "Comment#14\n" +
      "Comment#15\n"
  }
}
