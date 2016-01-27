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

import java.util.{List => JavaList, Random}

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.utils._
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class SampleITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private val RNG: Random = new Random

  private var result: JavaList[String] = null;

  @Before
  def initiate {
    ExecutionEnvironment.getExecutionEnvironment.setParallelism(5)
  }

  @After
  def after() = {
    TestBaseUtils.containsResultAsText(result, getSourceStrings)
  }

  @Test
  @throws(classOf[Exception])
  def testSamplerWithFractionWithoutReplacement {
    verifySamplerWithFractionWithoutReplacement(0d)
    verifySamplerWithFractionWithoutReplacement(0.2d)
    verifySamplerWithFractionWithoutReplacement(1.0d)
  }

  @Test
  @throws(classOf[Exception])
  def testSamplerWithFractionWithReplacement {
    verifySamplerWithFractionWithReplacement(0d)
    verifySamplerWithFractionWithReplacement(0.2d)
    verifySamplerWithFractionWithReplacement(1.0d)
    verifySamplerWithFractionWithReplacement(2.0d)
  }

  @Test
  @throws(classOf[Exception])
  def testSamplerWithSizeWithoutReplacement {
    verifySamplerWithFixedSizeWithoutReplacement(0)
    verifySamplerWithFixedSizeWithoutReplacement(2)
    verifySamplerWithFixedSizeWithoutReplacement(21)
  }

  @Test
  @throws(classOf[Exception])
  def testSamplerWithSizeWithReplacement {
    verifySamplerWithFixedSizeWithReplacement(0)
    verifySamplerWithFixedSizeWithReplacement(2)
    verifySamplerWithFixedSizeWithReplacement(21)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFractionWithoutReplacement(fraction: Double) {
    verifySamplerWithFractionWithoutReplacement(fraction, RNG.nextLong)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFractionWithoutReplacement(fraction: Double, seed: Long) {
    verifySamplerWithFraction(false, fraction, seed)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFractionWithReplacement(fraction: Double) {
    verifySamplerWithFractionWithReplacement(fraction, RNG.nextLong)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFractionWithReplacement(fraction: Double, seed: Long) {
    verifySamplerWithFraction(true, fraction, seed)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFraction(withReplacement: Boolean, fraction: Double, seed: Long) {
    val ds = getSourceDataSet()
    val sampled = ds.sample(withReplacement, fraction, seed)
    result = sampled.collect.asJava
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFixedSizeWithoutReplacement(numSamples: Int) {
    verifySamplerWithFixedSizeWithoutReplacement(numSamples, RNG.nextLong)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFixedSizeWithoutReplacement(numSamples: Int, seed: Long) {
    verifySamplerWithFixedSize(false, numSamples, seed)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFixedSizeWithReplacement(numSamples: Int) {
    verifySamplerWithFixedSizeWithReplacement(numSamples, RNG.nextLong)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFixedSizeWithReplacement(numSamples: Int, seed: Long) {
    verifySamplerWithFixedSize(true, numSamples, seed)
  }

  @throws(classOf[Exception])
  private def verifySamplerWithFixedSize(withReplacement: Boolean, numSamples: Int, seed: Long) {
    val ds = getSourceDataSet()
    val sampled = ds.sampleWithSize(withReplacement, numSamples, seed)
    result = sampled.collect.asJava
    assertEquals(numSamples, result.size)
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
