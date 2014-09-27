/*
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
package org.apache.flink.api.scala

import java.lang.reflect.Method

import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.{DataSet => JavaDataSet}

import scala.language.existentials

import org.junit.Assert._
import org.junit.Test

/**
 * This checks whether the Scala API is up to feature parity with the Java API. Right now is very
 * simple, it is only checked whether a method with the same name exists.
 *
 * When adding excluded methods to the lists you should give a good reason in a comment.
 *
 * Note: This is inspired by the JavaAPICompletenessChecker from Spark.
 */
class ScalaAPICompletenessTest {

  private def isExcludedByName(method: Method): Boolean = {
    val name = method.getDeclaringClass.getName + "." + method.getName
    val excludedNames = Seq(
      // These are only used internally. Should be internal API but Java doesn't have
      // private[flink].
      "org.apache.flink.api.java.DataSet.getExecutionEnvironment",
      "org.apache.flink.api.java.DataSet.getType",
      "org.apache.flink.api.java.operators.Operator.getResultType",
      "org.apache.flink.api.java.operators.Operator.getName",
      "org.apache.flink.api.java.operators.Grouping.getDataSet",
      "org.apache.flink.api.java.operators.Grouping.getKeys",
      "org.apache.flink.api.java.operators.SingleInputOperator.getInput",
      "org.apache.flink.api.java.operators.SingleInputOperator.getInputType",
      "org.apache.flink.api.java.operators.TwoInputOperator.getInput1",
      "org.apache.flink.api.java.operators.TwoInputOperator.getInput2",
      "org.apache.flink.api.java.operators.TwoInputOperator.getInput1Type",
      "org.apache.flink.api.java.operators.TwoInputOperator.getInput2Type",
      "org.apache.flink.api.java.ExecutionEnvironment.localExecutionIsAllowed",
      "org.apache.flink.api.java.ExecutionEnvironment.setDefaultLocalParallelism",

      // This is really just a mapper, which in Scala can easily expressed as a map lambda
      "org.apache.flink.api.java.DataSet.writeAsFormattedText",

      // Exclude minBy and maxBy for now, since there is some discussion about our aggregator
      // semantics
      "org.apache.flink.api.java.DataSet.minBy",
      "org.apache.flink.api.java.DataSet.maxBy",
      "org.apache.flink.api.java.operators.UnsortedGrouping.minBy",
      "org.apache.flink.api.java.operators.UnsortedGrouping.maxBy",
      
      // Exclude first operator for now
      "org.apache.flink.api.java.DataSet.first",
      "org.apache.flink.api.java.operators.SortedGrouping.first",
      "org.apache.flink.api.java.operators.UnsortedGrouping.first",
      
      // Exclude explicit rebalance and hashPartitionBy for now
      "org.apache.flink.api.java.DataSet.partitionByHash",
      "org.apache.flink.api.java.DataSet.rebalance"

    )
    val excludedPatterns = Seq(
      // We don't have project on tuples in the Scala API
      """^org\.apache\.flink\.api.java.*project""",

      // I don't want to have withParameters in the API since I consider Configuration to be
      // deprecated. But maybe thats just me ...
      """^org\.apache\.flink\.api.java.*withParameters""",

      // These are only used internally. Should be internal API but Java doesn't have
      // private[flink].
      """^org\.apache\.flink\.api.java.*getBroadcastSets""",
      """^org\.apache\.flink\.api.java.*setSemanticProperties""",
      """^org\.apache\.flink\.api.java.*getSemanticProperties""",
      """^org\.apache\.flink\.api.java.*getParameters""",

      // Commented out for now until we have a use case for this.
      """^org\.apache\.flink\.api.java.*runOperation""",

      // Object methods
      """^.*notify""",
      """^.*wait""",
      """^.*notifyAll""",
      """^.*equals""",
      """^.*toString""",
      """^.*getClass""",
      """^.*hashCode"""
    ).map(_.r)
    lazy val excludedByPattern =
      excludedPatterns.map(_.findFirstIn(name)).filter(_.isDefined).nonEmpty
    name.contains("$") || excludedNames.contains(name) || excludedByPattern
  }

  private def isExcludedByInterface(method: Method): Boolean = {
    val excludedInterfaces =
      Set("org.apache.spark.Logging", "org.apache.hadoop.mapreduce.HadoopMapReduceUtil")
    def toComparisionKey(method: Method) =
      (method.getReturnType, method.getName, method.getGenericReturnType)
    val interfaces = method.getDeclaringClass.getInterfaces.filter { i =>
      excludedInterfaces.contains(i.getName)
    }
    val excludedMethods = interfaces.flatMap(_.getMethods.map(toComparisionKey))
    excludedMethods.contains(toComparisionKey(method))
  }

  private def checkMethods(
      javaClassName: String,
      scalaClassName: String,
      javaClass: Class[_],
      scalaClass: Class[_]) {
    val javaMethods = javaClass.getMethods
      .filterNot(_.isAccessible)
      .filterNot(isExcludedByName)
      .filterNot(isExcludedByInterface)
      .map(m => m.getName).toSet

    val scalaMethods = scalaClass.getMethods
      .filterNot(_.isAccessible)
      .filterNot(isExcludedByName)
      .filterNot(isExcludedByInterface)
      .map(m => m.getName).toSet

    val missingMethods = javaMethods -- scalaMethods

    for (method <- missingMethods) {
      fail("Method " + method + " from " + javaClass + " is missing from " + scalaClassName + ".")
    }
  }

  @Test
  def testCompleteness(): Unit = {
    checkMethods("DataSet", "DataSet", classOf[JavaDataSet[_]], classOf[DataSet[_]])

    checkMethods(
      "ExecutionEnvironment", "ExecutionEnvironment",
      classOf[org.apache.flink.api.java.ExecutionEnvironment], classOf[ExecutionEnvironment])

    checkMethods("Operator", "DataSet", classOf[Operator[_, _]], classOf[DataSet[_]])

    checkMethods("UnsortedGrouping", "GroupedDataSet",
      classOf[UnsortedGrouping[_]], classOf[GroupedDataSet[_]])

    checkMethods("SortedGrouping", "GroupedDataSet",
      classOf[SortedGrouping[_]], classOf[GroupedDataSet[_]])

    checkMethods("AggregateOperator", "AggregateDataSet",
      classOf[AggregateOperator[_]], classOf[AggregateDataSet[_]])

    checkMethods("SingleInputOperator", "DataSet",
      classOf[SingleInputOperator[_, _, _]], classOf[DataSet[_]])

    checkMethods("TwoInputOperator", "DataSet",
      classOf[TwoInputOperator[_, _, _, _]], classOf[DataSet[_]])

    checkMethods("SingleInputUdfOperator", "DataSet",
      classOf[SingleInputUdfOperator[_, _, _]], classOf[DataSet[_]])

    checkMethods("TwoInputUdfOperator", "DataSet",
      classOf[TwoInputUdfOperator[_, _, _, _]], classOf[DataSet[_]])
  }
}
