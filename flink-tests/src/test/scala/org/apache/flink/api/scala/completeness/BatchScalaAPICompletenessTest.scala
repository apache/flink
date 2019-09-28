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
package org.apache.flink.api.scala.completeness

import java.lang.reflect.Method

import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala._
import org.junit.Test

import scala.language.existentials

/**
  * This checks whether the Scala API is up to feature parity with the Java API. Right now is very
  * simple, it is only checked whether a method with the same name exists.
  *
  * When adding excluded methods to the lists you should give a good reason in a comment.
  *
  * Note: This is inspired by the JavaAPICompletenessChecker from Spark.
  */
class BatchScalaAPICompletenessTest extends ScalaAPICompletenessTestBase {

   override def isExcludedByName(method: Method): Boolean = {
     val name = method.getDeclaringClass.getName + "." + method.getName
     val excludedNames = Seq(
       // These are only used internally. Should be internal API but Java doesn't have
       // private[flink].
       "org.apache.flink.api.java.DataSet.getExecutionEnvironment",
       "org.apache.flink.api.java.DataSet.getType",
       "org.apache.flink.api.java.operators.Operator.getResultType",
       "org.apache.flink.api.java.operators.Operator.getName",
       "org.apache.flink.api.java.operators.Grouping.getInputDataSet",
       "org.apache.flink.api.java.operators.Grouping.getKeys",
       "org.apache.flink.api.java.operators.SingleInputOperator.getInput",
       "org.apache.flink.api.java.operators.SingleInputOperator.getInputType",
       "org.apache.flink.api.java.operators.TwoInputOperator.getInput1",
       "org.apache.flink.api.java.operators.TwoInputOperator.getInput2",
       "org.apache.flink.api.java.operators.TwoInputOperator.getInput1Type",
       "org.apache.flink.api.java.operators.TwoInputOperator.getInput2Type",
       "org.apache.flink.api.java.ExecutionEnvironment.areExplicitEnvironmentsAllowed",
       "org.apache.flink.api.java.ExecutionEnvironment.resetContextEnvironment",

       // TypeHints are only needed for Java API, Scala API doesn't need them
       "org.apache.flink.api.java.operators.SingleInputUdfOperator.returns",
       "org.apache.flink.api.java.operators.TwoInputUdfOperator.returns",

       // This is really just a mapper, which in Scala can easily expressed as a map lambda
       "org.apache.flink.api.java.DataSet.writeAsFormattedText",

       // Exclude minBy and maxBy for now, since there is some discussion about our aggregator
       // semantics
       "org.apache.flink.api.java.DataSet.minBy",
       "org.apache.flink.api.java.DataSet.maxBy",
       "org.apache.flink.api.java.operators.UnsortedGrouping.minBy",
       "org.apache.flink.api.java.operators.UnsortedGrouping.maxBy",

       // This method is actually just an internal helper
       "org.apache.flink.api.java.DataSet.getCallLocationName"
     )
     val excludedPatterns = Seq(
       // We don't have project on tuples in the Scala API
       """^org\.apache\.flink\.api.java.*project""",

       // I don't want to have withParameters in the API since I consider Configuration to be
       // deprecated. But maybe that's just me ...
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

   @Test
   override def testCompleteness(): Unit = {
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
