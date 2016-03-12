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
package org.apache.flink.api.scala.extensions

import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.optimizer.Optimizer
import org.apache.flink.optimizer.dag.{MapNode, OptimizerNode}
import org.apache.flink.optimizer.plan.{PlanNode, SinkPlanNode, SourcePlanNode}
import org.apache.flink.util.TestLogger
import org.junit.Assert.assertTrue
import org.junit._

import scala.collection.JavaConversions._

class AcceptPFMapITCase extends TestLogger {

  private val optimizer = new Optimizer(new Configuration)
  private val unusedResultPath = "UNUSED"

  def isTransformation(n: PlanNode): Boolean =
    !n.isInstanceOf[SourcePlanNode] && !n.isInstanceOf[SinkPlanNode]

  private def getOptimizerTransformationNodes(env: ExecutionEnvironment): Iterable[OptimizerNode] =
    for {
      node <- optimizer.compile(env.createProgramPlan("UNUSED")).getAllNodes
      transformation = node.getOptimizerNode if isTransformation(node)
    } yield transformation

  @Test
  def testIdentityMapperWithBasicType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getStringDataSet(env)
    val identityMapDs = ds.mapWith(identity)
    identityMapDs.writeAsText(unusedResultPath, WriteMode.OVERWRITE)
    val nodes = getOptimizerTransformationNodes(env)
    assertTrue("The plan should contain 1 transformation", nodes.size == 1)
    assertTrue("The transformation should be a map", nodes.forall(_.isInstanceOf[MapNode]))
  }

  @Test
  def testIdentityMapperWithTuple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val identityMapDs = ds.mapWith(identity)
    identityMapDs.writeAsCsv(unusedResultPath, writeMode = WriteMode.OVERWRITE)
    val nodes = getOptimizerTransformationNodes(env)
    assertTrue("The plan should contain 1 transformation", nodes.size == 1)
    assertTrue("The transformation should be a map", nodes.forall(_.isInstanceOf[MapNode]))
  }

  @Test
  def testTypeConversionMapperCustomToTuple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val typeConversionMapDs = ds.mapWith( c => (c.myInt, c.myLong, c.myString) )
    typeConversionMapDs.writeAsCsv(unusedResultPath, writeMode = WriteMode.OVERWRITE)
    val nodes = getOptimizerTransformationNodes(env)
    assertTrue("The plan should contain 1 transformation", nodes.size == 1)
    assertTrue("The transformation should be a map", nodes.forall(_.isInstanceOf[MapNode]))
  }

  @Test
  def testTypeConversionMapperTupleToBasic(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val typeConversionMapDs = ds.mapWith { case (_, _, a) => a }
    typeConversionMapDs.writeAsText(unusedResultPath, WriteMode.OVERWRITE)
    val nodes = getOptimizerTransformationNodes(env)
    assertTrue("The plan should contain 1 transformation", nodes.size == 1)
    assertTrue("The transformation should be a map", nodes.forall(_.isInstanceOf[MapNode]))
  }

  @Test
  def testMapperOnTupleIncrementFieldReorderSecondAndThirdFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val tupleMapDs = ds.mapWith { case (a, b, c) => (a + 1, c, b) }
    tupleMapDs.writeAsCsv(unusedResultPath, writeMode = WriteMode.OVERWRITE)
    val nodes = getOptimizerTransformationNodes(env)
    assertTrue("The plan should contain 1 transformation", nodes.size == 1)
    assertTrue("The transformation should be a map", nodes.forall(_.isInstanceOf[MapNode]))
  }

  @Test
  def testMapperOnCustomLowercaseString(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val customMapDs = ds.mapWith { c => c.myString = c.myString.toLowerCase; c }
    customMapDs.writeAsText(unusedResultPath, WriteMode.OVERWRITE)
    val nodes = getOptimizerTransformationNodes(env)
    assertTrue("The plan should contain 1 transformation", nodes.size == 1)
    assertTrue("The transformation should be a map", nodes.forall(_.isInstanceOf[MapNode]))
  }

}
