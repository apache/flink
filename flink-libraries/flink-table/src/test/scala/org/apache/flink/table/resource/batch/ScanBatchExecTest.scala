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

package org.apache.flink.table.resource.batch

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamSource
import org.apache.flink.streaming.api.transformations.SourceTransformation
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan
import org.apache.flink.table.util.TableTestBase
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito

class ScanBatchExecTest extends TableTestBase  {

  private val util = batchTestUtil()

  @Test
  def testSetDefaultResourceAndParallelism(): Unit = {
    val exec = new BatchExecBoundedStreamScan(
      Mockito.mock(classOf[RelOptCluster]),
      RelTraitSet.createEmpty(),
      Mockito.mock(classOf[RelOptTable]),
      Mockito.mock(classOf[RelDataType]));
    exec.getResource.setParallelism(2)
    val defaultResource = newResource(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_CPU.defaultValue(),
      TableConfigOptions.SQL_RESOURCE_SOURCE_DEFAULT_MEM.defaultValue())
    exec.sourceResSpec = defaultResource
    val transformation = new SourceTransformation[Integer](
      "test",
      Mockito.mock(classOf[StreamSource[Integer, SourceFunction[Integer]]]),
      BasicTypeInfo.INT_TYPE_INFO,
      -1).asInstanceOf[SourceTransformation[Any]]

    exec.assignSourceResourceAndParallelism(util.tableEnv, transformation)

    assertEquals(2, transformation.getParallelism);
    assertEquals(
      defaultResource,
      transformation.getMinResources);
    assertEquals(
      defaultResource,
      transformation.getPreferredResources);
  }

  def newResource(cpu: Double, memory: Int): ResourceSpec = {
    val builder = ResourceSpec.newBuilder()
    builder.setCpuCores(TableConfigOptions.SQL_RESOURCE_DEFAULT_CPU.defaultValue())
    builder.setHeapMemoryInMB(TableConfigOptions.SQL_RESOURCE_SOURCE_DEFAULT_MEM.defaultValue())
    builder.build()
  }
}
