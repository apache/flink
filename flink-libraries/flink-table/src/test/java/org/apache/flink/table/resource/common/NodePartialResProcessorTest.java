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

package org.apache.flink.table.resource.common;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.NodeResource;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecCalc;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.resource.MockNodeTestBase;

import org.apache.calcite.rel.RelDistribution;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for NodePartialResProcessor.
 */
public class NodePartialResProcessorTest extends MockNodeTestBase {
	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private BatchTableEnvironment tEnv;

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getBatchTableEnvironment(sEnv);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCalc() {
		/**
		 *        0, Source   1, Source   7, Source
		 *               \     /          /
		 *                2, Union
		 *                  /    \
		 *            3, Calc     \
		 *               \         \
		 *            4, Exchange  5, Exchange
		 *                \         /
		 *                 6, HashJoin
		 */
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_DEFAULT_MEM, 40);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_MEM, 20);
		tEnv.getConfig().getConf().setDouble(TableConfigOptions.SQL_RESOURCE_DEFAULT_CPU, 0.5);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_DIRECT_MEM, 25);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_DIRECT_MEM, 30);
		createNodeList(8);
		BatchExecScan scan0 = mock(BatchExecBoundedStreamScan.class);
		updateNode(0, scan0);
		BatchExecScan scan1 = mock(BatchExecTableSourceScan.class);
		updateNode(1, scan1);
		when(scan1.getSourceTransformation(any()).getMinResources()).thenReturn(buildResourceSpec(0.7d, 50, 10));
		when(scan1.needInternalConversion()).thenReturn(true);
		BatchExecScan scan2 = mock(BatchExecTableSourceScan.class);
		updateNode(7, scan2);
		when(scan2.getSourceTransformation(any()).getMinResources()).thenReturn(null);
		when(scan2.needInternalConversion()).thenReturn(true);
		updateNode(2, mock(BatchExecUnion.class));
		BatchExecExchange execExchange5 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange5.getDistribution().getType()).thenReturn(RelDistribution.Type.RANGE_DISTRIBUTED);
		updateNode(4, execExchange5);
		BatchExecExchange execExchange6 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange6.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		updateNode(5, execExchange6);
		connect(2, 0, 1, 7);
		connect(3, 2);
		connect(4, 3);
		connect(5, 2);
		connect(6, 4, 5);
		new NodePartialResProcessor().process(Collections.singletonList(nodeList.get(6)), new DAGProcessContext(tEnv, null));
		assertEquals(buildResource(0, 0, 0), nodeList.get(0).getResource());
		assertEquals(buildResource(0, 0, 0), nodeList.get(1).getResource());
		verify((BatchExecTableSourceScan) nodeList.get(1)).setResForSourceAndConversion(buildResourceSpec(0.7, 50, 10), buildResourceSpec(0.5, 20, 30));
		assertEquals(buildResource(0.5d, 20, 30), nodeList.get(3).getResource());
		assertEquals(buildResource(0.5d, 20, 30), nodeList.get(6).getResource());
		assertEquals(buildResource(0, 0, 0), nodeList.get(7).getResource());
		verify((BatchExecTableSourceScan) nodeList.get(7)).setResForSourceAndConversion(buildResourceSpec(0.5, 40, 25), buildResourceSpec(0.5, 20, 30));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testStreamCase() {
		/**
		 *        0, Source   1, Source   2, Source
		 *               \     /          /
		 *                3, Union
		 *                    \
		 *                 4, Calc
		 */
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_DEFAULT_MEM, 40);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_MEM, 20);
		tEnv.getConfig().getConf().setDouble(TableConfigOptions.SQL_RESOURCE_DEFAULT_CPU, 0.5);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_DIRECT_MEM, 25);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_DIRECT_MEM, 30);
		createNodeList(5);
		StreamExecScan scan0 = mock(StreamExecDataStreamScan.class);
		updateNode(0, scan0);
		StreamExecScan scan1 = mock(StreamExecTableSourceScan.class);
		updateNode(1, scan1);
		when(scan1.getSourceTransformation(any()).getMinResources()).thenReturn(buildResourceSpec(0.7d, 50, 10));
		when(scan1.needInternalConversion()).thenReturn(true);
		StreamExecScan scan2 = mock(StreamExecTableSourceScan.class);
		updateNode(2, scan2);
		when(scan2.getSourceTransformation(any()).getMinResources()).thenReturn(null);
		when(scan2.needInternalConversion()).thenReturn(true);
		updateNode(4, mock(StreamExecCalc.class));
		connect(3, 0, 1, 2);
		connect(4, 3);
		new NodePartialResProcessor().process(Collections.singletonList(nodeList.get(4)), new DAGProcessContext(tEnv, null));
		assertEquals(buildResource(0, 0, 0), nodeList.get(0).getResource());
		assertEquals(buildResource(0, 0, 0), nodeList.get(1).getResource());
		verify((StreamExecTableSourceScan) nodeList.get(1)).setResForSourceAndConversion(buildResourceSpec(0.7, 50, 10), buildResourceSpec(0.5, 20, 30));
		assertEquals(buildResource(0.5d, 20, 30), nodeList.get(4).getResource());
		verify((StreamExecTableSourceScan) nodeList.get(2)).setResForSourceAndConversion(buildResourceSpec(0.5, 40, 25), buildResourceSpec(0.5, 20, 30));
	}

	private NodeResource buildResource(double cpu, int heap, int direct) {
		NodeResource resource = new NodeResource();
		resource.setCpu(cpu);
		resource.setHeapMem(heap);
		resource.setDirectMem(direct);
		return resource;
	}

	private ResourceSpec buildResourceSpec(double cpu, int heap, int direct) {
		ResourceSpec.Builder builder = ResourceSpec.newBuilder();
		builder.setCpuCores(cpu);
		builder.setHeapMemoryInMB(heap);
		builder.setDirectMemoryInMB(direct);
		return builder.build();
	}
}
