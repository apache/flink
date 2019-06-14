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

package org.apache.flink.table.plan.nodes.resource.parallelism;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecCalc;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.plan.nodes.resource.NodeResourceConfig;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link ShuffleStageParallelismCalculator}.
 */
public class ShuffleStageParallelismCalculatorTest {

	private Configuration tableConf;
	private BatchExecTableSourceScan batchTableSourceScan = mock(BatchExecTableSourceScan.class);
	private StreamExecTableSourceScan streamTableSourceScan = mock(StreamExecTableSourceScan.class);
	private int envParallelism = 5;

	@Before
	public void setUp() {
		tableConf = new Configuration();
		tableConf.setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, NodeResourceConfig.InferMode.ONLY_SOURCE.toString());
		tableConf.setLong(TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION, 100);
		tableConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		when(batchTableSourceScan.getEstimatedRowCount()).thenReturn(3000d);
	}

	@Test
	public void testOnlyBatchSource() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.getMaxParallelism()).thenReturn(40);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(batchTableSourceScan)));
		ShuffleStageParallelismCalculator.calculate(tableConf, envParallelism, Arrays.asList(shuffleStage0));
		verify(shuffleStage0).setParallelism(30, false);
	}

	@Test
	public void testOnlyStreamSource() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.getMaxParallelism()).thenReturn(20);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(streamTableSourceScan)));
		ShuffleStageParallelismCalculator.calculate(tableConf, envParallelism, Arrays.asList(shuffleStage0));
		verify(shuffleStage0).setParallelism(20, false);
	}

	@Test
	public void testBatchSourceAndCalc() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.getMaxParallelism()).thenReturn(20);
		BatchExecCalc calc = mock(BatchExecCalc.class);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(batchTableSourceScan, calc)));
		ShuffleStageParallelismCalculator.calculate(tableConf, envParallelism, Arrays.asList(shuffleStage0));
		verify(shuffleStage0).setParallelism(20, false);
	}

	@Test
	public void testStreamSourceAndCalc() {
		tableConf.setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM, 60);
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.getMaxParallelism()).thenReturn(60);
		StreamExecCalc calc = mock(StreamExecCalc.class);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(streamTableSourceScan, calc)));
		ShuffleStageParallelismCalculator.calculate(tableConf, envParallelism, Arrays.asList(shuffleStage0));
		verify(shuffleStage0).setParallelism(60, false);
	}

	@Test
	public void testNoSource() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		BatchExecCalc calc = mock(BatchExecCalc.class);
		when(shuffleStage0.getMaxParallelism()).thenReturn(Integer.MAX_VALUE);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(calc)));
		ShuffleStageParallelismCalculator.calculate(tableConf, envParallelism, Arrays.asList(shuffleStage0));
		verify(shuffleStage0).setParallelism(50, false);
	}

	@Test
	public void testEnvParallelism() {
		tableConf.setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, NodeResourceConfig.InferMode.NONE.toString());
		tableConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, -1);
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.getMaxParallelism()).thenReturn(4);
		BatchExecCalc calc = mock(BatchExecCalc.class);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(batchTableSourceScan, calc)));
		ShuffleStageParallelismCalculator.calculate(tableConf, envParallelism, Arrays.asList(shuffleStage0));
		verify(shuffleStage0).setParallelism(4, false);
	}

	private Set<ExecNode<?, ?>> getNodeSet(List<ExecNode<?, ?>> nodeList) {
		Set<ExecNode<?, ?>> nodeSet = new HashSet<>();
		nodeSet.addAll(nodeList);
		return nodeSet;
	}
}

