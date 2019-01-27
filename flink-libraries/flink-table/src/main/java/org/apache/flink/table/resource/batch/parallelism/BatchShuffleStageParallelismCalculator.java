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

package org.apache.flink.table.resource.batch.parallelism;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.util.NodeResourceUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Abstract parallelism calculator for shuffle stage.
 */
public abstract class BatchShuffleStageParallelismCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(BatchShuffleStageParallelismCalculator.class);
	private final Configuration tableConf;
	protected final int envParallelism;

	public BatchShuffleStageParallelismCalculator(Configuration tableConf, int envParallelism) {
		this.tableConf = tableConf;
		this.envParallelism = envParallelism;
	}

	public void calculate(Collection<ShuffleStage> shuffleStages) {
		Set<ShuffleStage> shuffleStageSet = new HashSet<>(shuffleStages);
		shuffleStageSet.forEach(this::calculate);
	}

	protected abstract void calculate(ShuffleStage shuffleStage);

	protected int calculateSource(BatchExecScan scanBatchExec) {
		boolean infer = !NodeResourceUtil.getInferMode(tableConf).equals(NodeResourceUtil.InferMode.NONE);
		LOG.info("infer source partitions num: " + infer);
		if (infer) {
			double rowCount = scanBatchExec.getEstimatedRowCount();
			double io = rowCount * scanBatchExec.getEstimatedAverageRowSize();
			LOG.info("source row count is : " + rowCount);
			LOG.info("source data size is : " + io);
			long rowsPerPartition = NodeResourceUtil.getRelCountPerPartition(tableConf);
			long sizePerPartition = NodeResourceUtil.getSourceSizePerPartition(tableConf);
			int maxNum = NodeResourceUtil.getSourceMaxParallelism(tableConf);
			return Math.min(maxNum,
					Math.max(
							(int) Math.max(
									io / sizePerPartition / NodeResourceUtil.SIZE_IN_MB,
									rowCount / rowsPerPartition),
							1));
		} else {
			return NodeResourceUtil.getSourceParallelism(tableConf, envParallelism);
		}
	}

	protected Configuration getTableConf() {
		return this.tableConf;
	}
}
