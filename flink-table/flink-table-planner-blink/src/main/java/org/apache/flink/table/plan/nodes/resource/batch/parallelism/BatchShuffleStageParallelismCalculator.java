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

package org.apache.flink.table.plan.nodes.resource.batch.parallelism;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Parallelism calculator for shuffle stage.
 */
public class BatchShuffleStageParallelismCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(BatchShuffleStageParallelismCalculator.class);
	private final Configuration tableConf;
	private final int envParallelism;

	public BatchShuffleStageParallelismCalculator(Configuration tableConf, int envParallelism) {
		this.tableConf = tableConf;
		this.envParallelism = envParallelism;
	}

	public void calculate(Collection<ShuffleStage> shuffleStages) {
		Set<ShuffleStage> shuffleStageSet = new HashSet<>(shuffleStages);
		shuffleStageSet.forEach(this::calculate);
	}

	@VisibleForTesting
	protected void calculate(ShuffleStage shuffleStage) {
		if (shuffleStage.isFinalParallelism()) {
			return;
		}
		Set<ExecNode<?, ?>> nodeSet = shuffleStage.getExecNodeSet();
		int maxSourceParallelism = -1;
		for (ExecNode<?, ?> node : nodeSet) {
			if (node instanceof BatchExecTableSourceScan) {
				int result = calculateSource((BatchExecTableSourceScan) node);
				if (result > maxSourceParallelism) {
					maxSourceParallelism = result;
				}
			}
		}
		if (maxSourceParallelism > 0) {
			shuffleStage.setParallelism(maxSourceParallelism, false);
		} else {
			shuffleStage.setParallelism(NodeResourceConfig.getOperatorDefaultParallelism(getTableConf(), envParallelism), false);
		}
	}

	private int calculateSource(BatchExecTableSourceScan tableSourceScan) {
		boolean infer = !NodeResourceConfig.getInferMode(tableConf).equals(NodeResourceConfig.InferMode.NONE);
		LOG.info("infer source partitions num: " + infer);
		if (infer) {
			double rowCount = tableSourceScan.getEstimatedRowCount();
			LOG.info("source row count is : " + rowCount);
			long rowsPerPartition = NodeResourceConfig.getInferRowCountPerPartition(tableConf);
			int maxNum = NodeResourceConfig.getSourceMaxParallelism(tableConf);
			return Math.min(maxNum,
					Math.max(
							(int) (rowCount / rowsPerPartition),
							1));
		} else {
			return NodeResourceConfig.getSourceParallelism(tableConf, envParallelism);
		}
	}

	private Configuration getTableConf() {
		return this.tableConf;
	}
}
