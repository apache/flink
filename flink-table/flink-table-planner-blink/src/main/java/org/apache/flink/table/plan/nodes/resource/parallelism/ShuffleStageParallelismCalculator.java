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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.plan.nodes.resource.NodeResourceUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Parallelism calculator for shuffleStages.
 */
public class ShuffleStageParallelismCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(ShuffleStageParallelismCalculator.class);
	private final Configuration tableConf;
	private final int envParallelism;

	private ShuffleStageParallelismCalculator(Configuration tableConf, int envParallelism) {
		this.tableConf = tableConf;
		this.envParallelism = envParallelism;
	}

	public static void calculate(Configuration tableConf, int envParallelism, Collection<ShuffleStage> shuffleStages) {
		new ShuffleStageParallelismCalculator(tableConf, envParallelism).calculate(shuffleStages);
	}

	private void calculate(Collection<ShuffleStage> shuffleStages) {
		Set<ShuffleStage> shuffleStageSet = new HashSet<>(shuffleStages);
		shuffleStageSet.forEach(this::calculate);
	}

	/**
	 * If there are source nodes in a shuffleStage, its parallelism is the max parallelism of source
	 * nodes. Otherwise, its parallelism is the default operator parallelism.
	 */
	@VisibleForTesting
	protected void calculate(ShuffleStage shuffleStage) {
		if (shuffleStage.isFinalParallelism()) {
			return;
		}
		Set<ExecNode<?, ?>> nodeSet = shuffleStage.getExecNodeSet();
		int sourceParallelism = -1;
		int maxParallelism = shuffleStage.getMaxParallelism();
		for (ExecNode<?, ?> node : nodeSet) {
			if (node instanceof BatchExecTableSourceScan || node instanceof StreamExecTableSourceScan) {
				int result = NodeResourceUtil.getSourceParallelism(tableConf, envParallelism);
				if (result > sourceParallelism) {
					sourceParallelism = result;
				}
			}
		}
		int shuffleStageParallelism;
		if (sourceParallelism > 0) {
			shuffleStageParallelism = sourceParallelism;
		} else {
			shuffleStageParallelism = NodeResourceUtil.getOperatorDefaultParallelism(getTableConf(), envParallelism);
		}
		if (shuffleStageParallelism > maxParallelism) {
			shuffleStageParallelism = maxParallelism;
		}
		shuffleStage.setParallelism(shuffleStageParallelism, false);
	}

	private Configuration getTableConf() {
		return this.tableConf;
	}
}
