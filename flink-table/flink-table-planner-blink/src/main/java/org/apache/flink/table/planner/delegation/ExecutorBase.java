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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 */
@Internal
public abstract class ExecutorBase implements Executor {

	private static final String DEFAULT_JOB_NAME = "Flink Exec Table Job";

	private final StreamExecutionEnvironment execEnv;
	// buffer transformations to generate StreamGraph
	private List<Transformation<?>> transformations = new ArrayList<>();
	protected TableConfig tableConfig;

	public ExecutorBase(StreamExecutionEnvironment executionEnvironment) {
		this.execEnv = executionEnvironment;
	}

	public void setTableConfig(TableConfig tableConfig) {
		this.tableConfig = tableConfig;
	}

	@Override
	public void apply(List<Transformation<?>> transformations) {
		this.transformations.addAll(transformations);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		StreamGraph streamGraph = getStreamGraph(jobName);
		return execEnv.execute(streamGraph);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return execEnv;
	}

	/**
	 * Translates the applied transformations to a stream graph.
	 */
	public StreamGraph getStreamGraph(String jobName) {
		try {
			return getStreamGraph(transformations, jobName);
		} finally {
			transformations.clear();
		}
	}

	/**
	 * Translates the given transformations to a stream graph.
	 */
	public abstract StreamGraph getStreamGraph(List<Transformation<?>> transformations, String jobName);

	/**
	 * {@link StreamExecutionEnvironment} will hold the transformations and can not be cleared unless
	 * {@link StreamExecutionEnvironment#execute()} method is called.
	 * so if {@link TableEnvironment#explain(boolean)} is called before {@link TableEnvironment#execute(String)},
	 * the StreamGraph to executed will contain duplicated transformations,
	 * one is from explain method, and another is from execute method.
	 *
	 * <p>use {@link StreamGraphGenerator} directly instead of {@link StreamExecutionEnvironment#getStreamGraph}
	 * to avoid above case.
	 */
	protected StreamGraph generateStreamGraph(List<Transformation<?>> transformations, String jobName) {
		if (transformations.size() <= 0) {
			throw new IllegalStateException("No operators defined in streaming topology. Cannot generate StreamGraph.");
		}
		return getStreamGraphGenerator(transformations).setJobName(jobName).generate();
	}

	private StreamGraphGenerator getStreamGraphGenerator(List<Transformation<?>> transformations) {
		return new StreamGraphGenerator(transformations, execEnv.getConfig(), execEnv.getCheckpointConfig())
				.setStateBackend(execEnv.getStateBackend())
				.setChaining(execEnv.isChainingEnabled())
				.setUserArtifacts(execEnv.getCachedFiles())
				.setTimeCharacteristic(execEnv.getStreamTimeCharacteristic())
				.setDefaultBufferTimeout(execEnv.getBufferTimeout());
	}

	protected String getNonEmptyJobName(String jobName) {
		if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
			return DEFAULT_JOB_NAME;
		} else {
			return jobName;
		}
	}
}
