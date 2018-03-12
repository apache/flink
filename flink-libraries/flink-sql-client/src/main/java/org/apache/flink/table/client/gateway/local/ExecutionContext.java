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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactoryService;

import java.net.URL;
import java.util.List;

/**
 * Context for executing table programs. It contains configured environments and environment
 * specific logic such as plan translation.
 */
public class ExecutionContext {

	private final SessionContext sessionContext;
	private final Environment mergedEnv;
	private final ExecutionEnvironment execEnv;
	private final StreamExecutionEnvironment streamExecEnv;
	private final TableEnvironment tableEnv;
	private final ClassLoader classLoader;
	private final QueryConfig queryConfig;

	public ExecutionContext(Environment defaultEnvironment, SessionContext sessionContext, List<URL> dependencies) {
		this.sessionContext = sessionContext;
		this.mergedEnv = Environment.merge(defaultEnvironment, sessionContext.getEnvironment());

		// create environments
		if (mergedEnv.getExecution().isStreamingExecution()) {
			streamExecEnv = createStreamExecutionEnvironment();
			execEnv = null;
			tableEnv = TableEnvironment.getTableEnvironment(streamExecEnv);
		} else {
			streamExecEnv = null;
			execEnv = createExecutionEnvironment();
			tableEnv = TableEnvironment.getTableEnvironment(execEnv);
		}

		// create class loader
		classLoader = FlinkUserCodeClassLoaders.parentFirst(
			dependencies.toArray(new URL[dependencies.size()]),
			this.getClass().getClassLoader());

		// create table sources
		mergedEnv.getSources().forEach((name, source) -> {
			TableSource<?> tableSource = TableSourceFactoryService.findAndCreateTableSource(source, classLoader);
			tableEnv.registerTableSource(name, tableSource);
		});

		// create query config
		queryConfig = createQueryConfig();
	}

	public SessionContext getSessionContext() {
		return sessionContext;
	}

	public ExecutionEnvironment getExecutionEnvironment() {
		return execEnv;
	}

	public StreamExecutionEnvironment getStreamExecutionEnvironment() {
		return streamExecEnv;
	}

	public TableEnvironment getTableEnvironment() {
		return tableEnv;
	}

	public ClassLoader getClassLoader() {
		return classLoader;
	}

	public Environment getMergedEnvironment() {
		return mergedEnv;
	}

	public QueryConfig getQueryConfig() {
		return queryConfig;
	}

	public ExecutionConfig getExecutionConfig() {
		if (streamExecEnv != null) {
			return streamExecEnv.getConfig();
		} else {
			return execEnv.getConfig();
		}
	}

	public FlinkPlan createPlan(String name, Configuration flinkConfig) {
		if (streamExecEnv != null) {
			final StreamGraph graph = streamExecEnv.getStreamGraph();
			graph.setJobName(name);
			return graph;
		} else {
			final int parallelism = execEnv.getParallelism();
			final Plan unoptimizedPlan = execEnv.createProgramPlan();
			unoptimizedPlan.setJobName(name);
			final Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), flinkConfig);
			return ClusterClient.getOptimizedPlan(compiler, unoptimizedPlan, parallelism);
		}
	}

	// --------------------------------------------------------------------------------------------

	private ExecutionEnvironment createExecutionEnvironment() {
		final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
		execEnv.setParallelism(mergedEnv.getExecution().getParallelism());
		return execEnv;
	}

	private StreamExecutionEnvironment createStreamExecutionEnvironment() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(mergedEnv.getExecution().getParallelism());
		env.setMaxParallelism(mergedEnv.getExecution().getMaxParallelism());
		env.setStreamTimeCharacteristic(mergedEnv.getExecution().getTimeCharacteristic());
		return env;
	}

	private QueryConfig createQueryConfig() {
		if (streamExecEnv != null) {
			final StreamQueryConfig config = new StreamQueryConfig();
			final long minRetention = mergedEnv.getExecution().getMinStateRetention();
			final long maxRetention = mergedEnv.getExecution().getMaxStateRetention();
			config.withIdleStateRetentionTime(Time.milliseconds(minRetention), Time.milliseconds(maxRetention));
			return config;
		} else {
			return new BatchQueryConfig();
		}
	}
}
