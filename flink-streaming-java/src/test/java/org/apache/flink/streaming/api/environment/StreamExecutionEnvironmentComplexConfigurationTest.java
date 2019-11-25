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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Tests for configuring {@link StreamExecutionEnvironment} via
 * {@link StreamExecutionEnvironment#configure(ReadableConfig, ClassLoader)} with complex types.
 *
 * @see StreamExecutionEnvironmentConfigurationTest
 */
public class StreamExecutionEnvironmentComplexConfigurationTest {
	@Test
	public void testLoadingStateBackendFromConfiguration() {
		StreamExecutionEnvironment envFromConfiguration = StreamExecutionEnvironment.getExecutionEnvironment();

		Configuration configuration = new Configuration();
		configuration.setString("state.backend", "jobmanager");

		// mutate config according to configuration
		envFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

		StateBackend actualStateBackend = envFromConfiguration.getStateBackend();
		assertThat(actualStateBackend, instanceOf(MemoryStateBackend.class));
	}

	@Test
	public void testLoadingCachedFilesFromConfiguration() {
		StreamExecutionEnvironment envFromConfiguration = StreamExecutionEnvironment.getExecutionEnvironment();
		envFromConfiguration.registerCachedFile("/tmp3", "file3", true);

		Configuration configuration = new Configuration();
		configuration.setString("pipeline.cached-files", "name:file1,path:/tmp1,executable:true;name:file2,path:/tmp2");

		// mutate config according to configuration
		envFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

		assertThat(envFromConfiguration.getCachedFiles(), equalTo(Arrays.asList(
			Tuple2.of("file1", new DistributedCache.DistributedCacheEntry("/tmp1", true)),
			Tuple2.of("file2", new DistributedCache.DistributedCacheEntry("/tmp2", false))
		)));
	}

	@Test
	public void testNotOverridingStateBackendWithDefaultsFromConfiguration() {
		StreamExecutionEnvironment envFromConfiguration = StreamExecutionEnvironment.getExecutionEnvironment();
		envFromConfiguration.setStateBackend(new MemoryStateBackend());

		// mutate config according to configuration
		envFromConfiguration.configure(new Configuration(), Thread.currentThread().getContextClassLoader());

		StateBackend actualStateBackend = envFromConfiguration.getStateBackend();
		assertThat(actualStateBackend, instanceOf(MemoryStateBackend.class));
	}

	@Test
	public void testNotOverridingCachedFilesFromConfiguration() {
		StreamExecutionEnvironment envFromConfiguration = StreamExecutionEnvironment.getExecutionEnvironment();
		envFromConfiguration.registerCachedFile("/tmp3", "file3", true);

		Configuration configuration = new Configuration();

		// mutate config according to configuration
		envFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

		assertThat(envFromConfiguration.getCachedFiles(), equalTo(Arrays.asList(
			Tuple2.of("file3", new DistributedCache.DistributedCacheEntry("/tmp3", true))
		)));
	}
}
