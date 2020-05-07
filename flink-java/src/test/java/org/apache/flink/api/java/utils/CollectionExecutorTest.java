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

package org.apache.flink.api.java.utils;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CollectionPipelineExecutor} and {@link CollectionExecutorFactory}.
 */
public class CollectionExecutorTest {

	@Test
	public void testExecuteWithCollectionExecutor() throws Exception {
		Configuration config = new Configuration();
		config.set(DeploymentOptions.TARGET, CollectionPipelineExecutor.NAME);
		config.set(DeploymentOptions.ATTACHED, true);

		PipelineExecutorFactory factory = DefaultExecutorServiceLoader.INSTANCE.getExecutorFactory(config);
		assertTrue(factory instanceof CollectionExecutorFactory);

		PipelineExecutor executor = factory.getExecutor(config);
		assertTrue(executor instanceof CollectionPipelineExecutor);

		// use CollectionsEnvironment to build DataSet graph
		final ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
		List<String> result = new ArrayList<>();

		DataSink<?> sink = env.fromCollection(Collections.singletonList("a#b")).flatMap(
				new FlatMapIterator<String, String>() {
					@Override
					public Iterator<String> flatMap(String value) {
						return Arrays.asList(value.split("#")).iterator();
					}
				}).output(new LocalCollectionOutputFormat<>(result));

		PlanGenerator generator = new PlanGenerator(
				Collections.singletonList(sink),
				env.getConfig(),
				env.getParallelism(),
				Collections.emptyList(),
				"test");
		Plan plan = generator.generate();
		// execute with CollectionPipelineExecutor
		executor.execute(plan, config);

		assertEquals(Arrays.asList("a", "b"), result);
	}
}
