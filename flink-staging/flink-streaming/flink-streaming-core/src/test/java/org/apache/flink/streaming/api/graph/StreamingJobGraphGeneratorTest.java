/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.graph;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingJobGraphGeneratorTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGeneratorTest.class);
	
	@Test
	public void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
		final long seed = System.currentTimeMillis();
		LOG.info("Test seed: {}", new Long(seed));
		final Random r = new Random(seed);
		
		TestStreamEnvironment env = new TestStreamEnvironment(4, 32);
		StreamGraph streamingJob = new StreamGraph(env);
		StreamingJobGraphGenerator compiler = new StreamingJobGraphGenerator(streamingJob);
		
		boolean closureCleanerEnabled = r.nextBoolean(), forceAvroEnabled = r.nextBoolean(), forceKryoEnabled = r.nextBoolean(), objectReuseEnabled = r.nextBoolean(), sysoutLoggingEnabled = r.nextBoolean();
		int dop = 1 + r.nextInt(10);
		
		ExecutionConfig config = streamingJob.getExecutionConfig();
		if(closureCleanerEnabled) {
			config.enableClosureCleaner();
		} else {
			config.disableClosureCleaner();
		}
		if(forceAvroEnabled) {
			config.enableForceAvro();
		} else {
			config.disableForceAvro();
		}
		if(forceKryoEnabled) {
			config.enableForceKryo();
		} else {
			config.disableForceKryo();
		}
		if(objectReuseEnabled) {
			config.enableObjectReuse();
		} else {
			config.disableObjectReuse();
		}
		if(sysoutLoggingEnabled) {
			config.enableSysoutLogging();
		} else {
			config.disableSysoutLogging();
		}
		config.setParallelism(dop);
		
		JobGraph jobGraph = compiler.createJobGraph("test");
		ExecutionConfig executionConfig = (ExecutionConfig) InstantiationUtil.readObjectFromConfig(
				jobGraph.getJobConfiguration(),
				ExecutionConfig.CONFIG_KEY,
				Thread.currentThread().getContextClassLoader());
		
		Assert.assertEquals(closureCleanerEnabled, executionConfig.isClosureCleanerEnabled());
		Assert.assertEquals(forceAvroEnabled, executionConfig.isForceAvroEnabled());
		Assert.assertEquals(forceKryoEnabled, executionConfig.isForceKryoEnabled());
		Assert.assertEquals(objectReuseEnabled, executionConfig.isObjectReuseEnabled());
		Assert.assertEquals(sysoutLoggingEnabled, executionConfig.isSysoutLoggingEnabled());
		Assert.assertEquals(dop, executionConfig.getParallelism());
	}
}
