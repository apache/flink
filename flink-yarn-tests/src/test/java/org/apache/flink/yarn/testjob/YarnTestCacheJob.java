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

package org.apache.flink.yarn.testjob;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Testing job for distributed cache in per job cluster mode.
 */
public class YarnTestCacheJob {
	private static final List<String> LIST = ImmutableList.of("test1", "test2");
	private static final String TEST_DIRECTORY_NAME = "test_directory";

	public static JobGraph getDistributedCacheJobGraph(File testDirectory) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final String cacheFilePath = Thread.currentThread().getContextClassLoader()
			.getResource("cache.properties").getFile();
		env.registerCachedFile(testDirectory.getAbsolutePath(), TEST_DIRECTORY_NAME);
		env.registerCachedFile(cacheFilePath, "cacheFile", false);

		env.addSource(new GenericSourceFunction(LIST, TypeInformation.of(String.class)))
			.setParallelism(1)
			.map(new MapperFunction(), TypeInformation.of(String.class))
			.setParallelism(1)
			.addSink(new DiscardingSink<String>())
			.setParallelism(1);

		return env.getStreamGraph().getJobGraph();
	}

	private static class MapperFunction extends RichMapFunction<String, String> {
		private Properties properties;
		private static final long serialVersionUID = -1238033916372648233L;

		@Override
		public void open(Configuration config) throws IOException {
			// access cached file via RuntimeContext and DistributedCache
			final File cacheFile = getRuntimeContext().getDistributedCache().getFile("cacheFile");
			final FileInputStream inputStream = new FileInputStream(cacheFile);
			properties = new Properties();
			properties.load(inputStream);
			checkArgument(properties.size() == 2, "The property file should contains 2 pair of key values");

			final File testDirectory = getRuntimeContext().getDistributedCache().getFile(TEST_DIRECTORY_NAME);
			if (!testDirectory.isDirectory()) {
				throw new RuntimeException(String.format("%s is not a directory!", testDirectory.getAbsolutePath()));
			}
		}

		@Override
		public String map(String value) {
			final String property = (String) properties.getOrDefault(value, "null");
			checkState(property.equals(value + "_property"));
			return value;
		}
	}

	private static class GenericSourceFunction<T> implements SourceFunction<T>, ResultTypeQueryable<T> {
		private List<T> inputDataset;
		private TypeInformation returnType;

		GenericSourceFunction(List<T> inputDataset, TypeInformation returnType) {
			this.inputDataset = inputDataset;
			this.returnType = returnType;
		}

		@Override
		public void run(SourceContext<T> ctx) throws Exception {

			for (T t : inputDataset) {
				ctx.collect(t);
			}
		}

		@Override
		public void cancel() {}

		@Override
		public TypeInformation getProducedType() {
			return this.returnType;
		}
	}
}
