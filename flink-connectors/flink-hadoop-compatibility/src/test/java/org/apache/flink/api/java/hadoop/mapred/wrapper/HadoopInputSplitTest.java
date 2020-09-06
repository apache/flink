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

package org.apache.flink.api.java.hadoop.mapred.wrapper;

import org.apache.flink.util.InstantiationUtil;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Test for {@link HadoopInputSplit}.
 */
public class HadoopInputSplitTest {

	private JobConf conf;

	@Before
	public void before() {
		Configuration configuration = new Configuration();
		for (int i = 0; i < 10000; i++) {
			configuration.set("key-" + i, "value-" + i);
		}
		this.conf = new JobConf(configuration);
	}

	private void testInner(
			FileSplit fileSplit,
			Consumer<Integer> serializeSizeChecker,
			Consumer<InputSplit> splitChecker) throws IOException, ClassNotFoundException {
		HadoopInputSplit split = new HadoopInputSplit(5, fileSplit, conf);

		byte[] bytes = InstantiationUtil.serializeObject(split);
		serializeSizeChecker.accept(bytes.length);

		split  = InstantiationUtil.deserializeObject(bytes, split.getClass().getClassLoader());
		Assert.assertEquals(5, split.getSplitNumber());
		Assert.assertArrayEquals(new String[]{"host0"}, split.getHostnames());
		splitChecker.accept(split.getHadoopInputSplit());
	}

	@Test
	public void testFileSplit() throws IOException, ClassNotFoundException {
		FileSplit fileSplit = new FileSplit(new Path("/test"), 0, 100, new String[]{"host0"});
		testInner(
				fileSplit,
				i -> Assert.assertTrue(i < 10000),
				split -> Assert.assertEquals(fileSplit, split));
	}

	@Test
	public void testConfigurable() throws IOException, ClassNotFoundException {
		ConfigurableFileSplit fileSplit = new ConfigurableFileSplit(
				new Path("/test"), 0, 100, new String[]{"host0"});
		testInner(fileSplit, i -> {}, inputSplit -> {
			ConfigurableFileSplit split = (ConfigurableFileSplit) inputSplit;
			Assert.assertNotNull(split.getConf());
			Assert.assertEquals(fileSplit, split);
		});
	}

	@Test
	public void testJobConfigurable() throws IOException, ClassNotFoundException {
		JobConfigurableFileSplit fileSplit = new JobConfigurableFileSplit(
				new Path("/test"), 0, 100, new String[]{"host0"});
		testInner(fileSplit, i -> {}, inputSplit -> {
			JobConfigurableFileSplit split = (JobConfigurableFileSplit) inputSplit;
			Assert.assertNotNull(split.getConf());
			Assert.assertEquals(fileSplit, split);
		});
	}

	/**
	 * Because of test class conflict, we can not use hadoop FileSplit, so we create
	 * a new FileSplit to test.
	 */
	private static class FileSplit implements InputSplit {

		private Path file;
		private long start;
		private long length;
		private String[] hosts;

		public FileSplit() {}

		private FileSplit(Path file, long start, long length, String[] hosts) {
			this.file = file;
			this.start = start;
			this.length = length;
			this.hosts = hosts;
		}

		@Override
		public long getLength() throws IOException {
			return length;
		}

		@Override
		public String[] getLocations() throws IOException {
			return hosts;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(file.toString());
			out.writeLong(start);
			out.writeLong(length);
			out.writeInt(hosts.length);
			for (String host : hosts) {
				out.writeUTF(host);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			file = new Path(in.readUTF());
			start = in.readLong();
			length = in.readLong();
			int size = in.readInt();
			hosts = new String[size];
			for (int i = 0; i < size; i++) {
				hosts[i] = in.readUTF();
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			FileSplit fileSplit = (FileSplit) o;
			return start == fileSplit.start &&
					length == fileSplit.length &&
					Objects.equals(file, fileSplit.file) &&
					Arrays.equals(hosts, fileSplit.hosts);
		}
	}

	private static class ConfigurableFileSplit extends FileSplit implements Configurable {

		private Configuration conf;

		public ConfigurableFileSplit() {}

		private ConfigurableFileSplit(Path file, long start, long length, String[] hosts) {
			super(file, start, length, hosts);
		}

		@Override
		public void setConf(Configuration configuration) {
			this.conf = configuration;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}
	}

	private static class JobConfigurableFileSplit extends FileSplit implements JobConfigurable {

		private JobConf jobConf;

		public JobConfigurableFileSplit() {}

		private JobConfigurableFileSplit(Path file, long start, long length, String[] hosts) {
			super(file, start, length, hosts);
		}

		@Override
		public void configure(JobConf jobConf) {
			this.jobConf = jobConf;
		}

		private JobConf getConf() {
			return jobConf;
		}
	}
}
