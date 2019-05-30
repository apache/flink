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

package org.apache.flink.hadoopcompatibility.mapreduce.wrapper;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;

/**
 * This is a proxy maps a Hadoop Mapper (mapreduce API) which use reflection to get and invoke some method.
 */
public class HadoopProxyMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private final boolean isObjectReuseEnabled;
	private final Method mapMethod;
	private Method setupMethod;
	private Method cleanupMethod;

	public HadoopProxyMapper(Mapper delegatedMapper, boolean isObjectReuseEnabled) throws NoSuchMethodException {
		Preconditions.checkNotNull(delegatedMapper);
		this.isObjectReuseEnabled = isObjectReuseEnabled;
		mapMethod = delegatedMapper.getClass().getDeclaredMethod("map", Object.class, Object.class, Mapper.Context.class);
		mapMethod.setAccessible(true);

		try {
			setupMethod = delegatedMapper.getClass().getDeclaredMethod("setup", Context.class);
			setupMethod.setAccessible(true);
		} catch (Exception e) {
			setupMethod = null;
		}

		try {
			cleanupMethod = delegatedMapper.getClass().getDeclaredMethod("cleanup", Context.class);
			cleanupMethod.setAccessible(true);
		} catch (Exception e) {
			cleanupMethod = null;
		}
	}

	public Method getMapMethod() {
		return mapMethod;
	}

	public Method getSetupMethod() {
		return setupMethod;
	}

	public Method getCleanupMethod() {
		return cleanupMethod;
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	/**
	 * A dummy mapper context wrapped {@link Mapper.Context}.
	 */
	public class HadoopDummyMapperContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

		private final Tuple2<KEYOUT, VALUEOUT> outTuple = new Tuple2<KEYOUT, VALUEOUT>();

		private KEYIN keyIn;
		private VALUEIN valueIn;
		private KEYOUT keyOut;
		private VALUEOUT valueOut;

		private Collector<Tuple2<KEYOUT, VALUEOUT>> flinkCollector;

		private Configuration hadoopConf;

		private Class<? extends Mapper<?, ?, ?, ?>> mapperClass;

		public void setFlinkCollector(Collector<Tuple2<KEYOUT, VALUEOUT>> flinkCollector) {
			this.flinkCollector = flinkCollector;
		}

		public void setHadoopConf(Configuration hadoopConf) {
			this.hadoopConf = hadoopConf;
		}

		public void setKeyIn(KEYIN keyIn) {
			this.keyIn = keyIn;
		}

		public void setValueIn(VALUEIN valueIn) {
			this.valueIn = valueIn;
		}

		public void setMapperClass(Class<? extends Mapper<?, ?, ?, ?>> mapperClass) {
			this.mapperClass = mapperClass;
		}

		@VisibleForTesting
		public boolean isObjectReuseEnabled() {
			return isObjectReuseEnabled;
		}

		@Override
		public InputSplit getInputSplit() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			throw new UnsupportedOperationException();
		}

		@Override
		public KEYIN getCurrentKey() throws IOException, InterruptedException {
			return this.keyIn;
		}

		@Override
		public VALUEIN getCurrentValue() throws IOException, InterruptedException {
			return this.valueIn;
		}

		@Override
		public void write(KEYOUT keyout, VALUEOUT valueout) throws IOException, InterruptedException {
			this.keyOut = keyout;
			this.valueOut = valueout;
			if (isObjectReuseEnabled) {
				this.outTuple.f0 = this.keyOut;
				this.outTuple.f1 = this.valueOut;
				this.flinkCollector.collect(outTuple);
			} else {
				Tuple2<KEYOUT, VALUEOUT> outTuple = new Tuple2<KEYOUT, VALUEOUT>();
				outTuple.f0 = this.keyOut;
				outTuple.f1 = this.valueOut;
				this.flinkCollector.collect(outTuple);
			}
		}

		@Override
		public OutputCommitter getOutputCommitter() {
			throw new UnsupportedOperationException();
		}

		@Override
		public TaskAttemptID getTaskAttemptID() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setStatus(String s) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getStatus() {
			throw new UnsupportedOperationException();
		}

		@Override
		public float getProgress() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Counter getCounter(Enum<?> anEnum) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Counter getCounter(String s, String s1) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Configuration getConfiguration() {
			return this.hadoopConf;
		}

		@Override
		public Credentials getCredentials() {
			throw new UnsupportedOperationException();
		}

		@Override
		public JobID getJobID() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getNumReduceTasks() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path getWorkingDirectory() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<?> getOutputKeyClass() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<?> getOutputValueClass() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<?> getMapOutputKeyClass() {
			return this.keyOut.getClass();
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return this.valueOut.getClass();
		}

		@Override
		public String getJobName() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
			return this.mapperClass;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
			throw new UnsupportedOperationException();
		}

		@Override
		public RawComparator<?> getSortComparator() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getJar() {
			throw new UnsupportedOperationException();
		}

		@Override
		public RawComparator<?> getCombinerKeyGroupingComparator() {
			throw new UnsupportedOperationException();
		}

		@Override
		public RawComparator<?> getGroupingComparator() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean getJobSetupCleanupNeeded() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean getTaskCleanupNeeded() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean getProfileEnabled() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getProfileParams() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getUser() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean getSymlink() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path[] getArchiveClassPaths() {
			throw new UnsupportedOperationException();
		}

		@Override
		public URI[] getCacheArchives() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public URI[] getCacheFiles() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path[] getLocalCacheArchives() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path[] getLocalCacheFiles() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path[] getFileClassPaths() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String[] getArchiveTimestamps() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String[] getFileTimestamps() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getMaxMapAttempts() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getMaxReduceAttempts() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void progress() {
			throw new UnsupportedOperationException();
		}
	}
}
