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

	private final Mapper delegatedMapper;
	private Method mapMethod;
	private Method setupMethod;
	private Method cleanupMethod;

	public HadoopProxyMapper(Mapper delegatedMapper) {
		this.delegatedMapper = Preconditions.checkNotNull(delegatedMapper);
		try {
			mapMethod = delegatedMapper.getClass().getDeclaredMethod("map", Object.class, Object.class, Mapper.Context.class);
			mapMethod.setAccessible(true);
		} catch (NoSuchMethodException e) {
			mapMethod = null;
		}

		try {
			setupMethod = delegatedMapper.getClass().getDeclaredMethod("setup", Context.class);
			setupMethod.setAccessible(true);
		} catch (NoSuchMethodException e) {
			setupMethod = null;
		}

		try {
			cleanupMethod = delegatedMapper.getClass().getDeclaredMethod("cleanup", Context.class);
			cleanupMethod.setAccessible(true);
		} catch (NoSuchMethodException e) {
			cleanupMethod = null;
		}
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		if (setupMethod != null) {
			try {
				setupMethod.invoke(delegatedMapper, context);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		if (mapMethod != null) {
			try {
				mapMethod.invoke(delegatedMapper, key, value, context);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		if (cleanupMethod != null) {
			try {
				cleanupMethod.invoke(delegatedMapper, context);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	/**
	 * A dummy mapper context wrapped {@link Mapper.Context}.
	 */
	public class HadoopDummyMapperContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

		private Collector<Tuple2<KEYOUT, VALUEOUT>> flinkCollector;

		private Configuration hadoopConf;

		private final Tuple2<KEYOUT, VALUEOUT> outTuple = new Tuple2<KEYOUT, VALUEOUT>();

		public void setFlinkCollector(Collector<Tuple2<KEYOUT, VALUEOUT>> flinkCollector) {
			this.flinkCollector = flinkCollector;
		}

		public void setHadoopConf(Configuration hadoopConf) {
			this.hadoopConf = hadoopConf;
		}

		@Override
		public InputSplit getInputSplit() {
			return null;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return false;
		}

		@Override
		public KEYIN getCurrentKey() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public VALUEIN getCurrentValue() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public void write(KEYOUT keyout, VALUEOUT valueout) throws IOException, InterruptedException {
			this.outTuple.f0 = keyout;
			this.outTuple.f1 = valueout;
			this.flinkCollector.collect(outTuple);
		}

		@Override
		public OutputCommitter getOutputCommitter() {
			return null;
		}

		@Override
		public TaskAttemptID getTaskAttemptID() {
			return null;
		}

		@Override
		public void setStatus(String s) {

		}

		@Override
		public String getStatus() {
			return null;
		}

		@Override
		public float getProgress() {
			return 0;
		}

		@Override
		public Counter getCounter(Enum<?> anEnum) {
			return null;
		}

		@Override
		public Counter getCounter(String s, String s1) {
			return null;
		}

		@Override
		public Configuration getConfiguration() {
			return this.hadoopConf;
		}

		@Override
		public Credentials getCredentials() {
			return null;
		}

		@Override
		public JobID getJobID() {
			return null;
		}

		@Override
		public int getNumReduceTasks() {
			return 0;
		}

		@Override
		public Path getWorkingDirectory() throws IOException {
			return null;
		}

		@Override
		public Class<?> getOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getOutputValueClass() {
			return null;
		}

		@Override
		public Class<?> getMapOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return null;
		}

		@Override
		public String getJobName() {
			return null;
		}

		@Override
		public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
			return null;
		}

		@Override
		public RawComparator<?> getSortComparator() {
			return null;
		}

		@Override
		public String getJar() {
			return null;
		}

		@Override
		public RawComparator<?> getCombinerKeyGroupingComparator() {
			return null;
		}

		@Override
		public RawComparator<?> getGroupingComparator() {
			return null;
		}

		@Override
		public boolean getJobSetupCleanupNeeded() {
			return false;
		}

		@Override
		public boolean getTaskCleanupNeeded() {
			return false;
		}

		@Override
		public boolean getProfileEnabled() {
			return false;
		}

		@Override
		public String getProfileParams() {
			return null;
		}

		@Override
		public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
			return null;
		}

		@Override
		public String getUser() {
			return null;
		}

		@Override
		public boolean getSymlink() {
			return false;
		}

		@Override
		public Path[] getArchiveClassPaths() {
			return new Path[0];
		}

		@Override
		public URI[] getCacheArchives() throws IOException {
			return new URI[0];
		}

		@Override
		public URI[] getCacheFiles() throws IOException {
			return new URI[0];
		}

		@Override
		public Path[] getLocalCacheArchives() throws IOException {
			return new Path[0];
		}

		@Override
		public Path[] getLocalCacheFiles() throws IOException {
			return new Path[0];
		}

		@Override
		public Path[] getFileClassPaths() {
			return new Path[0];
		}

		@Override
		public String[] getArchiveTimestamps() {
			return new String[0];
		}

		@Override
		public String[] getFileTimestamps() {
			return new String[0];
		}

		@Override
		public int getMaxMapAttempts() {
			return 0;
		}

		@Override
		public int getMaxReduceAttempts() {
			return 0;
		}

		@Override
		public void progress() {

		}
	}
}
