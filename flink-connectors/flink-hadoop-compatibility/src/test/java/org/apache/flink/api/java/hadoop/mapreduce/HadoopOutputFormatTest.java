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

package org.apache.flink.api.java.hadoop.mapreduce;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HadoopOutputFormat}.
 */
public class HadoopOutputFormatTest {

	private static final String MAPRED_OUTPUT_PATH = "an/ignored/file/";
	private static final String MAPRED_OUTPUT_DIR_KEY = "mapred.output.dir";

	@Test
	public void testWriteRecord() throws Exception {

		RecordWriter<String, Long> recordWriter = mock(DummyRecordWriter.class);
		HadoopOutputFormat<String, Long> hadoopOutputFormat = setupHadoopOutputFormat(new DummyOutputFormat(),
			Job.getInstance(), recordWriter, null, new Configuration());

		hadoopOutputFormat.writeRecord(new Tuple2<String, Long>());

		verify(recordWriter, times(1)).write(nullable(String.class), nullable(Long.class));
	}

	@Test
	public void testOpen() throws Exception {

		OutputFormat<String, Long> dummyOutputFormat = mock(DummyOutputFormat.class);
		OutputCommitter outputCommitter = setupOutputCommitter(true);
		when(dummyOutputFormat.getOutputCommitter(any(TaskAttemptContext.class))).thenReturn(outputCommitter);

		HadoopOutputFormat<String, Long> hadoopOutputFormat = setupHadoopOutputFormat(dummyOutputFormat,
			Job.getInstance(), new DummyRecordWriter(), setupOutputCommitter(true), new Configuration());

		hadoopOutputFormat.open(1, 4);

		verify(hadoopOutputFormat.outputCommitter, times(1)).setupJob(any(JobContext.class));
		verify(hadoopOutputFormat.mapreduceOutputFormat, times(1)).getRecordWriter(any(TaskAttemptContext.class));
	}

	@Test
	public void testCloseWithNeedsTaskCommitTrue() throws Exception {

		RecordWriter<String, Long> recordWriter = Mockito.mock(DummyRecordWriter.class);
		OutputCommitter outputCommitter = setupOutputCommitter(true);

		HadoopOutputFormat<String, Long> hadoopOutputFormat = setupHadoopOutputFormat(new DummyOutputFormat(),
			Job.getInstance(), recordWriter, outputCommitter, new Configuration());

		hadoopOutputFormat.close();

		verify(outputCommitter, times(1)).commitTask(nullable(TaskAttemptContext.class));
		verify(recordWriter, times(1)).close(nullable(TaskAttemptContext.class));
	}

	@Test
	public void testCloseWithNeedsTaskCommitFalse() throws Exception {

		RecordWriter<String, Long> recordWriter = Mockito.mock(DummyRecordWriter.class);
		OutputCommitter outputCommitter = setupOutputCommitter(false);

		HadoopOutputFormat<String, Long> hadoopOutputFormat = setupHadoopOutputFormat(new DummyOutputFormat(),
			Job.getInstance(), recordWriter, outputCommitter, new Configuration());

		hadoopOutputFormat.close();

		verify(outputCommitter, times(0)).commitTask(nullable(TaskAttemptContext.class));
		verify(recordWriter, times(1)).close(nullable(TaskAttemptContext.class));
	}

	@Test
	public void testConfigure() throws Exception {

		ConfigurableDummyOutputFormat outputFormat = mock(ConfigurableDummyOutputFormat.class);

		HadoopOutputFormat<String, Long> hadoopOutputFormat = setupHadoopOutputFormat(outputFormat, Job.getInstance(),
			null, null, new Configuration());

		hadoopOutputFormat.configure(new org.apache.flink.configuration.Configuration());

		verify(outputFormat, times(1)).setConf(any(Configuration.class));
	}

	@Test
	public void testFinalizedGlobal() throws Exception {

		HadoopOutputFormat<String, Long> hadoopOutputFormat = setupHadoopOutputFormat(new DummyOutputFormat(),
			Job.getInstance(), null, null, new Configuration());

		hadoopOutputFormat.finalizeGlobal(1);

		verify(hadoopOutputFormat.outputCommitter, times(1)).commitJob(any(JobContext.class));
	}

	private OutputCommitter setupOutputCommitter(boolean needsTaskCommit) throws IOException {
		OutputCommitter outputCommitter = Mockito.mock(OutputCommitter.class);
		when(outputCommitter.needsTaskCommit(nullable(TaskAttemptContext.class))).thenReturn(needsTaskCommit);
		doNothing().when(outputCommitter).commitTask(any(TaskAttemptContext.class));

		return outputCommitter;
	}

	private HadoopOutputFormat<String, Long> setupHadoopOutputFormat(
		OutputFormat<String, Long> outputFormat,
		Job job,
		RecordWriter<String, Long> recordWriter,
		OutputCommitter outputCommitter,
		Configuration configuration) {

		HadoopOutputFormat<String, Long> hadoopOutputFormat = new HadoopOutputFormat<>(outputFormat, job);
		hadoopOutputFormat.recordWriter = recordWriter;
		hadoopOutputFormat.outputCommitter = outputCommitter;
		hadoopOutputFormat.configuration = configuration;
		hadoopOutputFormat.configuration.set(MAPRED_OUTPUT_DIR_KEY, MAPRED_OUTPUT_PATH);

		return hadoopOutputFormat;
	}

	class DummyRecordWriter extends RecordWriter<String, Long> {
		@Override
		public void write(String key, Long value) throws IOException, InterruptedException {
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {

		}
	}

	class DummyOutputFormat extends OutputFormat<String, Long> {
		@Override
		public RecordWriter<String, Long> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
			return null;
		}

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
			final OutputCommitter outputCommitter = Mockito.mock(OutputCommitter.class);
			doNothing().when(outputCommitter).setupJob(any(JobContext.class));

			return outputCommitter;
		}
	}

	class ConfigurableDummyOutputFormat extends DummyOutputFormat implements Configurable {

		@Override
		public void setConf(Configuration configuration) {}

		@Override
		public Configuration getConf() {
			return null;
		}
	}
}
