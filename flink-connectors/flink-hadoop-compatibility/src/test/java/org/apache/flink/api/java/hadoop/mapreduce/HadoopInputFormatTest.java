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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapreduce.wrapper.HadoopInputSplit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HadoopInputFormat}.
 */
public class HadoopInputFormatTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testConfigure() throws Exception {

		ConfigurableDummyInputFormat inputFormat = mock(ConfigurableDummyInputFormat.class);

		HadoopInputFormat<String, Long> hadoopInputFormat = setupHadoopInputFormat(inputFormat, Job.getInstance(), null);
		hadoopInputFormat.configure(new org.apache.flink.configuration.Configuration());

		verify(inputFormat, times(1)).setConf(any(Configuration.class));
	}

	@Test
	public void testCreateInputSplits() throws Exception {
		DummyInputFormat inputFormat = mock(DummyInputFormat.class);

		HadoopInputFormat<String, Long> hadoopInputFormat = setupHadoopInputFormat(inputFormat, Job.getInstance(), null);
		hadoopInputFormat.createInputSplits(2);

		verify(inputFormat, times(1)).getSplits(any(JobContext.class));
	}

	@Test
	public void testOpen() throws Exception {
		DummyInputFormat inputFormat = mock(DummyInputFormat.class);
		when(inputFormat.createRecordReader(any(InputSplit.class), any(TaskAttemptContext.class))).thenReturn(new DummyRecordReader());
		HadoopInputSplit inputSplit = mock(HadoopInputSplit.class);

		HadoopInputFormat<String, Long> hadoopInputFormat = setupHadoopInputFormat(inputFormat, Job.getInstance(), null);
		hadoopInputFormat.open(inputSplit);

		verify(inputFormat, times(1)).createRecordReader(any(InputSplit.class), any(TaskAttemptContext.class));
		assertThat(hadoopInputFormat.fetched, is(false));
	}

	@Test
	public void testClose() throws Exception {

		DummyRecordReader recordReader = mock(DummyRecordReader.class);

		HadoopInputFormat<String, Long> hadoopInputFormat = setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);
		hadoopInputFormat.close();

		verify(recordReader, times(1)).close();
	}

	@Test
	public void testCloseWithoutOpen() throws Exception {
		HadoopInputFormat<String, Long> hadoopInputFormat = new HadoopInputFormat<>(new DummyInputFormat(), String.class, Long.class, Job.getInstance());
		hadoopInputFormat.close();
	}

	@Test
	public void testFetchNextInitialState() throws Exception {
		DummyRecordReader recordReader = new DummyRecordReader();

		HadoopInputFormat<String, Long> hadoopInputFormat = setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);
		hadoopInputFormat.fetchNext();

		assertThat(hadoopInputFormat.fetched, is(true));
		assertThat(hadoopInputFormat.hasNext, is(false));
	}

	@Test
	public void testFetchNextRecordReaderHasNewValue() throws Exception {

		DummyRecordReader recordReader = mock(DummyRecordReader.class);
		when(recordReader.nextKeyValue()).thenReturn(true);

		HadoopInputFormat<String, Long> hadoopInputFormat = setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);
		hadoopInputFormat.fetchNext();

		assertThat(hadoopInputFormat.fetched, is(true));
		assertThat(hadoopInputFormat.hasNext, is(true));
	}

	@Test
	public void testFetchNextRecordReaderThrowsException() throws Exception {

		DummyRecordReader recordReader = mock(DummyRecordReader.class);
		when(recordReader.nextKeyValue()).thenThrow(new InterruptedException());

		HadoopInputFormat<String, Long> hadoopInputFormat = setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);

		exception.expect(IOException.class);
		hadoopInputFormat.fetchNext();

		assertThat(hadoopInputFormat.hasNext, is(true));
	}

	@Test
	public void checkTypeInformation() throws Exception {

		HadoopInputFormat<Void, Long> hadoopInputFormat = new HadoopInputFormat<>(
				new DummyVoidKeyInputFormat<Long>(), Void.class, Long.class, Job.getInstance());

		TypeInformation<Tuple2<Void, Long>> tupleType = hadoopInputFormat.getProducedType();
		TypeInformation<Tuple2<Void, Long>> expectedType = new TupleTypeInfo<>(BasicTypeInfo.VOID_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

		assertThat(tupleType.isTupleType(), is(true));
		assertThat(tupleType, is(equalTo(expectedType)));
	}

	private HadoopInputFormat<String, Long> setupHadoopInputFormat(InputFormat<String, Long> inputFormat, Job job,
																	RecordReader<String, Long> recordReader) {

		HadoopInputFormat<String, Long> hadoopInputFormat = new HadoopInputFormat<>(inputFormat,
				String.class, Long.class, job);
		hadoopInputFormat.recordReader = recordReader;

		return hadoopInputFormat;
	}

	private class DummyVoidKeyInputFormat<T> extends FileInputFormat<Void, T> {

		public DummyVoidKeyInputFormat() {}

		@Override
		public RecordReader<Void, T> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
			return null;
		}
	}

	private class DummyRecordReader extends RecordReader<String, Long> {

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return false;
		}

		@Override
		public String getCurrentKey() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public Long getCurrentValue() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void close() throws IOException {

		}
	}

	private class DummyInputFormat extends InputFormat<String, Long> {

		@Override
		public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
			return null;
		}

		@Override
		public RecordReader<String, Long> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
			return new DummyRecordReader();
		}
	}

	private class ConfigurableDummyInputFormat extends DummyInputFormat implements Configurable {

		@Override
		public void setConf(Configuration configuration) {}

		@Override
		public Configuration getConf() {
			return null;
		}
	}
}
