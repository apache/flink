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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link HadoopInputFormat}. */
class HadoopInputFormatTest {

    @Test
    void testConfigure() throws Exception {

        ConfigurableDummyInputFormat inputFormat = mock(ConfigurableDummyInputFormat.class);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                setupHadoopInputFormat(inputFormat, Job.getInstance(), null);
        hadoopInputFormat.configure(new org.apache.flink.configuration.Configuration());

        verify(inputFormat, times(1)).setConf(any(Configuration.class));
    }

    @Test
    void testCreateInputSplits() throws Exception {
        DummyInputFormat inputFormat = mock(DummyInputFormat.class);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                setupHadoopInputFormat(inputFormat, Job.getInstance(), null);
        hadoopInputFormat.createInputSplits(2);

        verify(inputFormat, times(1)).getSplits(any(JobContext.class));
    }

    @Test
    void testOpen() throws Exception {
        DummyInputFormat inputFormat = mock(DummyInputFormat.class);
        when(inputFormat.createRecordReader(
                        nullable(InputSplit.class), any(TaskAttemptContext.class)))
                .thenReturn(new DummyRecordReader());
        HadoopInputSplit inputSplit = mock(HadoopInputSplit.class);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                setupHadoopInputFormat(inputFormat, Job.getInstance(), null);
        hadoopInputFormat.open(inputSplit);

        verify(inputFormat, times(1))
                .createRecordReader(nullable(InputSplit.class), any(TaskAttemptContext.class));
        assertThat(hadoopInputFormat.fetched).isFalse();
    }

    @Test
    void testClose() throws Exception {

        DummyRecordReader recordReader = mock(DummyRecordReader.class);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);
        hadoopInputFormat.close();

        verify(recordReader, times(1)).close();
    }

    @Test
    void testCloseWithoutOpen() throws Exception {
        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(
                        new DummyInputFormat(), String.class, Long.class, Job.getInstance());
        hadoopInputFormat.close();
    }

    @Test
    void testFetchNextInitialState() throws Exception {
        DummyRecordReader recordReader = new DummyRecordReader();

        HadoopInputFormat<String, Long> hadoopInputFormat =
                setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);
        hadoopInputFormat.fetchNext();

        assertThat(hadoopInputFormat.fetched).isTrue();
        assertThat(hadoopInputFormat.hasNext).isFalse();
    }

    @Test
    void testFetchNextRecordReaderHasNewValue() throws Exception {

        DummyRecordReader recordReader = mock(DummyRecordReader.class);
        when(recordReader.nextKeyValue()).thenReturn(true);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);
        hadoopInputFormat.fetchNext();

        assertThat(hadoopInputFormat.fetched).isTrue();
        assertThat(hadoopInputFormat.hasNext).isTrue();
    }

    @Test
    void testFetchNextRecordReaderThrowsException() throws Exception {

        DummyRecordReader recordReader = mock(DummyRecordReader.class);
        when(recordReader.nextKeyValue()).thenThrow(new InterruptedException());

        HadoopInputFormat<String, Long> hadoopInputFormat =
                setupHadoopInputFormat(new DummyInputFormat(), Job.getInstance(), recordReader);

        assertThatThrownBy(hadoopInputFormat::fetchNext)
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(InterruptedException.class);

        assertThat(hadoopInputFormat.hasNext).isFalse();
        assertThat(hadoopInputFormat.fetched).isTrue();
    }

    @Test
    void checkTypeInformation() throws Exception {

        HadoopInputFormat<Void, Long> hadoopInputFormat =
                new HadoopInputFormat<>(
                        new DummyVoidKeyInputFormat<Long>(),
                        Void.class,
                        Long.class,
                        Job.getInstance());

        TypeInformation<Tuple2<Void, Long>> tupleType = hadoopInputFormat.getProducedType();
        TypeInformation<Tuple2<Void, Long>> expectedType =
                new TupleTypeInfo<>(BasicTypeInfo.VOID_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        assertThat(tupleType.isTupleType()).isTrue();
        assertThat(tupleType).isEqualTo(expectedType);
    }

    private HadoopInputFormat<String, Long> setupHadoopInputFormat(
            InputFormat<String, Long> inputFormat,
            Job job,
            RecordReader<String, Long> recordReader) {

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(inputFormat, String.class, Long.class, job);
        hadoopInputFormat.recordReader = recordReader;

        return hadoopInputFormat;
    }

    private class DummyVoidKeyInputFormat<T> extends FileInputFormat<Void, T> {

        public DummyVoidKeyInputFormat() {}

        @Override
        public RecordReader<Void, T> createRecordReader(
                InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {
            return null;
        }
    }

    private class DummyRecordReader extends RecordReader<String, Long> {

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {}

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
        public void close() throws IOException {}
    }

    private class DummyInputFormat extends InputFormat<String, Long> {

        @Override
        public List<InputSplit> getSplits(JobContext jobContext)
                throws IOException, InterruptedException {
            return null;
        }

        @Override
        public RecordReader<String, Long> createRecordReader(
                InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {
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
