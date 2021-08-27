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

package org.apache.flink.api.java.hadoop.mapred;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link HadoopInputFormat}. */
public class HadoopInputFormatTest {

    @Test
    public void testConfigureWithConfigurableInstance() {
        ConfigurableDummyInputFormat inputFormat = mock(ConfigurableDummyInputFormat.class);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(inputFormat, String.class, Long.class, new JobConf());
        verify(inputFormat, times(1)).setConf(any(JobConf.class));

        hadoopInputFormat.configure(new org.apache.flink.configuration.Configuration());
        verify(inputFormat, times(2)).setConf(any(JobConf.class));
    }

    @Test
    public void testConfigureWithJobConfigurableInstance() {
        JobConfigurableDummyInputFormat inputFormat = mock(JobConfigurableDummyInputFormat.class);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(inputFormat, String.class, Long.class, new JobConf());
        verify(inputFormat, times(1)).configure(any(JobConf.class));

        hadoopInputFormat.configure(new org.apache.flink.configuration.Configuration());
        verify(inputFormat, times(2)).configure(any(JobConf.class));
    }

    @Test
    public void testOpenClose() throws Exception {
        DummyRecordReader recordReader = mock(DummyRecordReader.class);
        DummyInputFormat inputFormat = mock(DummyInputFormat.class);
        when(inputFormat.getRecordReader(
                        any(InputSplit.class), any(JobConf.class), any(Reporter.class)))
                .thenReturn(recordReader);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(inputFormat, String.class, Long.class, new JobConf());
        hadoopInputFormat.open(getHadoopInputSplit());

        verify(inputFormat, times(1))
                .getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class));
        verify(recordReader, times(1)).createKey();
        verify(recordReader, times(1)).createValue();

        assertThat(hadoopInputFormat.fetched, is(false));

        hadoopInputFormat.close();
        verify(recordReader, times(1)).close();
    }

    @Test
    public void testOpenWithConfigurableReader() throws Exception {
        ConfigurableDummyRecordReader recordReader = mock(ConfigurableDummyRecordReader.class);
        DummyInputFormat inputFormat = mock(DummyInputFormat.class);
        when(inputFormat.getRecordReader(
                        any(InputSplit.class), any(JobConf.class), any(Reporter.class)))
                .thenReturn(recordReader);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(inputFormat, String.class, Long.class, new JobConf());
        hadoopInputFormat.open(getHadoopInputSplit());

        verify(inputFormat, times(1))
                .getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class));
        verify(recordReader, times(1)).setConf(any(JobConf.class));
        verify(recordReader, times(1)).createKey();
        verify(recordReader, times(1)).createValue();

        assertThat(hadoopInputFormat.fetched, is(false));
    }

    @Test
    public void testCreateInputSplits() throws Exception {

        FileSplit[] result = new FileSplit[1];
        result[0] = getFileSplit();
        DummyInputFormat inputFormat = mock(DummyInputFormat.class);
        when(inputFormat.getSplits(any(JobConf.class), anyInt())).thenReturn(result);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(inputFormat, String.class, Long.class, new JobConf());
        hadoopInputFormat.createInputSplits(2);

        verify(inputFormat, times(1)).getSplits(any(JobConf.class), anyInt());
    }

    @Test
    public void testReachedEndWithElementsRemaining() throws IOException {

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(
                        new DummyInputFormat(), String.class, Long.class, new JobConf());
        hadoopInputFormat.fetched = true;
        hadoopInputFormat.hasNext = true;

        assertThat(hadoopInputFormat.reachedEnd(), is(false));
    }

    @Test
    public void testReachedEndWithNoElementsRemaining() throws IOException {
        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(
                        new DummyInputFormat(), String.class, Long.class, new JobConf());
        hadoopInputFormat.fetched = true;
        hadoopInputFormat.hasNext = false;

        assertThat(hadoopInputFormat.reachedEnd(), is(true));
    }

    @Test
    public void testFetchNext() throws IOException {
        DummyRecordReader recordReader = mock(DummyRecordReader.class);
        when(recordReader.next(nullable(String.class), nullable(Long.class))).thenReturn(true);

        DummyInputFormat inputFormat = mock(DummyInputFormat.class);
        when(inputFormat.getRecordReader(
                        any(InputSplit.class), any(JobConf.class), any(Reporter.class)))
                .thenReturn(recordReader);

        HadoopInputFormat<String, Long> hadoopInputFormat =
                new HadoopInputFormat<>(inputFormat, String.class, Long.class, new JobConf());
        hadoopInputFormat.open(getHadoopInputSplit());
        hadoopInputFormat.fetchNext();

        verify(recordReader, times(1)).next(nullable(String.class), anyLong());
        assertThat(hadoopInputFormat.hasNext, is(true));
        assertThat(hadoopInputFormat.fetched, is(true));
    }

    @Test
    public void checkTypeInformation() throws Exception {
        HadoopInputFormat<Void, Long> hadoopInputFormat =
                new HadoopInputFormat<>(
                        new DummyVoidKeyInputFormat<Long>(), Void.class, Long.class, new JobConf());

        TypeInformation<Tuple2<Void, Long>> tupleType = hadoopInputFormat.getProducedType();
        TypeInformation<Tuple2<Void, Long>> expectedType =
                new TupleTypeInfo<>(BasicTypeInfo.VOID_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        assertThat(tupleType.isTupleType(), is(true));
        assertThat(tupleType, is(equalTo(expectedType)));
    }

    @Test
    public void testCloseWithoutOpen() throws Exception {
        HadoopInputFormat<Void, Long> hadoopInputFormat =
                new HadoopInputFormat<>(
                        new DummyVoidKeyInputFormat<Long>(), Void.class, Long.class, new JobConf());
        hadoopInputFormat.close();
    }

    private HadoopInputSplit getHadoopInputSplit() {
        return new HadoopInputSplit(1, getFileSplit(), new JobConf());
    }

    private FileSplit getFileSplit() {
        return new FileSplit(new Path("path"), 1, 2, new String[] {});
    }

    private class DummyVoidKeyInputFormat<T> extends FileInputFormat<Void, T> {

        public DummyVoidKeyInputFormat() {}

        @Override
        public org.apache.hadoop.mapred.RecordReader<Void, T> getRecordReader(
                org.apache.hadoop.mapred.InputSplit inputSplit, JobConf jobConf, Reporter reporter)
                throws IOException {
            return null;
        }
    }

    private class DummyRecordReader implements RecordReader<String, Long> {

        @Override
        public float getProgress() throws IOException {
            return 0;
        }

        @Override
        public boolean next(String s, Long aLong) throws IOException {
            return false;
        }

        @Override
        public String createKey() {
            return null;
        }

        @Override
        public Long createValue() {
            return null;
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {}
    }

    private class ConfigurableDummyRecordReader
            implements RecordReader<String, Long>, Configurable {

        @Override
        public void setConf(Configuration configuration) {}

        @Override
        public Configuration getConf() {
            return null;
        }

        @Override
        public boolean next(String s, Long aLong) throws IOException {
            return false;
        }

        @Override
        public String createKey() {
            return null;
        }

        @Override
        public Long createValue() {
            return null;
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {}

        @Override
        public float getProgress() throws IOException {
            return 0;
        }
    }

    private class DummyInputFormat implements InputFormat<String, Long> {

        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            return new InputSplit[0];
        }

        @Override
        public RecordReader<String, Long> getRecordReader(
                InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            return null;
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

    private class JobConfigurableDummyInputFormat extends DummyInputFormat
            implements JobConfigurable {

        @Override
        public void configure(JobConf jobConf) {}
    }
}
