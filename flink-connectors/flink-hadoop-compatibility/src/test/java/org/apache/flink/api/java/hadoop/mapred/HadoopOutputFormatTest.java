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

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link HadoopOutputFormat}. */
public class HadoopOutputFormatTest {

    @Test
    public void testOpen() throws Exception {

        OutputFormat<String, Long> dummyOutputFormat = mock(DummyOutputFormat.class);
        DummyOutputCommitter outputCommitter = mock(DummyOutputCommitter.class);
        JobConf jobConf = Mockito.spy(new JobConf());
        when(jobConf.getOutputCommitter()).thenReturn(outputCommitter);

        HadoopOutputFormat<String, Long> outputFormat =
                new HadoopOutputFormat<>(dummyOutputFormat, jobConf);

        outputFormat.open(1, 1);

        verify(jobConf, times(2)).getOutputCommitter();
        verify(outputCommitter, times(1)).setupJob(any(JobContext.class));
        verify(dummyOutputFormat, times(1))
                .getRecordWriter(
                        nullable(FileSystem.class),
                        any(JobConf.class),
                        anyString(),
                        any(Progressable.class));
    }

    @Test
    public void testConfigureWithConfigurable() {
        ConfigurableDummyOutputFormat dummyOutputFormat = mock(ConfigurableDummyOutputFormat.class);
        JobConf jobConf = mock(JobConf.class);

        HadoopOutputFormat<String, Long> outputFormat =
                new HadoopOutputFormat<>(dummyOutputFormat, jobConf);

        outputFormat.configure(Matchers.<org.apache.flink.configuration.Configuration>any());

        verify(dummyOutputFormat, times(1)).setConf(any(Configuration.class));
    }

    @Test
    public void testConfigureWithJobConfigurable() {
        JobConfigurableDummyOutputFormat dummyOutputFormat =
                mock(JobConfigurableDummyOutputFormat.class);
        JobConf jobConf = mock(JobConf.class);

        HadoopOutputFormat<String, Long> outputFormat =
                new HadoopOutputFormat<>(dummyOutputFormat, jobConf);

        outputFormat.configure(Matchers.<org.apache.flink.configuration.Configuration>any());

        verify(dummyOutputFormat, times(1)).configure(any(JobConf.class));
    }

    @Test
    public void testCloseWithTaskCommit() throws Exception {
        OutputFormat<String, Long> dummyOutputFormat = mock(DummyOutputFormat.class);
        DummyOutputCommitter outputCommitter = mock(DummyOutputCommitter.class);
        when(outputCommitter.needsTaskCommit(nullable(TaskAttemptContext.class))).thenReturn(true);
        DummyRecordWriter recordWriter = mock(DummyRecordWriter.class);
        JobConf jobConf = mock(JobConf.class);

        HadoopOutputFormat<String, Long> outputFormat =
                new HadoopOutputFormat<>(dummyOutputFormat, jobConf);
        outputFormat.recordWriter = recordWriter;
        outputFormat.outputCommitter = outputCommitter;

        outputFormat.close();

        verify(recordWriter, times(1)).close(nullable(Reporter.class));
        verify(outputCommitter, times(1)).commitTask(nullable(TaskAttemptContext.class));
    }

    @Test
    public void testCloseWithoutTaskCommit() throws Exception {
        OutputFormat<String, Long> dummyOutputFormat = mock(DummyOutputFormat.class);
        DummyOutputCommitter outputCommitter = mock(DummyOutputCommitter.class);
        when(outputCommitter.needsTaskCommit(any(TaskAttemptContext.class))).thenReturn(false);
        DummyRecordWriter recordWriter = mock(DummyRecordWriter.class);
        JobConf jobConf = mock(JobConf.class);

        HadoopOutputFormat<String, Long> outputFormat =
                new HadoopOutputFormat<>(dummyOutputFormat, jobConf);
        outputFormat.recordWriter = recordWriter;
        outputFormat.outputCommitter = outputCommitter;

        outputFormat.close();

        verify(recordWriter, times(1)).close(any(Reporter.class));
        verify(outputCommitter, times(0)).commitTask(any(TaskAttemptContext.class));
    }

    @Test
    public void testWriteRecord() throws Exception {
        OutputFormat<String, Long> dummyOutputFormat = mock(DummyOutputFormat.class);
        DummyRecordWriter recordWriter = mock(DummyRecordWriter.class);
        JobConf jobConf = mock(JobConf.class);

        HadoopOutputFormat<String, Long> outputFormat =
                new HadoopOutputFormat<>(dummyOutputFormat, jobConf);
        outputFormat.recordWriter = recordWriter;

        outputFormat.writeRecord(new Tuple2<>("key", 1L));

        verify(recordWriter, times(1)).write(anyString(), anyLong());
    }

    @Test
    public void testFinalizeGlobal() throws Exception {
        OutputFormat<String, Long> dummyOutputFormat = mock(DummyOutputFormat.class);
        DummyOutputCommitter outputCommitter = mock(DummyOutputCommitter.class);
        JobConf jobConf = Mockito.spy(new JobConf());
        when(jobConf.getOutputCommitter()).thenReturn(outputCommitter);

        HadoopOutputFormat<String, Long> outputFormat =
                new HadoopOutputFormat<>(dummyOutputFormat, jobConf);

        outputFormat.finalizeGlobal(1);

        verify(outputCommitter, times(1)).commitJob(any(JobContext.class));
    }

    private class DummyOutputFormat implements OutputFormat<String, Long> {

        @Override
        public RecordWriter<String, Long> getRecordWriter(
                FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
                throws IOException {
            return null;
        }

        @Override
        public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {}
    }

    private class ConfigurableDummyOutputFormat extends DummyOutputFormat implements Configurable {

        @Override
        public void setConf(Configuration configuration) {}

        @Override
        public Configuration getConf() {
            return null;
        }
    }

    private class JobConfigurableDummyOutputFormat extends DummyOutputFormat
            implements JobConfigurable {

        @Override
        public void configure(JobConf jobConf) {}
    }

    private class DummyOutputCommitter extends OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) throws IOException {}

        @Override
        public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {}

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {}

        @Override
        public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {}
    }

    private class DummyRecordWriter implements RecordWriter<String, Long> {

        @Override
        public void write(String s, Long aLong) throws IOException {}

        @Override
        public void close(Reporter reporter) throws IOException {}
    }
}
