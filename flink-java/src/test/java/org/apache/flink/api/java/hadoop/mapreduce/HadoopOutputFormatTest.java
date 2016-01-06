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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HadoopOutputFormatTest {

    private static final String PATH = "an/ignored/file/";
    private Map<String, Long> map;

    @Test
    public void testWriteRecord() {
        OutputFormat<String, Long> dummyOutputFormat = new DummyOutputFormat();
        String key = "Test";
        Long value = 1L;
        map = new HashMap<>();
        map.put(key, 0L);
        try {
            Job job = Job.getInstance();
            Tuple2<String, Long> tuple = new Tuple2<>();
            tuple.setFields(key, value);
            HadoopOutputFormat<String, Long> hadoopOutputFormat = new HadoopOutputFormat<>(dummyOutputFormat, job);

            hadoopOutputFormat.recordWriter = new DummyRecordWriter();
            hadoopOutputFormat.writeRecord(tuple);

            Long expected = map.get(key);
            assertEquals(expected, value);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testOpen() {
        OutputFormat<String, Long> dummyOutputFormat = new DummyOutputFormat();
        try {
            Job job = Job.getInstance();
            HadoopOutputFormat<String, Long> hadoopOutputFormat = new HadoopOutputFormat<>(dummyOutputFormat, job);

            hadoopOutputFormat.recordWriter = new DummyRecordWriter();
            hadoopOutputFormat.open(1, 4);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testClose() {
        OutputFormat<String, Long> dummyOutputFormat = new DummyOutputFormat();
        try {
            Job job = Job.getInstance();
            HadoopOutputFormat<String, Long> hadoopOutputFormat = new HadoopOutputFormat<>(dummyOutputFormat, job);

            hadoopOutputFormat.recordWriter = new DummyRecordWriter();

            final OutputCommitter outputCommitter = Mockito.mock(OutputCommitter.class);
            Mockito.when(outputCommitter.needsTaskCommit(Mockito.any(TaskAttemptContext.class))).thenReturn(true);
            Mockito.doNothing().when(outputCommitter).commitTask(Mockito.any(TaskAttemptContext.class));
            hadoopOutputFormat.outputCommitter = outputCommitter;
            hadoopOutputFormat.configuration = new Configuration();
            hadoopOutputFormat.configuration.set("mapred.output.dir", PATH);

            hadoopOutputFormat.close();
        } catch (IOException e) {
            fail();
        }
    }


    class DummyRecordWriter extends RecordWriter<String, Long> {
        @Override
        public void write(String key, Long value) throws IOException, InterruptedException {
            map.put(key, value);
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
            Mockito.doNothing().when(outputCommitter).setupJob(Mockito.any(JobContext.class));

            return outputCommitter;
        }
    }
}
