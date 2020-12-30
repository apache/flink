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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;

import static org.junit.Assert.fail;

/** {@link ContinuousFileReaderOperator} test. */
public class ContinuousFileReaderOperatorTest {

    @Test(expected = ExpectedTestException.class)
    public void testExceptionRethrownFromClose() throws Exception {
        OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> harness =
                createHarness(failingFormat());
        harness.getExecutionConfig().setAutoWatermarkInterval(10);
        harness.setTimeCharacteristic(TimeCharacteristic.IngestionTime);
        try (OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> tester =
                harness) {
            tester.open();
        }
    }

    @Test(expected = ExpectedTestException.class)
    public void testExceptionRethrownFromProcessElement() throws Exception {
        OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> harness =
                createHarness(failingFormat());
        harness.getExecutionConfig().setAutoWatermarkInterval(10);
        harness.setTimeCharacteristic(TimeCharacteristic.IngestionTime);
        try (OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> tester =
                harness) {
            tester.open();
            tester.processElement(
                    new StreamRecord<>(
                            new TimestampedFileInputSplit(
                                    0L, 1, new Path(), 0L, 0L, new String[] {})));
            for (Mail m : harness.getTaskMailbox().drain()) {
                m.run();
            }
            fail("should throw from processElement");
        }
    }

    private FileInputFormat<String> failingFormat() {
        return new FileInputFormat<String>() {
            @Override
            public boolean reachedEnd() {
                return false;
            }

            @Override
            public String nextRecord(String reuse) {
                throw new ExpectedTestException();
            }

            @Override
            public void open(FileInputSplit fileSplit) {
                throw new ExpectedTestException();
            }

            @Override
            public void close() {
                throw new ExpectedTestException();
            }

            @Override
            public void configure(Configuration parameters) {}
        };
    }

    private <T> OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, T> createHarness(
            FileInputFormat<T> format) throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        return new OneInputStreamOperatorTestHarness<>(
                new ContinuousFileReaderOperatorFactory<>(
                        format, TypeExtractor.getInputFormatTypes(format), config),
                TypeExtractor.getForClass(TimestampedFileInputSplit.class)
                        .createSerializer(config));
    }
}
