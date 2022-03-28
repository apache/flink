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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.firehose.model.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Covers construction, defaults and sanity checking of {@link KinesisFirehoseSinkWriter}. */
public class KinesisFirehoseSinkWriterTest {

    private KinesisFirehoseSinkWriter<String> sinkWriter;

    private static final ElementConverter<String, Record> ELEMENT_CONVERTER_PLACEHOLDER =
            KinesisFirehoseSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @Before
    public void setup() {
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        Properties sinkProperties = AWSServicesTestUtils.createConfig("https://fake_aws_endpoint");
        sinkWriter =
                new KinesisFirehoseSinkWriter<>(
                        ELEMENT_CONVERTER_PLACEHOLDER,
                        sinkInitContext,
                        50,
                        16,
                        10000,
                        4 * 1024 * 1024,
                        5000,
                        1000 * 1024,
                        true,
                        "streamName",
                        sinkProperties);
    }

    @Test
    public void getSizeInBytesReturnsSizeOfBlobBeforeBase64Encoding() {
        String testString = "{many hands make light work;";
        Record record = Record.builder().data(SdkBytes.fromUtf8String(testString)).build();
        assertThat(sinkWriter.getSizeInBytes(record))
                .isEqualTo(testString.getBytes(StandardCharsets.US_ASCII).length);
    }

    @Test
    public void getNumRecordsOutErrorsCounterRecordsCorrectNumberOfFailures()
            throws IOException, InterruptedException {
        TestSinkInitContext ctx = new TestSinkInitContext();
        KinesisFirehoseSink<String> kinesisFirehoseSink =
                new KinesisFirehoseSink<>(
                        ELEMENT_CONVERTER_PLACEHOLDER,
                        12,
                        16,
                        10000,
                        4 * 1024 * 1024L,
                        5000L,
                        1000 * 1024L,
                        true,
                        "test-stream",
                        AWSServicesTestUtils.createConfig("https://localhost"));
        SinkWriter<String> writer = kinesisFirehoseSink.createWriter(ctx);

        for (int i = 0; i < 12; i++) {
            writer.write("data_bytes", null);
        }
        assertThatExceptionOfType(CompletionException.class)
                .isThrownBy(() -> writer.flush(true))
                .withCauseInstanceOf(SdkClientException.class)
                .withMessageContaining(
                        "Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:443");
        assertThat(ctx.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(12);
        assertThat(ctx.metricGroup().getNumRecordsSendErrorsCounter().getCount()).isEqualTo(12);
    }
}
