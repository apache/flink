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
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.model.Record;

import static org.assertj.core.api.Assertions.assertThat;

/** Covers construction and sanity checking of {@link KinesisFirehoseSinkElementConverter}. */
class KinesisFirehoseSinkElementConverterTest {

    @Test
    void elementConverterWillComplainASerializationSchemaIsNotSetIfBuildIsCalledWithoutIt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> KinesisFirehoseSinkElementConverter.<String>builder().build())
                .withMessageContaining(
                        "No SerializationSchema was supplied to the KinesisFirehoseSink builder.");
    }

    @Test
    void elementConverterUsesProvidedSchemaToSerializeRecord() {
        ElementConverter<String, Record> elementConverter =
                KinesisFirehoseSinkElementConverter.<String>builder()
                        .setSerializationSchema(new SimpleStringSchema())
                        .build();

        String testString = "{many hands make light work;";

        Record serializedRecord = elementConverter.apply(testString, null);
        byte[] serializedString = (new SimpleStringSchema()).serialize(testString);
        assertThat(serializedRecord.data()).isEqualTo(SdkBytes.fromByteArray(serializedString));
    }
}
