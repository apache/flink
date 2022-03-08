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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

/**
 * An implementation of the {@link ElementConverter} that uses the AWS Kinesis SDK v2. The user only
 * needs to provide a {@link SerializationSchema} of the {@code InputT} and a {@link
 * PartitionKeyGenerator} lambda to transform the input element into a String.
 */
@Internal
public class KinesisStreamsSinkElementConverter<InputT>
        implements ElementConverter<InputT, PutRecordsRequestEntry> {

    /** A serialization schema to specify how the input element should be serialized. */
    private final SerializationSchema<InputT> serializationSchema;

    /**
     * A partition key generator functional interface that produces a string from the input element.
     */
    private final PartitionKeyGenerator<InputT> partitionKeyGenerator;

    private KinesisStreamsSinkElementConverter(
            SerializationSchema<InputT> serializationSchema,
            PartitionKeyGenerator<InputT> partitionKeyGenerator) {
        this.serializationSchema = serializationSchema;
        this.partitionKeyGenerator = partitionKeyGenerator;
    }

    @Override
    public PutRecordsRequestEntry apply(InputT element, SinkWriter.Context context) {
        return PutRecordsRequestEntry.builder()
                .data(SdkBytes.fromByteArray(serializationSchema.serialize(element)))
                .partitionKey(partitionKeyGenerator.apply(element))
                .build();
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the KinesisStreamsSinkElementConverter. */
    public static class Builder<InputT> {

        private SerializationSchema<InputT> serializationSchema;
        private PartitionKeyGenerator<InputT> partitionKeyGenerator;

        public Builder<InputT> setSerializationSchema(
                SerializationSchema<InputT> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        public Builder<InputT> setPartitionKeyGenerator(
                PartitionKeyGenerator<InputT> partitionKeyGenerator) {
            this.partitionKeyGenerator = partitionKeyGenerator;
            return this;
        }

        public KinesisStreamsSinkElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    serializationSchema,
                    "No SerializationSchema was supplied to the "
                            + "KinesisStreamsSinkElementConverter builder.");
            Preconditions.checkNotNull(
                    partitionKeyGenerator,
                    "No PartitionKeyGenerator lambda was supplied to the "
                            + "KinesisStreamsSinkElementConverter builder.");
            return new KinesisStreamsSinkElementConverter<>(
                    serializationSchema, partitionKeyGenerator);
        }
    }
}
