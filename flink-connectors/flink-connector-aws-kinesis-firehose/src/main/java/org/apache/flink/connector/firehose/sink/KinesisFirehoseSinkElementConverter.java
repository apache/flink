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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.model.Record;

/**
 * An implementation of the {@link ElementConverter} that uses the AWS Kinesis SDK v2. The user only
 * needs to provide a {@link SerializationSchema} of the {@code InputT} to transform it into a
 * {@link Record} that may be persisted.
 */
@Internal
public class KinesisFirehoseSinkElementConverter<InputT>
        implements ElementConverter<InputT, Record> {

    /** A serialization schema to specify how the input element should be serialized. */
    private final SerializationSchema<InputT> serializationSchema;

    private KinesisFirehoseSinkElementConverter(SerializationSchema<InputT> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Override
    public Record apply(InputT element, SinkWriter.Context context) {
        return Record.builder()
                .data(SdkBytes.fromByteArray(serializationSchema.serialize(element)))
                .build();
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the KinesisFirehoseSinkElementConverter. */
    public static class Builder<InputT> {

        private SerializationSchema<InputT> serializationSchema;

        public Builder<InputT> setSerializationSchema(
                SerializationSchema<InputT> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        public KinesisFirehoseSinkElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    serializationSchema,
                    "No SerializationSchema was supplied to the " + "KinesisFirehoseSink builder.");
            return new KinesisFirehoseSinkElementConverter<>(serializationSchema);
        }
    }
}
