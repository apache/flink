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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.connectors.dynamodb.util.DynamoDbAttributeValueUtils;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * An implementation of the {@link ElementConverter} that uses the AWS DynamoDb SDK v2. The user
 * needs to provide a {@link DynamoDbRequestConverter} of the {@code InputT} to transform it into a
 * {@link DynamoDbWriteRequest} that may be persisted.
 */
@Internal
public class DynamoDbSinkElementConverter<InputT>
        implements ElementConverter<InputT, DynamoDbWriteRequest> {

    private final DynamoDbRequestConverter<InputT> dynamoDbRequestConverter;

    public DynamoDbSinkElementConverter(DynamoDbRequestConverter<InputT> dynamoDbRequestConverter) {
        this.dynamoDbRequestConverter = dynamoDbRequestConverter;
    }

    @Override
    public DynamoDbWriteRequest apply(InputT element, SinkWriter.Context context) {
        DynamoDbRequest dynamoDbRequest = dynamoDbRequestConverter.apply(element);
        WriteRequest writeRequest = DynamoDbAttributeValueUtils.toWriteRequest(dynamoDbRequest);
        return new DynamoDbWriteRequest(dynamoDbRequest.tableName(), writeRequest);
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the DynamoDbSinkElementConverter. */
    public static class Builder<InputT> {

        private DynamoDbRequestConverter<InputT> dynamoDbRequestConverter;

        public Builder<InputT> setDynamoDbRequestConverter(
                DynamoDbRequestConverter<InputT> dynamoDbRequestConverter) {
            this.dynamoDbRequestConverter = dynamoDbRequestConverter;
            return this;
        }

        public DynamoDbSinkElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    dynamoDbRequestConverter,
                    "No DynamoDbRequestConverter was supplied to the " + "DynamoDbSink builder.");
            return new DynamoDbSinkElementConverter<>(dynamoDbRequestConverter);
        }
    }
}
