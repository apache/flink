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

package org.apache.flink.streaming.connectors.dynamodb.sink.examples;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbSink;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbWriteRequest;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.utils.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * An example application demonstrating how to use the {@link DynamoDbSink} to sink into DynamoDb.
 */
public class SinkIntoDynamoDb {

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<String> fromGen =
                env.fromSequence(1, 10_000_000L)
                        .map(Object::toString)
                        .returns(String.class)
                        .map(data -> mapper.writeValueAsString(ImmutableMap.of("data", data)));

        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, "your-region-here");

        DynamoDbSink<String> dynamoDbSink =
                DynamoDbSink.<String>builder()
                        .setElementConverter(new DummyElementConverter())
                        .setMaxBatchSize(20)
                        .setDynamoDbProperties(sinkProperties)
                        .build();

        fromGen.sinkTo(dynamoDbSink);

        env.execute("DynamoDb Async Sink Example Program");
    }

    private static class DummyElementConverter
            implements ElementConverter<String, DynamoDbWriteRequest> {

        @Override
        public DynamoDbWriteRequest apply(String element, SinkWriter.Context context) {
            final Map<String, AttributeValue> map = new HashMap<>();
            map.put("your-key", AttributeValue.builder().s(element).build());
            return new DynamoDbWriteRequest(
                    "your-table-name",
                    WriteRequest.builder()
                            .putRequest(PutRequest.builder().item(map).build())
                            .build());
        }
    }
}
