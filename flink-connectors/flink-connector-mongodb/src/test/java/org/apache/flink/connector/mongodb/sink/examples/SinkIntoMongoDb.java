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

package org.apache.flink.connector.mongodb.sink.examples;

import org.apache.flink.connector.mongodb.sink.MongoDbInsertOneOperation;
import org.apache.flink.connector.mongodb.sink.MongoDbSink;
import org.apache.flink.connector.mongodb.util.MongoDbConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

/**
 * An example application demonstrating how to use the {@link MongoDbSink} to sink into MongoDB.
 *
 * <p>The {@link com.mongodb.reactivestreams.client.MongoClient} used here may be configured in the
 * standard way for the MongoDB driver reactive streams 4.6.x.
 */
public class SinkIntoMongoDb {

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
        sinkProperties.put(
                MongoDbConfigConstants.APPLY_CONNECTION_STRING, "your-connection-string-here");

        MongoDbSink<String> mongoDbSink =
                MongoDbSink.<String>builder()
                        .setMongoDbWriteOperationConverter(
                                element ->
                                        new MongoDbInsertOneOperation(
                                                ImmutableMap.of(
                                                        "your-key-here", "your-value-here")))
                        .setDatabaseName("your-database-name")
                        .setCollectionName("your-collection-name")
                        .setMaxBatchSize(20)
                        .setMongoProperties(sinkProperties)
                        .build();

        fromGen.sinkTo(mongoDbSink);

        env.execute("MongoDB Async Sink Example Program");
    }
}
