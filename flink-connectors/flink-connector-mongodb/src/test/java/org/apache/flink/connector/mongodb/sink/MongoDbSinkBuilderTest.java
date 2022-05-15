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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Covers construction, defaults and sanity checking of MongoDbSinkBuilder. */
public class MongoDbSinkBuilderTest {

    private static final MongoDbWriteOperationConverter<String> MONGO_DB_WRITE_OPERATION_CONVERTER =
            s ->
                    new MongoDbReplaceOneOperation(
                            ImmutableMap.of("filter_key", s),
                            ImmutableMap.of("replacement_key", s));

    @Test
    public void databaseNameOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setCollectionName("collectionName")
                                        .setMongoDbWriteOperationConverter(
                                                MONGO_DB_WRITE_OPERATION_CONVERTER)
                                        .build())
                .withMessageContaining(
                        "The database name must not be null when initializing the MongoDB Sink.");
    }

    @Test
    public void databaseNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("")
                                        .setCollectionName("collectionName")
                                        .setMongoDbWriteOperationConverter(
                                                MONGO_DB_WRITE_OPERATION_CONVERTER)
                                        .build())
                .withMessageContaining(
                        "The database name must be set when initializing the MongoDB Sink.");
    }

    @Test
    public void collectionNameOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("databaseName")
                                        .setMongoDbWriteOperationConverter(
                                                MONGO_DB_WRITE_OPERATION_CONVERTER)
                                        .build())
                .withMessageContaining(
                        "The collection name must not be null when initializing the MongoDB Sink.");
    }

    @Test
    public void collectionNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("databaseName")
                                        .setCollectionName("")
                                        .setMongoDbWriteOperationConverter(
                                                MONGO_DB_WRITE_OPERATION_CONVERTER)
                                        .build())
                .withMessageContaining(
                        "The collection name must be set when initializing the MongoDB Sink.");
    }

    @Test
    public void mongoDbWriteOperationConverterMustBeSetWhenSinkIsBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("databaseName")
                                        .setCollectionName("collectionName")
                                        .build())
                .withMessageContaining(
                        "No MongoDbWriteOperationConverter was supplied to the MongoDbSinkElementConverter builder.");
    }
}
