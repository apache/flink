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

    private static final DocumentGenerator<String> DOCUMENT_GENERATOR =
            element -> ImmutableMap.of("k1", "v1");
    private static final FilterGenerator<String> FILTER_GENERATOR =
            element -> ImmutableMap.of("k2", "v2");

    @Test
    public void streamNameOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setFilterGenerator(FILTER_GENERATOR)
                                        .setDocumentGenerator(DOCUMENT_GENERATOR)
                                        .build())
                .withMessageContaining(
                        "The database name must not be null when initializing the MongoDb Sink.");
    }

    @Test
    public void databaseNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("")
                                        .setCollectionName("collection")
                                        .setFilterGenerator(FILTER_GENERATOR)
                                        .setDocumentGenerator(DOCUMENT_GENERATOR)
                                        .build())
                .withMessageContaining(
                        "The database name must be set when initializing the MongoDb Sink.");
    }

    @Test
    public void collectionNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("database")
                                        .setCollectionName("")
                                        .setFilterGenerator(FILTER_GENERATOR)
                                        .setDocumentGenerator(DOCUMENT_GENERATOR)
                                        .build())
                .withMessageContaining(
                        "The collection name must be set when initializing the MongoDb Sink.");
    }

    @Test
    public void documentGeneratorMustBeSetWhenSinkIsBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("database")
                                        .setCollectionName("collectionName")
                                        .setFilterGenerator(FILTER_GENERATOR)
                                        .build())
                .withMessageContaining(
                        "No DocumentGenerator was supplied to the MongoDbSinkElementConverter builder.");
    }

    @Test
    public void filterGeneratorMustBeSetWhenSinkIsBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                MongoDbSink.<String>builder()
                                        .setDatabaseName("database")
                                        .setCollectionName("collectionName")
                                        .setDocumentGenerator(DOCUMENT_GENERATOR)
                                        .build())
                .withMessageContaining(
                        "No FilterGenerator lambda was supplied to the MongoDbSinkElementConverter builder.");
    }
}
