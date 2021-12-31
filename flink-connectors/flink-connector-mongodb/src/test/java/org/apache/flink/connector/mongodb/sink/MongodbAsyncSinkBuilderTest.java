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

import org.apache.flink.connector.base.sink.writer.ElementConverter;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version.Main;
import de.flapdoodle.embed.process.runtime.Network;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Covers construction, defaults and sanity checking of MongodbAsyncSinkBuilder. */
public class MongodbAsyncSinkBuilderTest {

    protected MongodExecutable mongodExe;
    protected MongodProcess mongod;
    protected MongoClient mongo;

    // these can be overridden by subclasses
    private static final String MONGODB_HOST = "127.0.0.1";
    private static final int MONGODB_PORT = 27018;
    private static final String DATABASE_NAME = "testdb";
    private static final String COLLECTION = "testcoll";
    private static final String CONNECT_STRING =
            String.format("mongodb://%s:%d/%s", MONGODB_HOST, MONGODB_PORT, DATABASE_NAME);

    @Before
    public void before() throws Exception {
        MongodStarter starter = MongodStarter.getDefaultInstance();
        MongodConfig mongodConfig =
                MongodConfig.builder()
                        .version(Main.V4_0)
                        .net(new Net(MONGODB_HOST, MONGODB_PORT, Network.localhostIsIPv6()))
                        .build();
        this.mongodExe = starter.prepare(mongodConfig);
        this.mongod = mongodExe.start();
        this.mongo = MongoClients.create(CONNECT_STRING);
    }

    @After
    public void after() throws Exception {
        if (this.mongo != null) {
            mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).drop();
            mongo.close();
        }
        if (this.mongod != null) {
            this.mongod.stop();
            this.mongodExe.stop();
        }
    }

    private static final ElementConverter<String, Document> ELEMENT_CONVERTER_PLACEHOLDER =
            MongodbAsyncSinkElementConverter.<String>builder()
                    .setSerializationSchema(
                            new DocumentSerializer<String>() {
                                @Override
                                public Document serialize(String str) {
                                    Document document = new Document();
                                    return document.append("_id", str);
                                }
                            })
                    .build();

    @Test
    public void elementConverterOfSinkMustBeSetWhenBuilt() {
        Throwable thrown =
                assertThrows(NullPointerException.class, () -> MongodbAsyncSink.builder().build());
        assertEquals(
                "ElementConverter must be not null when initilizing the AsyncSinkBase.",
                thrown.getMessage());
    }

    @Test
    public void databaseOfSinkMustBeSetWhenBuilt() {
        Throwable thrown =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                MongodbAsyncSink.<String>builder()
                                        .setCollection(COLLECTION)
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .build());
        assertEquals(
                "The databaseName must not be null when initializing the Mongodb Sink.",
                thrown.getMessage());
    }

    @Test
    public void collectionNameOfSinkMustBeSetWhenBuilt() {
        Throwable thrown =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                MongodbAsyncSink.<String>builder()
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .setDatabase(DATABASE_NAME)
                                        .setFailOnError(false)
                                        .build());
        assertEquals(
                "The collectionName must not be null when initializing the Mongodb Sink.",
                thrown.getMessage());
    }

    @Test
    public void databaseNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                MongodbAsyncSink.<String>builder()
                                        .setCollection(DATABASE_NAME)
                                        .setDatabase("")
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .build())
                .withMessageContaining(
                        "The databaseName must be set when initializing the Mongodb Sink.");
    }

    @Test
    public void collectionNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                MongodbAsyncSink.<String>builder()
                                        .setDatabase(DATABASE_NAME)
                                        .setCollection("")
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .build())
                .withMessageContaining(
                        "The collectionName must be set when initializing the Mongodb Sink.");
    }
}
