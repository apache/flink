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

package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.mongodb.streaming.serde.DocumentSerializer;

import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Covers construction, defaults and sanity checking of MongodbAsyncSinkBuilder. */
public class MongodbAsyncSinkBuilderTest {
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
    public void streamNameOfSinkMustBeSetWhenBuilt() {
        Throwable thrown =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                MongodbAsyncSink.<String>builder()
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .build());
        assertEquals(
                "The stream name must not be null when initializing the KDS Sink.",
                thrown.getMessage());
    }

    @Test
    public void streamNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Throwable thrown =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongodbAsyncSink.<String>builder()
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .setCollection("")
                                        .setDatabase("")
                                        .setCollection("")
                                        .build());
        assertEquals(
                "The stream name must be set when initializing the KDS Sink.", thrown.getMessage());
    }
}
