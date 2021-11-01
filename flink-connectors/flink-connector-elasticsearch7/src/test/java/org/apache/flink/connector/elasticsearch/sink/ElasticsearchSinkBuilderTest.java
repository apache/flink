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

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link ElasticsearchSinkBuilder}. */
class ElasticsearchSinkBuilderTest extends TestLogger {

    @ParameterizedTest
    @MethodSource("validBuilders")
    void testBuildElasticsearchSink(ElasticsearchSinkBuilder<?> builder) {
        builder.build();
    }

    @Test
    void testThrowIfExactlyOnceConfigured() {
        assertThrows(
                IllegalStateException.class,
                () -> createBuilder().setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE));
    }

    @Test
    void testThrowIfHostsNotSet() {
        assertThrows(
                NullPointerException.class,
                () ->
                        new ElasticsearchSinkBuilder<>()
                                .setEmitter((element, indexer, context) -> {})
                                .build());
    }

    @Test
    void testThrowIfEmitterNotSet() {
        assertThrows(
                NullPointerException.class,
                () ->
                        new ElasticsearchSinkBuilder<>()
                                .setHosts(new HttpHost("localhost:3000"))
                                .build());
    }

    private static List<ElasticsearchSinkBuilder<?>> validBuilders() {
        return Lists.newArrayList(
                createBuilder(),
                createBuilder().setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE),
                createBuilder().setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT, 1, 1),
                createBuilder()
                        .setConnectionUsername("username")
                        .setConnectionPassword("password"));
    }

    private static ElasticsearchSinkBuilder<?> createBuilder() {
        return new ElasticsearchSinkBuilder<>()
                .setEmitter((element, indexer, context) -> {})
                .setHosts(new HttpHost("localhost:3000"));
    }
}
