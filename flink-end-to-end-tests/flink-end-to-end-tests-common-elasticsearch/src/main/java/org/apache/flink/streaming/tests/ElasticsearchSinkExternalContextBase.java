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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV2ExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;

import org.apache.commons.lang3.RandomStringUtils;

import java.net.URL;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The base class for Elasticsearch sink context. */
abstract class ElasticsearchSinkExternalContextBase
        implements DataStreamSinkV2ExternalContext<KeyValue<Integer, String>> {
    /** The constant INDEX_NAME_PREFIX. */
    protected static final String INDEX_NAME_PREFIX = "es-index";

    private static final int RANDOM_STRING_MAX_LENGTH = 50;
    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;
    protected static final int BULK_BUFFER = 100;
    protected static final int PAGE_LENGTH = NUM_RECORDS_UPPER_BOUND + 1;
    /** The index name. */
    protected final String indexName;

    /** The address reachable from Flink (internal to the testing environment). */
    protected final String addressInternal;

    /** The connector jar paths. */
    protected final List<URL> connectorJarPaths;

    /** The client. */
    protected final ElasticsearchClient client;

    /**
     * Instantiates a new Elasticsearch sink context base.
     *
     * @param addressInternal The address to access Elasticsearch from within Flink. When running in
     *     a containerized environment, should correspond to the network alias that resolves within
     *     the environment's network together with the exposed port.
     * @param connectorJarPaths The connector jar paths.
     * @param client The Elasticsearch client.
     */
    ElasticsearchSinkExternalContextBase(
            String addressInternal, List<URL> connectorJarPaths, ElasticsearchClient client) {
        this.addressInternal = checkNotNull(addressInternal);
        this.connectorJarPaths = checkNotNull(connectorJarPaths);
        this.client = checkNotNull(client);
        this.indexName =
                INDEX_NAME_PREFIX + "-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    }

    @Override
    public List<KeyValue<Integer, String>> generateTestData(
            TestingSinkSettings sinkSettings, long seed) {
        Random random = new Random(seed);
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;

        return IntStream.range(0, recordNum)
                .boxed()
                .map(
                        i -> {
                            int valueLength = random.nextInt(RANDOM_STRING_MAX_LENGTH) + 1;
                            String value = RandomStringUtils.random(valueLength, true, true);
                            return KeyValue.of(i, value);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        client.deleteIndex(indexName);
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return connectorJarPaths;
    }

    @Override
    public TypeInformation<KeyValue<Integer, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<KeyValue<Integer, String>>() {});
    }

    @Override
    public abstract Sink<KeyValue<Integer, String>> createSink(TestingSinkSettings sinkSettings);

    @Override
    public abstract ExternalSystemDataReader<KeyValue<Integer, String>> createSinkDataReader(
            TestingSinkSettings sinkSettings);

    @Override
    public abstract String toString();
}
