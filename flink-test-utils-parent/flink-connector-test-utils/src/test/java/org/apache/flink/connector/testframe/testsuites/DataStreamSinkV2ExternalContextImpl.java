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

package org.apache.flink.connector.testframe.testsuites;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV2ExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;

import java.io.Serializable;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A basic Implementation of {@link DataStreamSinkV2ExternalContext} that retain all records in
 * memory.
 */
public class DataStreamSinkV2ExternalContextImpl
        implements DataStreamSinkV2ExternalContext<String>, Serializable {

    /** key: identifier of SinkExternalContextImpl, value: List of elements for Sink. */
    private static final Map<String, List<String>> records = new ConcurrentHashMap<>();

    /**
     * Each test will only initialize one SinkExternalContextImpl, so identifier will keep the same
     * in each test.
     */
    private final String identifier = UUID.randomUUID().toString();

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return new ArrayList<>();
    }

    @Override
    public ExternalSystemDataReader<String> createSinkDataReader(TestingSinkSettings sinkSettings) {
        return new ExternalSystemDataReader<>() {
            @Override
            public List<String> poll(Duration timeout) {
                return records.getOrDefault(identifier, new ArrayList<>());
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public List<String> generateTestData(TestingSinkSettings sinkSettings, long seed) {
        List<String> testDatas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // Make the testDatas is sortable.
            testDatas.add(i + UUID.randomUUID().toString());
        }
        return testDatas;
    }

    @Override
    public Sink<String> createSink(TestingSinkSettings sinkSettings)
            throws UnsupportedOperationException {
        return writerInitContext ->
                new SinkWriter<>() {

                    @Override
                    public void write(String element, Context context) {
                        synchronized (records) {
                            List<String> allElements =
                                    records.getOrDefault(identifier, new ArrayList<>());
                            allElements.add(element);
                            records.put(identifier, allElements);
                            writerInitContext.metricGroup().getNumRecordsSendCounter().inc();
                        }
                    }

                    @Override
                    public void flush(boolean endOfInput) {}

                    @Override
                    public void close() {}
                };
    }

    @Override
    public void close() throws Exception {}
}
