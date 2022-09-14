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

import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.testframe.utils.CollectIteratorAssertions.assertThat;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;

/** Base classs for end to end ElasticsearchSink tests based on connector testing framework. */
@SuppressWarnings("unused")
public abstract class ElasticsearchSinkE2ECaseBase<T extends Comparable<T>>
        extends SinkTestSuiteBase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkE2ECaseBase.class);
    private static final int READER_RETRY_ATTEMPTS = 10;
    private static final int READER_TIMEOUT = -1; // Not used

    protected static final String ELASTICSEARCH_HOSTNAME = "elasticsearch";

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    // Defines TestEnvironment
    @TestEnv
    protected FlinkContainerTestEnvironment flink = new FlinkContainerTestEnvironment(1, 6);

    // Defines ConnectorExternalSystem
    @TestExternalSystem
    DefaultContainerizedExternalSystem<ElasticsearchContainer> elasticsearch =
            DefaultContainerizedExternalSystem.builder()
                    .fromContainer(
                            new ElasticsearchContainer(
                                            DockerImageName.parse(getElasticsearchContainerName()))
                                    .withEnv(
                                            "cluster.routing.allocation.disk.threshold_enabled",
                                            "false")
                                    .withNetworkAliases(ELASTICSEARCH_HOSTNAME))
                    .bindWithFlinkContainer(flink.getFlinkContainers().getJobManager())
                    .build();

    @Override
    protected void checkResultWithSemantic(
            ExternalSystemDataReader<T> reader, List<T> testData, CheckpointingMode semantic)
            throws Exception {
        waitUntilCondition(
                () -> {
                    try {
                        List<T> result = reader.poll(Duration.ofMillis(READER_TIMEOUT));
                        assertThat(sort(result).iterator())
                                .matchesRecordsFromSource(
                                        Collections.singletonList(sort(testData)), semantic);
                        return true;
                    } catch (Throwable t) {
                        LOG.warn("Polled results not as expected", t);
                        return false;
                    }
                },
                5000,
                READER_RETRY_ATTEMPTS);
    }

    private List<T> sort(List<T> list) {
        return list.stream().sorted().collect(Collectors.toList());
    }

    abstract String getElasticsearchContainerName();
}
