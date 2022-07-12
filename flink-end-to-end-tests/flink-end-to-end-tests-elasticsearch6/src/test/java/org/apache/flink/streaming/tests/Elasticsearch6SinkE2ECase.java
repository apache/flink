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

package org.apache.flink.streaming.tests;

import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.util.DockerImageVersions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/** End to end test for Elasticsearch6Sink based on connector testing framework. */
@SuppressWarnings("unused")
public class Elasticsearch6SinkE2ECase
        extends ElasticsearchSinkE2ECaseBase<KeyValue<Integer, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6SinkE2ECase.class);

    public Elasticsearch6SinkE2ECase() throws Exception {}

    String getElasticsearchContainerName() {
        return DockerImageVersions.ELASTICSEARCH_6;
    }

    @TestContext
    Elasticsearch6SinkExternalContextFactory contextFactory =
            new Elasticsearch6SinkExternalContextFactory(
                    elasticsearch.getContainer(),
                    Arrays.asList(
                            TestUtils.getResource("dependencies/elasticsearch6-end-to-end-test.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL(),
                            TestUtils.getResource("dependencies/flink-connector-test-utils.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL(),
                            TestUtils.getResource(
                                            "dependencies/flink-connector-elasticsearch-test-utils.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL()));
}
